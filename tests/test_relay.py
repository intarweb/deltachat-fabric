"""Unit tests for the relay engine. NO live rpc-server, NO live network.

The deltachat account-manager, the a2a-directory fetch, and the wake POST are all mocked:
  * ``FakeBackend``        stands in for DeltaBackend (send + inbound stream)
  * ``httpx.MockTransport`` drives the a2a directory GET + wake POST
"""
from __future__ import annotations

import json
from typing import Optional

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import BotSpec, Config
from app.relay import (
    AgentDirectory,
    HoldQueue,
    InboundMessage,
    Relay,
    create_app,
    extract_mentions,
)


# --------------------------------------------------------------------------- fixtures


def make_config() -> Config:
    """Generic config (roster/domain/leads injected — nothing baked, matches deploy)."""
    return Config(
        mail_domain="deltachat.example.net",
        imap_host="mail.example.net",
        a2a_directory_url="http://directory.test/agents",
        roster=[
            BotSpec(id="bot-lead", realm="realm-a"),
            BotSpec(id="bot-a", realm="realm-a"),
            BotSpec(id="bot-b", realm="realm-a"),
        ],
        realm_leads={"realm-a": "bot-lead"},
    )


class FakeBackend:
    """In-memory stand-in for DeltaBackend."""

    def __init__(self, accounts: dict[str, int], inbound: Optional[list[InboundMessage]] = None,
                 contacts: Optional[dict[int, list[dict]]] = None,
                 channels: Optional[dict[int, list[dict]]] = None):
        self._accounts = accounts                 # localpart -> account_id
        self._inbox = list(inbound or [])
        self.sent: list[tuple[int, int, str]] = []
        self._next_msg_id = 1000
        self._contacts = contacts or {}           # account_id -> [contact dict, ...]
        self._channels = channels or {}           # account_id -> [channel dict, ...]
        self.created: list[tuple[int, str, list[str]]] = []
        self.added: list[tuple[int, int, str]] = []
        self.reacted: list[tuple[int, int, str]] = []
        self._next_chat_id = 500

    def account_id_for(self, localpart: str) -> Optional[int]:
        return self._accounts.get(localpart)

    def send(self, account_id: int, chat_id: int, text: str) -> int:
        self._next_msg_id += 1
        self.sent.append((account_id, chat_id, text))
        return self._next_msg_id

    def next_inbound(self) -> Optional[InboundMessage]:
        return self._inbox.pop(0) if self._inbox else None

    def list_contacts(self, account_id: int) -> list[dict]:
        return list(self._contacts.get(account_id, []))

    def list_channels(self, account_id: int) -> list[dict]:
        return list(self._channels.get(account_id, []))

    def create_channel(self, account_id: int, name: str, members: list[str]) -> int:
        self._next_chat_id += 1
        self.created.append((account_id, name, list(members)))
        return self._next_chat_id

    def add_member(self, account_id: int, chat_id: int, contact: str) -> None:
        self.added.append((account_id, chat_id, contact))

    def react(self, account_id: int, msg_id: int, emoji: str) -> None:
        self.reacted.append((account_id, msg_id, emoji))


def directory_transport(agents: list[dict], wake_sink: list[dict], *,
                        directory_status: int = 200):
    """An httpx.MockTransport: GETs return the agent directory, POSTs record the wake.

    ``agents`` is the directory payload; ``wake_sink`` collects delivered wakes. Set
    ``directory_status`` to a non-200 to simulate an unresolvable directory.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            if directory_status != 200:
                return httpx.Response(directory_status, json={})
            return httpx.Response(200, json={"agents": agents})
        # POST = wake delivery
        body = json.loads(request.content.decode() or "{}")
        wake_sink.append({"url": str(request.url), "body": body})
        return httpx.Response(200, json={"ok": True})

    return httpx.MockTransport(handler)


def make_relay(backend: FakeBackend, agents: list[dict], wake_sink: list[dict],
               tmp_path, *, directory_status: int = 200,
               config: Optional[Config] = None) -> Relay:
    config = config or make_config()
    client = httpx.AsyncClient(
        transport=directory_transport(agents, wake_sink, directory_status=directory_status)
    )
    directory = AgentDirectory(config, client)
    hold = HoldQueue(str(tmp_path))
    return Relay(config, backend, directory, hold)


# --------------------------------------------------------------------------- extract_mentions


def test_extract_mentions_filters_to_members_and_dedupes():
    assert extract_mentions("hey @bot-a and @bot-a and @bot-c", ["bot-a", "bot-b"]) == ["bot-a"]
    assert extract_mentions("no mentions here", ["bot-a"]) == []


# --------------------------------------------------------------------------- (1) /send


def test_send_routes_to_right_account_and_returns_status(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7, "bot-lead": 3})
    relay = make_relay(backend, [], [], tmp_path)

    result = relay.send("bot-a", target=42, text="hello")

    assert result["status"] == "sent"
    assert result["account_id"] == 7
    assert result["msg_id"] > 0
    # routed to bot-a's account (7), the given chat (42), with the text
    assert backend.sent == [(7, 42, "hello")]


def test_send_unknown_bot_raises(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path)
    with pytest.raises(KeyError):
        relay.send("bot-c", target=1, text="x")


def test_send_http_endpoint_contract(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path)
    app = create_app(relay)
    client = TestClient(app)

    resp = client.post("/send", json={"bot_id": "bot-a", "target": 42, "text": "hi"})
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"status": "sent", "msg_id": body["msg_id"], "account_id": 7}
    assert isinstance(body["msg_id"], int)

    miss = client.post("/send", json={"bot_id": "bot-c", "target": 1, "text": "x"})
    assert miss.status_code == 404


# --------------------------------------------------------------------------- (2) inbound → wake


async def test_inbound_group_mention_wakes_right_bot_via_resolved_url(tmp_path):
    # @bot-a mentioned in a realm-a group → wake ONLY bot-a, at its LIVE directory url
    msg = InboundMessage(
        account_id=3, chat_id=555, msg_id=99, text="ping @bot-a please",
        is_group=True, members=["bot-lead", "bot-a", "bot-b"], mentioned=["bot-a"],
    )
    backend = FakeBackend(accounts={"bot-lead": 3}, inbound=[msg])
    wakes: list[dict] = []
    agents = [
        {"name": "bot-a", "url": "http://bot-a.live:8020"},
        {"name": "bot-b", "url": "http://bot-b.live:8060"},
    ]
    relay = make_relay(backend, agents, wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == ["bot-a"]                       # delegates to routing.wake_targets
    assert len(wakes) == 1
    assert wakes[0]["url"] == "http://bot-a.live:8020/a2a"   # resolved from mocked directory
    assert wakes[0]["body"]["bot_id"] == "bot-a"
    assert wakes[0]["body"]["msg_id"] == 99
    assert len(relay.hold) == 0                     # delivered → nothing held


async def test_inbound_no_mention_wakes_only_channel_main(tmp_path):
    # unaddressed group msg → wake ONLY the realm lead (bot-lead), never all (anti-herd)
    msg = InboundMessage(
        account_id=7, chat_id=555, msg_id=100, text="just chatter",
        is_group=True, members=["bot-lead", "bot-a", "bot-b"], mentioned=[],
    )
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    wakes: list[dict] = []
    agents = [{"name": "bot-lead", "url": "http://bot-lead.live:7780"}]
    relay = make_relay(backend, agents, wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == ["bot-lead"]
    assert [w["body"]["bot_id"] for w in wakes] == ["bot-lead"]


async def test_inbound_non_group_is_ignored(tmp_path):
    msg = InboundMessage(account_id=7, chat_id=1, msg_id=1, text="hi", is_group=False,
                         members=[], mentioned=[])
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    relay = make_relay(backend, [], [], tmp_path)
    assert await relay.handle_inbound(msg) == []


# --------------------------------------------------------------------------- (3) unresolvable → held + drained


async def test_unresolvable_target_is_held_then_drained_idempotent(tmp_path):
    # bot-a is mentioned but NOT in the directory → wake fails → held
    msg = InboundMessage(
        account_id=3, chat_id=555, msg_id=99, text="@bot-a",
        is_group=True, members=["bot-lead", "bot-a"], mentioned=["bot-a"],
    )
    backend = FakeBackend(accounts={"bot-lead": 3}, inbound=[msg])
    wakes: list[dict] = []
    empty_agents: list[dict] = []          # bot-a not resolvable yet
    relay = make_relay(backend, empty_agents, wakes, tmp_path)

    woken = await relay.handle_inbound(msg)
    assert woken == []                     # not delivered
    assert len(relay.hold) == 1            # parked in the durable queue
    assert wakes == []

    # a re-hold of the SAME event must not duplicate (idempotent)
    await relay.handle_inbound(msg)
    assert len(relay.hold) == 1

    # bot-a comes online → point the directory at it and drain
    relay.directory.client = httpx.AsyncClient(
        transport=directory_transport(
            [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes
        )
    )
    delivered = await relay.drain_holds()
    assert delivered == 1
    assert len(relay.hold) == 0
    assert wakes[0]["url"] == "http://bot-a.live:8020/a2a"

    # draining again is a no-op (idempotent, nothing left)
    assert await relay.drain_holds() == 0


async def test_wake_post_failure_also_holds(tmp_path):
    # directory resolves the bot, but the wake POST fails (5xx) → still held
    msg = InboundMessage(
        account_id=3, chat_id=5, msg_id=1, text="@bot-a", is_group=True,
        members=["bot-lead", "bot-a"], mentioned=["bot-a"],
    )
    backend = FakeBackend(accounts={"bot-lead": 3})

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"agents": [{"name": "bot-a", "url": "http://bot-a:8020"}]})
        return httpx.Response(503)      # wake POST fails

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = make_config()
    relay = Relay(config, backend, AgentDirectory(config, client), HoldQueue(str(tmp_path)))

    woken = await relay.handle_inbound(msg)
    assert woken == []
    assert len(relay.hold) == 1


# --------------------------------------------------------------------------- (4) hold-queue survives reload


def test_hold_queue_survives_reload(tmp_path):
    q = HoldQueue(str(tmp_path))
    q.add("bot-a", {"chat_id": 5, "msg_id": 1, "text": "hi"})
    q.add("bot-b", {"chat_id": 5, "msg_id": 2, "text": "yo"})
    assert len(q) == 2

    # a brand-new instance (simulating a process reload) reads the same file back
    q2 = HoldQueue(str(tmp_path))
    assert len(q2) == 2
    bots = {i["bot_id"] for i in q2.pending()}
    assert bots == {"bot-a", "bot-b"}


def test_hold_queue_add_is_idempotent(tmp_path):
    q = HoldQueue(str(tmp_path))
    payload = {"chat_id": 5, "msg_id": 1, "text": "hi"}
    q.add("bot-a", payload)
    q.add("bot-a", payload)             # same (bot,chat,msg) → not duplicated
    assert len(q) == 1
    q.add("bot-a", {"chat_id": 5, "msg_id": 2, "text": "hi"})  # different msg → added
    assert len(q) == 2


# --------------------------------------------------------------------------- tick integration


async def test_tick_drains_stream_then_retries_holds(tmp_path):
    # one deliverable + one that must be held; tick processes both and reports counts
    deliver = InboundMessage(account_id=3, chat_id=1, msg_id=1, text="@bot-a", is_group=True,
                             members=["bot-lead", "bot-a"], mentioned=["bot-a"])
    hold_it = InboundMessage(account_id=3, chat_id=2, msg_id=2, text="@bot-b", is_group=True,
                             members=["bot-lead", "bot-b"], mentioned=["bot-b"])
    backend = FakeBackend(accounts={"bot-lead": 3}, inbound=[deliver, hold_it])
    wakes: list[dict] = []
    # only bot-a resolvable; bot-b will be held
    agents = [{"name": "bot-a", "url": "http://bot-a:8020"}]
    relay = make_relay(backend, agents, wakes, tmp_path)

    summary = await relay.tick()
    assert summary["processed"] == 2
    assert summary["woken"] == 1
    assert len(relay.hold) == 1        # bot-b held


# --------------------------------------------------------------------------- (5) contacts / channels: Relay methods


def test_relay_list_contacts_routes_to_account(tmp_path):
    backend = FakeBackend(
        accounts={"bot-a": 7},
        contacts={7: [{"id": 1, "address": "bot-b@x.net", "display_name": "bot-b"}]},
    )
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.list_contacts("bot-a")
    assert out["account_id"] == 7
    assert out["contacts"][0]["address"] == "bot-b@x.net"


def test_relay_list_contacts_unknown_bot_raises(tmp_path):
    relay = make_relay(FakeBackend(accounts={"bot-a": 7}), [], [], tmp_path)
    with pytest.raises(KeyError):
        relay.list_contacts("bot-c")


def test_relay_create_channel_passes_members_from_args_not_baked(tmp_path):
    # 🔴 generic model: members come from the CALLER, nothing about the roster is baked
    backend = FakeBackend(accounts={"bot-lead": 3})
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.create_channel("bot-lead", "realm-a-ops", ["bot-a@x.net", "bot-b@x.net"])
    assert out["status"] == "created"
    assert out["account_id"] == 3
    assert out["members"] == ["bot-a@x.net", "bot-b@x.net"]
    # backend saw exactly the caller-supplied members (generic — not derived from config)
    assert backend.created == [(3, "realm-a-ops", ["bot-a@x.net", "bot-b@x.net"])]


def test_relay_add_member_passes_contact_from_args(tmp_path):
    backend = FakeBackend(accounts={"bot-lead": 3})
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.add_member("bot-lead", channel_id=501, contact="bot-i@x.net")
    assert out["status"] == "added"
    assert backend.added == [(3, 501, "bot-i@x.net")]


def test_relay_react_routes_to_account(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.react("bot-a", chat_id=42, msg_id=99, emoji="👍")
    assert out == {"status": "reacted", "account_id": 7, "chat_id": 42, "msg_id": 99, "emoji": "👍"}
    assert backend.reacted == [(7, 99, "👍")]


# --------------------------------------------------------------------------- (6) contacts / channels: HTTP endpoints


def test_contacts_endpoint(tmp_path):
    backend = FakeBackend(
        accounts={"bot-a": 7},
        contacts={7: [{"id": 1, "address": "bot-b@x.net", "display_name": "bot-b"}]},
    )
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.get("/contacts", params={"bot_id": "bot-a"})
    assert resp.status_code == 200
    assert resp.json() == {"account_id": 7, "contacts": [
        {"id": 1, "address": "bot-b@x.net", "display_name": "bot-b"}]}

    miss = client.get("/contacts", params={"bot_id": "bot-c"})
    assert miss.status_code == 404


def test_channels_endpoint(tmp_path):
    backend = FakeBackend(
        accounts={"bot-lead": 3},
        channels={3: [{"id": 555, "name": "realm-a", "members": ["bot-lead", "bot-a"]}]},
    )
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.get("/channels", params={"bot_id": "bot-lead"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["account_id"] == 3
    assert body["channels"][0]["members"] == ["bot-lead", "bot-a"]

    miss = client.get("/channels", params={"bot_id": "bot-c"})
    assert miss.status_code == 404


def test_send_channel_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/send_channel", json={"bot_id": "bot-a", "channel_id": 555, "text": "hi all"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "sent"
    assert body["account_id"] == 7
    assert body["channel_id"] == 555
    assert backend.sent == [(7, 555, "hi all")]

    miss = client.post("/send_channel", json={"bot_id": "bot-c", "channel_id": 1, "text": "x"})
    assert miss.status_code == 404


def test_create_channel_endpoint_members_from_body(tmp_path):
    # 🔴 members are exactly what the request carries — generic, never baked
    backend = FakeBackend(accounts={"bot-lead": 3})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/channel", json={
        "bot_id": "bot-lead", "name": "ops", "members": ["bot-a@x.net", "bot-b@x.net"]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "created"
    assert body["members"] == ["bot-a@x.net", "bot-b@x.net"]
    assert backend.created == [(3, "ops", ["bot-a@x.net", "bot-b@x.net"])]

    # localpart alias also accepted; empty members is valid (generic)
    resp2 = client.post("/channel", json={"localpart": "bot-lead", "name": "solo"})
    assert resp2.status_code == 200
    assert resp2.json()["members"] == []


def test_add_member_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-lead": 3})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/channel/member", json={
        "bot_id": "bot-lead", "channel_id": 555, "contact": "bot-i@x.net"})
    assert resp.status_code == 200
    assert resp.json()["contact"] == "bot-i@x.net"
    assert backend.added == [(3, 555, "bot-i@x.net")]

    miss = client.post("/channel/member", json={
        "bot_id": "bot-c", "channel_id": 1, "contact": "x@x.net"})
    assert miss.status_code == 404


def test_react_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/react", json={
        "bot_id": "bot-a", "chat_id": 42, "msg_id": 99, "emoji": "🔥"})
    assert resp.status_code == 200
    assert resp.json()["emoji"] == "🔥"
    assert backend.reacted == [(7, 99, "🔥")]

    miss = client.post("/react", json={
        "bot_id": "bot-c", "chat_id": 1, "msg_id": 1, "emoji": "x"})
    assert miss.status_code == 404


# --------------------------------------------------------------------------- (7) integration seams: real deltachat2 types + core onboarding


def test_incoming_ids_selects_by_type_not_kind_string():
    """🔴 Regression: deltachat2's ``EventTypeIncomingMsg`` has NO ``.kind`` attribute — it
    is distinguished by TYPE. The old ``getattr(ev,"kind") == "IncomingMsg"`` check therefore
    silently dropped EVERY incoming message (would bind + onboard but receive nothing).
    Verified against the installed deltachat2 (Event.fields = context_id,event;
    EventTypeIncomingMsg.fields = chat_id,msg_id, no kind)."""
    from deltachat2 import EventTypeIncomingMsg

    from app.relay import DeltaChat2Backend

    # a real incoming-message event → (chat_id, msg_id)
    assert DeltaChat2Backend.incoming_ids(EventTypeIncomingMsg(chat_id=7, msg_id=42)) == (7, 42)
    # a non-incoming, typed event (no chat_id/msg_id) → ignored
    assert DeltaChat2Backend.incoming_ids(object()) == (None, None)
    # defensive dict-shaped fallback still works
    assert DeltaChat2Backend.incoming_ids(
        {"kind": "IncomingMsg", "chat_id": 1, "msg_id": 2}) == (1, 2)


class _FakeRpc:
    """Records core onboarding calls so ensure_account is unit-tested with no live rpc-server.
    Mirrors the deltachat2 Rpc surface ensure_account uses (verified against the package)."""

    def __init__(self):
        self.accounts: dict[int, str | None] = {}   # accid -> configured addr
        self.configured: set[int] = set()
        self.transports: list = []                  # (accid, EnteredLoginParam)
        self.started: list[int] = []
        self._next = 0

    def get_all_account_ids(self):
        return list(self.accounts)

    def get_config(self, accid, key):
        return self.accounts.get(accid)             # addr / configured_addr

    def add_account(self):
        self._next += 1
        self.accounts[self._next] = None
        return self._next

    def set_config(self, accid, key, val):
        if key == "addr":
            self.accounts[accid] = val

    def add_or_update_transport(self, accid, param):
        self.transports.append((accid, param))
        self.accounts[accid] = param.addr           # core now knows the configured addr
        self.configured.add(accid)

    def is_configured(self, accid):
        return accid in self.configured

    def start_io(self, accid):
        self.started.append(accid)

    def start_io_for_all_accounts(self):
        pass


def test_ensure_account_onboards_into_the_core_not_just_imap(tmp_path):
    """🔴 Bug 2 regression: onboarding must add+configure the account IN THE DELTACHAT CORE
    (add_account + add_or_update_transport), not merely IMAP-login — that's why /data/accounts
    had 0 .db files. Uses the REAL deltachat2 EnteredLoginParam/Socket via a fake rpc."""
    from app.relay import DeltaChat2Backend

    rpc = _FakeRpc()
    cfg = make_config()  # mail_domain = deltachat.example.net
    be = DeltaChat2Backend(cfg, str(tmp_path / "accounts"), _rpc=rpc)

    ok = be.ensure_account("bot-a", "pw123456789",
                           imap_host="mail.example.net", imap_port=993,
                           smtp_host="mail.example.net", smtp_port=587)

    assert ok is True
    # onboarded INTO THE CORE: exactly one transport configured, with the right addr + ports.
    assert len(rpc.transports) == 1
    accid, param = rpc.transports[0]
    assert param.addr == "bot-a@deltachat.example.net"
    assert (param.imap_server, param.imap_port) == ("mail.example.net", 993)
    assert (param.smtp_server, param.smtp_port) == ("mail.example.net", 587)
    assert str(param.imap_security) == "ssl" and str(param.smtp_security) == "starttls"
    assert rpc.is_configured(accid)
    assert accid in rpc.started                      # start_io called → begins receiving
    # the backend now indexes the account by localpart (so /send can resolve it)
    assert be.account_id_for("bot-a") == accid


def test_ensure_account_is_idempotent_skips_already_configured(tmp_path):
    """A second reconcile pass for an already-configured account is a no-op (no re-add)."""
    from app.relay import DeltaChat2Backend

    rpc = _FakeRpc()
    be = DeltaChat2Backend(make_config(), str(tmp_path / "accounts"), _rpc=rpc)
    kw = dict(imap_host="mail.example.net", imap_port=993,
              smtp_host="mail.example.net", smtp_port=587)

    assert be.ensure_account("bot-a", "pw123456789", **kw) is True
    assert be.ensure_account("bot-a", "pw123456789", **kw) is True   # idempotent
    assert len(rpc.transports) == 1                                  # NOT re-onboarded
