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

    def localpart_for(self, account_id: int) -> Optional[str]:
        for lp, accid in self._accounts.items():
            if accid == account_id:
                return lp
        return None

    def send(self, account_id: int, chat_id: int, text: str) -> int:
        self._next_msg_id += 1
        self.sent.append((account_id, chat_id, text))
        return self._next_msg_id

    def send_to_addr(self, account_id: int, addr: str, text: str) -> tuple[int, int]:
        self.sent_to = getattr(self, "sent_to", [])
        self._next_chat_id += 1
        self._next_msg_id += 1
        self.sent_to.append((account_id, addr, text))
        return self._next_chat_id, self._next_msg_id

    def next_inbound(self) -> Optional[InboundMessage]:
        return self._inbox.pop(0) if self._inbox else None

    def list_contacts(self, account_id: int) -> list[dict]:
        return list(self._contacts.get(account_id, []))

    def list_channels(self, account_id: int) -> list[dict]:
        return list(self._channels.get(account_id, []))

    def list_messages(self, account_id: int, chat_id: int, limit: int = 20) -> list[dict]:
        msgs = getattr(self, "_messages", {}).get((account_id, chat_id), [])
        return list(msgs)[-limit:]

    def create_invite(self, account_id: int) -> str:
        return f"https://i.delta.chat/#FAKEINVITE-acc{account_id}"

    def create_channel(self, account_id: int, name: str, members: list[str]) -> int:
        self._next_chat_id += 1
        self.created.append((account_id, name, list(members)))
        return self._next_chat_id

    def add_member(self, account_id: int, chat_id: int, contact: str) -> None:
        self.added.append((account_id, chat_id, contact))

    def react(self, account_id: int, msg_id: int, emoji: str) -> None:
        self.reacted.append((account_id, msg_id, emoji))

    def secure_join(self, account_id: int, invite: str) -> int:
        self.securejoined = getattr(self, "securejoined", [])
        self.securejoined.append((account_id, invite))
        return 4321

    def delete_chat(self, account_id: int, chat_id: int) -> None:
        self.deleted = getattr(self, "deleted", [])
        self.deleted.append((account_id, chat_id))


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
        # POST = wake delivery (A2A JSON-RPC message/send). Record url + the raw body + the
        # extracted message text so tests can assert on the human-readable envelope.
        body = json.loads(request.content.decode() or "{}")
        text = ""
        try:
            text = body["params"]["message"]["parts"][0]["text"]
        except Exception:
            pass
        wake_sink.append({"url": str(request.url), "body": body,
                          "method": body.get("method"), "text": text})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body.get("id"), "result": {}})

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


def test_send_to_addr_resolves_and_returns_chat_and_msg(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path)

    result = relay.send_to_addr("bot-a", "person@example.com", "hi there")

    assert result["status"] == "sent"
    assert result["account_id"] == 7
    assert result["chat_id"] > 0 and result["msg_id"] > 0
    assert backend.sent_to == [(7, "person@example.com", "hi there")]


def test_send_to_addr_unknown_bot_raises(tmp_path):
    relay = make_relay(FakeBackend(accounts={"bot-a": 7}), [], [], tmp_path)
    with pytest.raises(KeyError):
        relay.send_to_addr("nobody", "person@example.com", "x")


def test_send_to_endpoint_contract(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/send_to", json={"bot_id": "bot-a", "addr": "person@example.com", "text": "hi"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "sent" and body["account_id"] == 7
    assert body["chat_id"] > 0 and body["msg_id"] > 0
    assert backend.sent_to == [(7, "person@example.com", "hi")]

    miss = client.post("/send_to", json={"bot_id": "nobody", "addr": "p@example.com", "text": "x"})
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
    # A2A message/send to the bot's OWN agent url (NOT <url>/a2a), carrying the text envelope
    assert wakes[0]["url"] == "http://bot-a.live:8020"   # resolved from mocked directory
    assert wakes[0]["method"] == "message/send"
    assert "Delta Chat" in wakes[0]["text"] and "@bot-a" in wakes[0]["text"]
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
    assert [w["url"] for w in wakes] == ["http://bot-lead.live:7780"]
    assert all(w["method"] == "message/send" for w in wakes)


async def test_inbound_direct_1to1_wakes_receiving_bot(tmp_path):
    # a 1:1 (non-group) message — e.g. a human DM — wakes the RECEIVING bot (account owner),
    # marked direct, so bots see direct messages not just group traffic.
    msg = InboundMessage(account_id=7, chat_id=11, msg_id=16, text="Hrllo", is_group=False,
                         members=[], mentioned=[])
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    wakes: list[dict] = []
    agents = [{"name": "bot-a", "url": "http://bot-a.live:8020"}]
    relay = make_relay(backend, agents, wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == ["bot-a"]
    w = wakes[0]
    # A2A message/send to bot-a's own agent url; DM marker + message text in the envelope
    assert w["url"] == "http://bot-a.live:8020"
    assert w["method"] == "message/send"
    assert "DM" in w["text"] and "Hrllo" in w["text"]
    # no resolvable sender → falls back to "someone" (never a bare/blank sender)
    assert "someone" in w["text"]


async def test_inbound_direct_1to1_surfaces_sender_and_dedups_redelivery(tmp_path):
    # A human DM must (a) name the sender in the wake text — a bare "[Delta Chat DM]" is what
    # consumers render as "unknown"/"not found" — and (b) NOT re-wake on a re-delivery of the
    # SAME message (same global rfc724_mid), the same dedup the group path applies per target.
    msg = InboundMessage(account_id=7, chat_id=11, msg_id=16, text="hello there",
                         is_group=False, members=[], mentioned=[],
                         from_localpart="terafin", rfc724_mid="<dm-1@chatmail>")
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    wakes: list[dict] = []
    agents = [{"name": "bot-a", "url": "http://bot-a.live:8020"}]
    relay = make_relay(backend, agents, wakes, tmp_path)

    first = await relay.handle_inbound(msg)
    second = await relay.handle_inbound(msg)   # same rfc724_mid → already handled

    assert first == ["bot-a"]
    assert second == []                        # #1: 1:1 re-wake suppressed
    assert len(wakes) == 1                     # exactly one wake delivered
    assert "terafin" in wakes[0]["text"]       # #2: sender surfaced in the envelope text
    assert "hello there" in wakes[0]["text"]


async def test_inbound_dm_wake_text_carries_delta_send_reply_instruction(tmp_path):
    # A human-DM wake MUST tell the bot HOW to reply on Delta (delta_send tool + chat_id).
    # a2a_complete_task does NOT bridge back to Delta, so without this the human never sees
    # the reply (the bug Justin hit 2026-07-20).
    msg = InboundMessage(account_id=7, chat_id=11, msg_id=16, text="ping",
                         is_group=False, members=[], mentioned=[],
                         from_localpart="terafin", rfc724_mid="<dm-reply@chatmail>")
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    wakes: list[dict] = []
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == ["bot-a"]
    t = wakes[0]["text"]
    assert "delta_send" in t                 # names the reply tool
    assert "target=11" in t                  # carries the chat_id to reply into
    assert 'bot_id="bot-a"' in t             # which account to send AS
    assert "ping" in t and "terafin" in t    # original DM + sender still present


async def test_inbound_direct_1to1_unknown_account_noop(tmp_path):
    # non-group message for an account with no known bot → nothing to wake
    msg = InboundMessage(account_id=99, chat_id=1, msg_id=1, text="hi", is_group=False,
                         members=[], mentioned=[])
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], [], tmp_path)
    assert await relay.handle_inbound(msg) == []


async def test_handle_reaction_wakes_owner_with_envelope(tmp_path):
    # a human reacted to bot-a's OWN message → wake bot-a with {who, emoji, msg_id}
    from app.relay import InboundReaction
    r = InboundReaction(account_id=7, chat_id=11, msg_id=17, emoji="👍",
                        from_id=12, from_addr="person@example.com",
                        own_message=True, rfc724_mid="<react-1@x>")
    backend = FakeBackend(accounts={"bot-a": 7})
    wakes: list[dict] = []
    agents = [{"name": "bot-a", "url": "http://bot-a.live:8020"}]
    relay = make_relay(backend, agents, wakes, tmp_path)

    woken = await relay.handle_reaction(r)

    assert woken == ["bot-a"]
    w = wakes[0]
    # A2A message/send to bot-a's own agent url; the text surfaces {who, emoji, msg_id}
    assert w["url"] == "http://bot-a.live:8020"
    assert w["method"] == "message/send"
    assert "reacted 👍" in w["text"]
    assert "person@example.com" in w["text"]
    assert "msg 17" in w["text"]


async def test_handle_reaction_unknown_account_noop(tmp_path):
    from app.relay import InboundReaction
    r = InboundReaction(account_id=99, chat_id=1, msg_id=1, emoji="👍", from_addr="p@example.com")
    relay = make_relay(FakeBackend(accounts={"bot-a": 7}),
                       [{"name": "bot-a", "url": "http://bot-a.live:8020"}], [], tmp_path)
    assert await relay.handle_reaction(r) == []


async def test_handle_reaction_skips_non_author_account(tmp_path):
    # the reaction event reaches every member account; only the AUTHOR's account wakes.
    # own_message=False → this account didn't author the reacted-to message → no wake.
    from app.relay import InboundReaction
    r = InboundReaction(account_id=7, chat_id=11, msg_id=17, emoji="👍",
                        from_addr="person@example.com", own_message=False, rfc724_mid="<r@x>")
    relay = make_relay(FakeBackend(accounts={"bot-a": 7}),
                       [{"name": "bot-a", "url": "http://bot-a.live:8020"}], [], tmp_path)
    assert await relay.handle_reaction(r) == []


async def test_group_wake_dedup_collapses_n_member_amplification(tmp_path):
    # SAME group message surfaces on 3 member accounts (same global rfc724_mid); the target
    # (bot-a, @mentioned) must be woken EXACTLY ONCE across all copies — not 3×.
    wakes: list[dict] = []
    backend = FakeBackend(accounts={"bot-lead": 1, "bot-a": 2, "bot-b": 3})
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)
    woken_total = []
    for acct in (1, 2, 3):   # bot-lead, bot-a, bot-b accounts each get the same message
        msg = InboundMessage(account_id=acct, chat_id=555, msg_id=acct, text="hey @bot-a",
                             is_group=True, members=["bot-lead", "bot-a", "bot-b"],
                             mentioned=["bot-a"], rfc724_mid="<same-global-id@chatmail>")
        woken_total += await relay.handle_inbound(msg)
    assert woken_total == ["bot-a"]        # exactly one wake, not 3×
    assert len(wakes) == 1


async def test_group_wake_excludes_sender_leader_untagged(tmp_path):
    # the LEADER posts an untagged message → wake_targets=[main=bot-lead]=sender → excluded → no wake
    # (this is the self-wake bug: leader self-waking on its own untagged post).
    wakes: list[dict] = []
    backend = FakeBackend(accounts={"bot-lead": 1, "bot-a": 2})
    relay = make_relay(backend, [{"name": "bot-lead", "url": "http://bot-lead.live:7780"}], wakes, tmp_path)
    msg = InboundMessage(account_id=2, chat_id=555, msg_id=5, text="chatter", is_group=True,
                         members=["bot-lead", "bot-a"], mentioned=[],
                         from_localpart="bot-lead", rfc724_mid="<g1@x>")
    assert await relay.handle_inbound(msg) == []      # sender (leader) excluded from its own wake
    assert wakes == []


async def test_inbound_self_echo_skipped(tmp_path):
    # a bot's own message echoed back to ITS OWN account (from_localpart == account owner) → no wake
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], [], tmp_path)
    msg = InboundMessage(account_id=7, chat_id=555, msg_id=9, text="my own post @bot-a",
                         is_group=True, members=["bot-a", "bot-b"], mentioned=["bot-a"],
                         from_localpart="bot-a", rfc724_mid="<self@x>")
    assert await relay.handle_inbound(msg) == []


def test_core_diagnostic_maps_surfaceable_events():
    """core_diagnostic maps Error/Warning/Securejoin*Progress → (level, text), else None —
    so the decrypt/SMTP/securejoin core events are surfaced to the relay log. Verified vs the
    installed deltachat2 types."""
    from deltachat2 import (EventTypeError, EventTypeWarning, EventTypeInfo,
                            EventTypeSecurejoinInviterProgress)
    from app.relay import DeltaChat2Backend
    cd = DeltaChat2Backend.core_diagnostic
    assert cd(EventTypeError(msg="boom")) == ("error", "boom")
    lvl, txt = cd(EventTypeWarning(msg="Could not find symmetric secret"))
    assert lvl == "warning" and "symmetric secret" in txt
    lvl, txt = cd(EventTypeSecurejoinInviterProgress(chat_id=5, chat_type=120, contact_id=9, progress=400))
    assert lvl == "info" and "progress=400" in txt and "contact=9" in txt
    assert cd(EventTypeInfo(msg="chatty")) is None      # Info intentionally not surfaced
    assert cd(object()) is None


async def test_wake_posts_a2a_jsonrpc_message_send_to_agent_url(tmp_path):
    """🔴 Regression: a2abridge speaks A2A JSON-RPC. The wake MUST be a `message/send` request
    POSTed to the agent's OWN url — NOT a plain JSON POST to `<url>/a2a` (that never lands in the
    bot's a2a inbox; it's why live reaction/DM wakes silently failed while /messages still read
    the store fine). This asserts the exact envelope shape against the resolved url."""
    from app.relay import AgentDirectory

    wakes: list[dict] = []
    client = httpx.AsyncClient(
        transport=directory_transport([{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes)
    )
    directory = AgentDirectory(make_config(), client)
    ok = await directory.wake("http://bot-a.live:8020", "bot-a", {"text": "hello bot"})

    assert ok is True
    w = wakes[0]
    assert w["url"] == "http://bot-a.live:8020"          # the agent url itself, NOT /a2a
    body = w["body"]
    assert body["jsonrpc"] == "2.0" and body["method"] == "message/send"
    msg = body["params"]["message"]
    assert msg["role"] == "user" and msg["kind"] == "message" and msg["messageId"]
    assert msg["parts"][0] == {"kind": "text", "text": "hello bot"}


async def test_resolve_fetches_agent_card_when_directory_lists_urls_only(tmp_path):
    """🔴 Regression + REAL a2abridge contract: the /agents endpoint lists {url,lastSeen} with
    NO name — identity is only in each agent's /.well-known/agent-card.json. resolve() must fetch
    the card to get the name. The old code matched an inline `name` that isn't there → None for
    EVERY bot → every wake pinned in the hold-queue forever (held:5, drained:0). This uses a
    transport that mimics the real nameless /agents + a card endpoint; FAILS on the old resolve."""
    from app.relay import AgentDirectory

    calls = {"agents": 0, "card": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/agents":
            calls["agents"] += 1
            # exactly what a2abridge returns — url + lastSeen, NO name; includes a DEAD entry
            return httpx.Response(200, json=[
                {"url": "http://bot-a.live:8020", "lastSeen": "2026-07-07T00:00:00Z"},
                {"url": "http://bot-b.live:8060", "lastSeen": "2026-07-07T00:00:00Z"},
                {"url": "http://bot-dead.live:9999", "lastSeen": "2026-07-07T00:00:00Z"},
            ])
        if path == "/.well-known/agent-card.json":
            calls["card"] += 1
            host = request.url.host
            if host == "bot-dead.live":
                return httpx.Response(503)   # dead/unreachable agent → must be SKIPPED, not fatal
            name = {"bot-a.live": "bot-a", "bot-b.live": "bot-b"}.get(host, "?")
            return httpx.Response(200, json={"name": name, "url": f"http://{host}:{request.url.port}"})
        return httpx.Response(404)

    directory = AgentDirectory(make_config(), httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    assert await directory.resolve("bot-a") == "http://bot-a.live:8020"
    assert calls["card"] >= 1                       # it fetched the card to get the name
    assert await directory.resolve("bot-b") == "http://bot-b.live:8060"  # served from cache
    assert await directory.resolve("nobody") is None                    # unknown → None
    # the dead agent didn't break resolve for the live ones (Bragi gotcha a)


async def test_resolve_prefers_freshest_lastseen_on_duplicate_name(tmp_path):
    """Bragi gotcha (c): if a name appears twice in /agents (e.g. a bot re-registered at a new
    url), resolve() must prefer the entry with the freshest lastSeen — not deliver to the stale url."""
    from app.relay import AgentDirectory

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/agents":
            return httpx.Response(200, json=[
                {"url": "http://old.live:1", "lastSeen": "2026-07-07T00:00:00Z"},   # stale
                {"url": "http://new.live:2", "lastSeen": "2026-07-07T09:00:00Z"},   # freshest
            ])
        if request.url.path == "/.well-known/agent-card.json":
            # both urls report the SAME bot name
            return httpx.Response(200, json={"name": "bot-a", "url": str(request.url).rsplit("/.well-known", 1)[0]})
        return httpx.Response(404)

    directory = AgentDirectory(make_config(), httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    assert await directory.resolve("bot-a") == "http://new.live:2"   # freshest lastSeen wins


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
    assert wakes[0]["url"] == "http://bot-a.live:8020"
    assert wakes[0]["method"] == "message/send"

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


def test_reaction_ids_selects_incoming_reaction_and_skips_removals():
    """Reactions arrive as EventTypeIncomingReaction on the core event stream (chat_id,
    contact_id, msg_id, reaction). A non-empty reaction → the 4-tuple; an EMPTY reaction =
    the reactor removed it → None (nothing to forward). Verified vs installed deltachat2."""
    from deltachat2 import EventTypeIncomingReaction

    from app.relay import DeltaChat2Backend

    ev = EventTypeIncomingReaction(chat_id=11, contact_id=12, msg_id=17, reaction="👍")
    assert DeltaChat2Backend.reaction_ids(ev) == (11, 12, 17, "👍")
    # removal (empty reaction) → skipped
    assert DeltaChat2Backend.reaction_ids(
        EventTypeIncomingReaction(chat_id=11, contact_id=12, msg_id=17, reaction="")) is None
    # a non-reaction typed event → None
    assert DeltaChat2Backend.reaction_ids(object()) is None


def test_securejoin_ids_selects_completed_inviter_progress():
    """A securejoin completes for the INVITER (realm lead) as EventTypeSecurejoinInviterProgress
    with progress==1000 (deltachat's done sentinel). securejoin_ids returns the joiner's
    contact_id on completion, None for in-progress or the wrong event type. Verified vs installed
    deltachat2 (EventTypeSecurejoinInviterProgress.fields = chat_id,chat_type,contact_id,progress)."""
    from deltachat2 import EventTypeSecurejoinInviterProgress, EventTypeSecurejoinJoinerProgress

    from app.relay import DeltaChat2Backend

    done = EventTypeSecurejoinInviterProgress(chat_id=5, chat_type=120, contact_id=42, progress=1000)
    assert DeltaChat2Backend.securejoin_ids(done) == 42
    # in-progress (< 1000) → not yet verified → None
    assert DeltaChat2Backend.securejoin_ids(
        EventTypeSecurejoinInviterProgress(chat_id=5, chat_type=120, contact_id=42, progress=400)) is None
    # the JOINER-side progress event is not what drives lead provisioning → None
    assert DeltaChat2Backend.securejoin_ids(
        EventTypeSecurejoinJoinerProgress(contact_id=42, progress=1000)) is None
    # a non-securejoin typed event → None
    assert DeltaChat2Backend.securejoin_ids(object()) is None


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


def test_contact_to_dict_normalizes_deltachat2_contact_object():
    """🔴 Regression: deltachat2 get_contacts() returns Contact OBJECTS (.id/.address/
    .display_name), NOT ids — list_contacts must build from the object, not re-fetch by id.
    Verified vs the installed package."""
    from app.relay import DeltaChat2Backend

    class Contact:            # mimics deltachat2.types.Contact
        id = 12
        address = "bot-a@deltachat.example.net"
        display_name = "bot-a"

    assert DeltaChat2Backend._contact_to_dict(Contact()) == {
        "id": 12, "address": "bot-a@deltachat.example.net", "display_name": "bot-a"}

    class Legacy:             # tolerate addr/name fallbacks
        id = 5
        addr = "x@y.net"
        name = "x"

    d = DeltaChat2Backend._contact_to_dict(Legacy())
    assert d["address"] == "x@y.net" and d["display_name"] == "x"


# --------------------------------------------------------------------------- (8) securejoin


def test_relay_secure_join_routes_to_account(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 3})
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.secure_join("bot-a", "https://i.delta.chat/#FAKE&a=alpha%40example.net")
    assert out == {"status": "securejoin-initiated", "account_id": 3, "chat_id": 4321}
    assert backend.securejoined == [(3, "https://i.delta.chat/#FAKE&a=alpha%40example.net")]


def test_relay_secure_join_unknown_bot_raises(tmp_path):
    relay = make_relay(FakeBackend(accounts={"bot-a": 3}), [], [], tmp_path)
    with pytest.raises(KeyError):
        relay.secure_join("nobody", "https://i.delta.chat/#FAKE")


def test_secure_join_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 3})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/secure_join", json={"bot_id": "bot-a", "invite": "https://i.delta.chat/#FAKE"})
    assert resp.status_code == 200
    assert resp.json() == {"status": "securejoin-initiated", "account_id": 3, "chat_id": 4321}
    assert backend.securejoined == [(3, "https://i.delta.chat/#FAKE")]

    miss = client.post("/secure_join", json={"bot_id": "nobody", "invite": "https://i.delta.chat/#FAKE"})
    assert miss.status_code == 404


def test_relay_delete_chat_routes_to_account(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 3})
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.delete_chat("bot-a", 12)
    assert out == {"status": "deleted", "account_id": 3, "chat_id": 12}
    assert backend.deleted == [(3, 12)]


def test_relay_delete_chat_unknown_bot_raises(tmp_path):
    relay = make_relay(FakeBackend(accounts={"bot-a": 3}), [], [], tmp_path)
    with pytest.raises(KeyError):
        relay.delete_chat("nobody", 12)


def test_delete_chat_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 3})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))

    resp = client.post("/delete_chat", json={"bot_id": "bot-a", "chat_id": 12})
    assert resp.status_code == 200
    assert resp.json() == {"status": "deleted", "account_id": 3, "chat_id": 12}
    assert backend.deleted == [(3, 12)]

    miss = client.post("/delete_chat", json={"bot_id": "nobody", "chat_id": 12})
    assert miss.status_code == 404


# --------------------------------------------------------------------------- (9) messages (receipt read-back)


def test_relay_list_messages_routes_and_returns(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    backend._messages = {(7, 55): [{"id": 1, "text": "hi", "from_id": 2},
                                   {"id": 2, "text": "roundtrip", "from_id": 3}]}
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.list_messages("bot-a", 55, limit=20)
    assert out["account_id"] == 7 and out["chat_id"] == 55
    assert out["messages"][-1] == {"id": 2, "text": "roundtrip", "from_id": 3}


def test_relay_list_messages_unknown_bot_raises(tmp_path):
    relay = make_relay(FakeBackend(accounts={"bot-a": 7}), [], [], tmp_path)
    with pytest.raises(KeyError):
        relay.list_messages("nobody", 55)


def test_messages_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    backend._messages = {(7, 55): [{"id": 9, "text": "delivered", "from_id": 2}]}
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))
    resp = client.get("/messages", params={"bot_id": "bot-a", "chat_id": 55})
    assert resp.status_code == 200
    body = resp.json()
    assert body["account_id"] == 7 and body["chat_id"] == 55
    assert body["messages"] == [{"id": 9, "text": "delivered", "from_id": 2}]

    miss = client.get("/messages", params={"bot_id": "bot-c", "chat_id": 1})
    assert miss.status_code == 404


def test_relay_create_invite_routes_and_returns(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path)
    out = relay.create_invite("bot-a")
    assert out == {"account_id": 7, "invite": "https://i.delta.chat/#FAKEINVITE-acc7"}


def test_invite_endpoint(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    client = TestClient(create_app(make_relay(backend, [], [], tmp_path)))
    resp = client.get("/invite", params={"bot_id": "bot-a"})
    assert resp.status_code == 200
    assert resp.json() == {"account_id": 7, "invite": "https://i.delta.chat/#FAKEINVITE-acc7"}
    miss = client.get("/invite", params={"bot_id": "bot-c"})
    assert miss.status_code == 404
