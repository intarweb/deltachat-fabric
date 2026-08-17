"""Messaging-layer fixes — device/system suppression, delta_send target disambiguation,
durable outbox, and the stable content-addressed taskId fallback.

No live rpc-server, no network: FakeBackend + httpx.MockTransport, exactly like the rest
of the suite.
"""
from __future__ import annotations

from typing import Optional

import httpx
import pytest

from app.relay import (
    AgentDirectory,
    DeltaChat2Backend,
    HoldQueue,
    InboundMessage,
    Outbox,
    Relay,
)
from tests.test_relay import FakeBackend, directory_transport, make_config, make_relay


# --------------------------------------------------------------------------- F1: device/system suppression


async def test_handle_inbound_skips_device_chat_notice(tmp_path):
    """A device-chat notice (e.g. chatmail login failure) is NOT a human DM: no wake, no
    reply_target. The poison device-chat reply handle + the random-taskId re-wake loop both
    originate here — suppressing at the source removes both."""
    msg = InboundMessage(account_id=7, chat_id=4, msg_id=11, text="Cannot login as ...",
                         is_group=False, is_device_chat=True, from_id=5)
    wakes: list[dict] = []
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == []
    assert wakes == []          # nothing POSTed — no wake, no reply_target, no taskId


async def test_handle_inbound_skips_system_message_in_human_chat(tmp_path):
    """A core system/info message in a real chat is never human-authored: suppress the wake."""
    msg = InboundMessage(account_id=7, chat_id=4, msg_id=11, text="Contact verified",
                         is_group=False, is_system=True, from_id=5)
    wakes: list[dict] = []
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == []
    assert wakes == []


async def test_handle_inbound_still_wakes_human_dm(tmp_path):
    """A genuine human DM (not device, not system) still wakes the receiving bot."""
    msg = InboundMessage(account_id=7, chat_id=4, msg_id=11,
                         text="hello from a person", is_group=False,
                         from_localpart="justin", rfc724_mid="m1@example")
    wakes: list[dict] = []
    backend = FakeBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a:8020"}], wakes, tmp_path)

    woken = await relay.handle_inbound(msg)

    assert woken == ["bot-a"]
    assert len(wakes) == 1
    assert wakes[0]["body"]["params"]["message"]["taskId"] == "bot-a:m1@example"


# --------------------------------------------------------------------------- F2: delta_send target disambiguation


class _BasicChat:
    """Minimal BasicChat stand-in carrying exactly the fields _resolve_chat_id reads.

    Mirrors the REAL deltachat2 ``BasicChat`` schema: the chat id field is ``id``, not
    ``chat_id`` (a mismatch here caused every real strict send to reject with 'no such chat')."""

    def __init__(self, chat_id: int, is_device_chat: bool = False, is_self_talk: bool = False):
        self.id = chat_id
        self.is_device_chat = is_device_chat
        self.is_self_talk = is_self_talk


class _BasicContact:
    """Minimal Contact stand-in carrying the address/key fields send paths read."""

    def __init__(self, cid: int, address: str = "", is_key_contact: bool = False,
                 is_verified: bool = False):
        self.id = cid
        self.address = address or f"contact{cid}@chatmail.example"
        self.is_key_contact = is_key_contact
        self.is_verified = is_verified


class _StubRpc:
    """In-memory rpc stand-in: get_basic_chat_info / get_chat_id_by_contact_id /
    create_chat_by_contact_id / send_msg, with a fake chats/contacts namespace."""

    def __init__(self):
        self.sent: list[tuple[int, int, str]] = []
        # chat_id -> BasicChat (absent = not a chat)
        self.chats: dict[int, _BasicChat] = {}
        # contact_id -> existing 1:1 chat_id (absent = none)
        self.contact_chats: dict[int, int] = {}
        self.created: list[int] = []
        # contact ids that EXIST (get_contact resolves them; absent = unknown contact)
        self.contacts: set[int] = set()
        # contacts reachable by get_contacts(query): lowercased address -> [contact,...]
        self.address_contacts: dict[str, list] = {}

    # Account-index surface the backend init calls (_reindex_accounts).
    def get_all_account_ids(self) -> list[int]:
        return []

    def get_config(self, _accid: int, _key: str):
        return None

    def get_basic_chat_info(self, _accid: int, chat_id: int) -> _BasicChat:
        return self.chats.get(chat_id, _BasicChat(chat_id=0))

    def get_chat_id_by_contact_id(self, _accid: int, cid: int) -> Optional[int]:
        return self.contact_chats.get(cid)

    def get_contact(self, _accid: int, cid: int):
        """Resolve a contact id; raises on an unknown id (mirrors the real core)."""
        if cid not in self.contacts:
            raise KeyError(f"contact {cid} not found")
        return _BasicContact(cid)

    def get_contacts(self, _accid: int, _flags: int, query: str) -> list:
        """Enumerate contacts matching ``query`` (address substring) — the reliable path."""
        q = (query or "").strip().lower()
        if not q:
            return [self.get_contact(_accid, cid) for cid in sorted(self.contacts)]
        return [c for c in self.address_contacts.get(q, [])]

    def lookup_contact_id_by_addr(self, _accid: int, addr: str) -> Optional[int]:
        """Simulate the REAL core's unreliable by-address lookup: deliberately MISSES even a
        listed contact (deltachat-core: 'do not use to look them up'). A call to send_to_addr
        that relied on this would 404 despite the contact existing."""
        return None

    def create_chat_by_contact_id(self, _accid: int, cid: int) -> int:
        # mint a 1:1 chat (id = 1000 + cid) and remember it
        chat = self.contact_chats.get(cid)
        if chat is None:
            chat = 1000 + cid
            self.contact_chats[cid] = chat
            self.chats[chat] = _BasicChat(chat)
            self.created.append(cid)
        return chat

    def send_msg(self, accid: int, chat_id: int, _data) -> int:
        self.sent.append((accid, chat_id, ""))
        return len(self.sent) + 500


def _backend_with_rpc() -> tuple[DeltaChat2Backend, _StubRpc]:
    rpc = _StubRpc()
    backend = DeltaChat2Backend(make_config(), "/tmp/df-fix-test", _rpc=rpc)
    return backend, rpc


def test_backend_send_uses_live_chat_directly():
    backend, rpc = _backend_with_rpc()
    rpc.chats[77] = _BasicChat(77)

    backend.send(1, 77, "hi")

    assert rpc.sent == [(1, 77, "")]


def test_backend_send_refuses_device_chat_with_typed_error():
    backend, rpc = _backend_with_rpc()
    rpc.chats[22] = _BasicChat(22, is_device_chat=True)

    with pytest.raises(TypeError, match="device/self-talk chat"):
        backend.send(1, 22, "hi")

    assert rpc.sent == []       # nothing hit the core


def test_backend_send_resolves_contact_id_with_existing_chat():
    backend, rpc = _backend_with_rpc()
    rpc.chats[33] = _BasicChat(33)
    rpc.contact_chats[55] = 33   # contact 55 → existing 1:1 chat 33

    backend.send(1, 55, "hi")

    assert rpc.sent == [(1, 33, "")]


def test_backend_send_resolves_contact_id_creating_chat():
    backend, rpc = _backend_with_rpc()
    # contact 55 has NO 1:1 chat yet → create_chat_by_contact_id mints one
    backend.send(1, 55, "hi")

    assert 55 in rpc.created
    assert rpc.sent == [(1, 1055, "")]


# --------------------------------------------------------------------------- F2b: STRICT send paths
# (fleet-messaging #16 split-tools contract) — a numeric id must never silently cross the
# chat↔contact namespace boundary. send_chat = chat-only, send_contact = contact-only, and
# send_channel uses the strict chat path (no disambiguation fallback).


def test_backend_send_chat_uses_live_chat_directly():
    backend, rpc = _backend_with_rpc()
    rpc.chats[77] = _BasicChat(77)

    backend.send_chat(1, 77, "hi")

    assert rpc.sent == [(1, 77, "")]
    assert rpc.created == []   # no contact resolution happened


def test_backend_send_chat_refuses_device_chat_with_typed_error():
    backend, rpc = _backend_with_rpc()
    rpc.chats[22] = _BasicChat(22, is_device_chat=True)

    with pytest.raises(TypeError, match="device/self-talk chat"):
        backend.send_chat(1, 22, "hi")

    assert rpc.sent == []      # nothing hit the core


# The (a)/(d) fix at the backend level: send_chat to an id that is NOT a live chat must
# fail loud — it must NOT fall back to the contact path (which is how a stale channel id
# silently re-resolved to a contact's 1:1 chat and acked 'sent' into the wrong chat).
def test_backend_send_chat_does_not_resolve_contact_id():
    backend, rpc = _backend_with_rpc()
    # 55 is a CONTACT (its 1:1 chat is 33) but NOT a chat id on this account
    rpc.contact_chats[55] = 33
    rpc.chats[33] = _BasicChat(33)

    with pytest.raises(KeyError, match="no such chat"):
        backend.send_chat(1, 55, "hi")

    assert rpc.sent == []      # the legacy send() WOULD have sent to chat 33 here


def test_backend_send_contact_resolves_to_chat():
    backend, rpc = _backend_with_rpc()
    rpc.contacts.add(55)
    rpc.contact_chats[55] = 33   # existing 1:1 chat
    rpc.chats[33] = _BasicChat(33)

    backend.send_contact(1, 55, "hi")

    assert rpc.sent == [(1, 33, "")]
    assert rpc.created == []


def test_backend_send_contact_creates_chat_when_none():
    backend, rpc = _backend_with_rpc()
    rpc.contacts.add(55)         # contact exists, no 1:1 chat yet

    backend.send_contact(1, 55, "hi")

    assert 55 in rpc.created
    assert rpc.sent == [(1, 1055, "")]


def test_backend_send_contact_refuses_unknown_contact():
    backend, rpc = _backend_with_rpc()
    # 55 does NOT exist as a contact (not in rpc.contacts) — and it IS not a chat either
    rpc.chats[33] = _BasicChat(33)

    with pytest.raises(KeyError, match="no contact id"):
        backend.send_contact(1, 55, "hi")

    assert rpc.sent == []


# The (b) fix at the backend level: send_to_addr must resolve by ENUMERATION, not the
# unreliable lookup_contact_id_by_addr (deltachat-core: "do not use to look them up" — it can
# miss a listed contact entirely, 404ing a send the enumeration clearly contains). The stub's
# lookup_contact_id_by_addr is DELIBERATELY a miss, so this test only passes via enumeration.
def test_backend_send_to_addr_resolves_by_enumeration_when_lookup_misses():
    backend, rpc = _backend_with_rpc()
    rpc.contacts.update({13, 7})
    rpc.address_contacts["brokkr@chatmail.siliconspirit.net"] = [
        _BasicContact(13, "brokkr@chatmail.siliconspirit.net",
                      is_key_contact=True, is_verified=True),
    ]
    rpc.contact_chats[13] = 33
    rpc.chats[33] = _BasicChat(33)

    chat_id, msg_id = backend.send_to_addr(1, "brokkr@chatmail.siliconspirit.net", "hi")

    assert chat_id == 33 and msg_id > 0
    assert rpc.sent == [(1, 33, "")]


def test_backend_send_to_addr_refuses_unlisted_address():
    backend, rpc = _backend_with_rpc()
    rpc.contacts.add(7)  # only contact 7 exists; nobody@ is absent from the enumeration

    with pytest.raises(KeyError, match="no contact for address"):
        backend.send_to_addr(1, "nobody@chatmail.siliconspirit.net", "hi")

    assert rpc.sent == []


def test_backend_send_to_addr_prefers_verified_key_contact():
    backend, rpc = _backend_with_rpc()
    rpc.contacts.update({13, 9})
    addr = "brokkr@chatmail.siliconspirit.net"
    # two contacts share the address: an old address-contact and a verified key-contact
    rpc.address_contacts[addr] = [
        _BasicContact(9, addr, is_key_contact=False, is_verified=False),
        _BasicContact(13, addr, is_key_contact=True, is_verified=True),
    ]
    rpc.contact_chats[13] = 33
    rpc.chats[33] = _BasicChat(33)

    chat_id, _ = backend.send_to_addr(1, addr, "hi")

    assert chat_id == 33            # resolved via the verified key-contact (13), not the stale one


def test_backend_send_to_addr_refuses_ambiguous_unverified_matches():
    """No verified key-contact + MULTIPLE unverified contacts sharing the address is a
    wrong-recipient door — refuse-and-say-which, never silently pick 'the last one'."""
    backend, rpc = _backend_with_rpc()
    addr = "dupe@chatmail.siliconspirit.net"
    rpc.contacts.update({5, 6})
    rpc.address_contacts[addr] = [
        _BasicContact(5, addr, is_key_contact=False, is_verified=False),
        _BasicContact(6, addr, is_key_contact=False, is_verified=False),
    ]

    with pytest.raises(KeyError, match="ambiguous address"):
        backend.send_to_addr(1, addr, "hi")

    assert rpc.sent == []           # nothing hit the core — no guess between duplicates


def test_backend_send_to_addr_sends_single_unverified_match():
    """A SINGLE unverified match is unambiguous → send (no refusal regression for the normal
    one-contact-per-address fleet state; only the multi-match ambiguous case refuses)."""
    backend, rpc = _backend_with_rpc()
    addr = "solo@chatmail.siliconspirit.net"
    rpc.contacts.add(5)
    rpc.address_contacts[addr] = [
        _BasicContact(5, addr, is_key_contact=False, is_verified=False),
    ]
    rpc.contact_chats[5] = 33
    rpc.chats[33] = _BasicChat(33)

    chat_id, msg_id = backend.send_to_addr(1, addr, "hi")

    assert chat_id == 33 and msg_id > 0
    assert rpc.sent == [(1, 33, "")]


# --------------------------------------------------------------------------- F3: durable outbox


class _FlakyBackend(FakeBackend):
    """Backend that raises a transient transport error on send until ``healthy``.

    Overrides the STRICT paths (send_chat) too: the drain retries a parked kind='chat' entry
    on the strict chat path (never the legacy disambiguating ``send``), so the flakiness must
    live there for the retry test to exercise the real drain behavior."""

    def __init__(self, accounts: dict[str, int]):
        super().__init__(accounts)
        self.healthy = False
        self.attempts = 0

    def send(self, account_id: int, chat_id: int, text: str) -> int:
        self.attempts += 1
        if not self.healthy:
            raise RuntimeError("chatmail transport down (transient)")
        return super().send(account_id, chat_id, text)

    def send_chat(self, account_id: int, chat_id: int, text: str) -> int:
        self.attempts += 1
        if not self.healthy:
            raise RuntimeError("chatmail transport down (transient)")
        return super().send_chat(account_id, chat_id, text)


def test_outbox_persists_then_drains(tmp_path):
    backend = _FlakyBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path, outbox=Outbox(str(tmp_path)))

    # First send fails transiently → parked, reported as queued (NOT dropped, NOT a 502).
    result = relay.send("bot-a", 5, "important message")
    assert result["status"] == "queued"
    assert len(relay._get_outbox().pending()) == 1
    assert backend.sent == []

    # Still down → drain retries and keeps it (attempts++).
    relay.drain_outbox()
    assert len(relay._get_outbox().pending()) == 1

    # Transport recovers → drain sends and removes it.
    backend.healthy = True
    assert relay.drain_outbox() == 0
    assert backend.sent == [(7, 5, "important message")]
    assert relay._get_outbox().pending() == []


def test_outbox_survives_reload(tmp_path):
    """The outbox is durable on disk — a process reload re-reads the parked message."""
    backend = _FlakyBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path, outbox=Outbox(str(tmp_path)))
    relay.send("bot-a", 5, "parked during outage")
    assert len(relay._get_outbox().pending()) == 1

    outbox2 = Outbox(str(tmp_path))     # brand-new instance == process reload
    assert len(outbox2.pending()) == 1
    assert outbox2.pending()[0]["text"] == "parked during outage"


def test_outbox_drops_permanent_failure_loudly(tmp_path):
    class _DeviceChatBackend(FakeBackend):
        def send(self, *a):
            raise TypeError("target 22 is a device/self-talk chat")

    backend = _DeviceChatBackend(accounts={"bot-a": 7})
    relay = make_relay(backend, [], [], tmp_path, outbox=Outbox(str(tmp_path)))
    relay._get_outbox().enqueue({"kind": "chat", "bot": "bot-a", "target": 22,
                                 "text": "hi", "key": "k1", "attempts": 0})

    relay.drain_outbox()

    assert relay._get_outbox().pending() == []   # permanent error → dropped, not retried forever


# --------------------------------------------------------------------------- F4: stable content-addressed taskId fallback


async def test_wake_content_addressed_taskid_when_no_stable_ids(tmp_path):
    """A payload with NO rfc724_mid / chat_id / msg_id must STILL get a stable producer-owned
    taskId (content-addressed) — never a fresh random uuid that defeats the dedup digest."""
    from app.relay import AgentDirectory

    wakes: list[dict] = []
    client = httpx.AsyncClient(
        transport=directory_transport([{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes)
    )
    directory = AgentDirectory(make_config(), client)
    payload = {"text": "[Delta Chat] hello from someone", "from": "someone"}
    await directory.wake("http://bot-a.live:8020", "bot-a", payload)

    msg = wakes[0]["body"]["params"]["message"]
    task_id = msg["taskId"]
    assert task_id.startswith("bot-a:content:")
    assert msg["messageId"] == task_id           # messageId mirrors the stable id → bridge dedup stable
    # Determinism: the same payload → the same id (this is the whole point).
    wakes2: list[dict] = []
    client2 = httpx.AsyncClient(
        transport=directory_transport([{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes2)
    )
    await AgentDirectory(make_config(), client2).wake("http://bot-a.live:8020", "bot-a", payload)
    assert wakes2[0]["body"]["params"]["message"]["taskId"] == task_id
