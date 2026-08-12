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
    """Minimal BasicChat stand-in carrying exactly the fields _resolve_chat_id reads."""

    def __init__(self, chat_id: int, is_device_chat: bool = False, is_self_talk: bool = False):
        self.chat_id = chat_id
        self.is_device_chat = is_device_chat
        self.is_self_talk = is_self_talk


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

    # Account-index surface the backend init calls (_reindex_accounts).
    def get_all_account_ids(self) -> list[int]:
        return []

    def get_config(self, _accid: int, _key: str):
        return None

    def get_basic_chat_info(self, _accid: int, chat_id: int) -> _BasicChat:
        return self.chats.get(chat_id, _BasicChat(chat_id=0))

    def get_chat_id_by_contact_id(self, _accid: int, cid: int) -> Optional[int]:
        return self.contact_chats.get(cid)

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


# --------------------------------------------------------------------------- F3: durable outbox


class _FlakyBackend(FakeBackend):
    """Backend that raises a transient transport error on send until ``healthy``."""

    def __init__(self, accounts: dict[str, int]):
        super().__init__(accounts)
        self.healthy = False
        self.attempts = 0

    def send(self, account_id: int, chat_id: int, text: str) -> int:
        self.attempts += 1
        if not self.healthy:
            raise RuntimeError("chatmail transport down (transient)")
        return super().send(account_id, chat_id, text)


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
