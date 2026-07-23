"""Tests for the messaging-reliability fixes (stable taskId, wake-dedup-on-success,
reaction self-exclusion, hold-queue key + TTL, directory refresh robustness).

Reuses the FakeBackend + httpx.MockTransport scaffolding from test_relay.py.
"""
from __future__ import annotations

import httpx
import pytest

from app.config import BotSpec, Config
from app.relay import (
    AgentDirectory, HoldQueue, InboundMessage, InboundReaction, Relay,
    _stable_task_id,
)

from tests.test_relay import FakeBackend, make_config, make_relay, directory_transport


def _wake_task_id(wake: dict) -> str:
    return wake["body"]["params"]["message"].get("taskId", "")


# --- random-taskid-defeats-dedup -------------------------------------------------------------

async def test_wake_sets_stable_taskid_from_rfc724_mid(tmp_path):
    """The wake envelope MUST carry a deterministic taskId derived from the GLOBAL rfc724_mid.
    Discriminates: BEFORE, the relay set only messageId (random uuid4) and never taskId, so the
    bridge minted a fresh random taskId per delivery and the dedup digest was unique every time.
    Now params.message.taskId is present and stable."""
    msg = InboundMessage(account_id=7, chat_id=11, msg_id=16, text="ping",
                         is_group=False, members=[], mentioned=[],
                         from_localpart="terafin", rfc724_mid="<abc@chatmail>")
    backend = FakeBackend(accounts={"bot-a": 7}, inbound=[msg])
    wakes: list[dict] = []
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)

    await relay.handle_inbound(msg)

    tid = _wake_task_id(wakes[0])
    assert tid, "wake envelope carries NO taskId — the bridge will mint a random one per delivery"
    assert tid == "bot-a:<abc@chatmail>", f"taskId not the stable producer id: {tid!r}"


async def test_stable_task_id_is_deterministic_and_scoped():
    p = {"rfc724_mid": "<m1@x>", "chat_id": 5, "msg_id": 9}
    # Same message → same id for a given target; different target → different id.
    assert _stable_task_id("bragi", p) == _stable_task_id("bragi", p)
    assert _stable_task_id("bragi", p) != _stable_task_id("idunn", p)
    # No rfc724_mid → fall back to chat:msg (still stable), never empty.
    assert _stable_task_id("bragi", {"chat_id": 5, "msg_id": 9}) == "bragi:5:9"
    # Distinct reactions to one message get distinct ids (emoji + reactor discriminator).
    r1 = {"rfc724_mid": "<m@x>", "reaction": "👍", "from": "alice@x"}
    r2 = {"rfc724_mid": "<m@x>", "reaction": "🎉", "from": "alice@x"}
    assert _stable_task_id("bragi", r1) != _stable_task_id("bragi", r2)


# --- wake-once-recorded-before-delivery ------------------------------------------------------

async def test_failed_wake_is_not_marked_seen_so_redelivery_still_wakes(tmp_path):
    """A first wake that FAILS to deliver (held) must NOT suppress a genuine re-delivery of the
    same message within the dedup TTL. Discriminates: BEFORE, _wake_once recorded 'seen' at
    handling time regardless of delivery success, so the re-delivery was silently dropped."""
    msg = InboundMessage(account_id=7, chat_id=11, msg_id=16, text="ping",
                         is_group=False, members=[], mentioned=[],
                         from_localpart="terafin", rfc724_mid="<retry@chatmail>")
    backend = FakeBackend(accounts={"bot-a": 7})
    wakes: list[dict] = []
    # directory_status=500 → resolve() returns None → _deliver fails → held (first attempt fails).
    relay = make_relay(backend, [], wakes, tmp_path, directory_status=500)

    woken = await relay.handle_inbound(msg)
    assert woken == [], "first attempt should have failed (held)"
    assert len(wakes) == 0

    # Now the directory comes up: rebuild the relay with a live directory but the SAME dedup state
    # would matter only if it were committed. Simulate the re-delivery on a relay whose dedup was
    # touched by the failed attempt — use the same relay object with a now-working transport.
    relay.directory.client = httpx.AsyncClient(
        transport=directory_transport([{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes)
    )
    relay.directory._name_to_url = {}
    relay.directory._refreshed_at = 0.0
    woken2 = await relay.handle_inbound(msg)
    assert woken2 == ["bot-a"], "re-delivery after a failed first attempt was wrongly suppressed"
    assert len(wakes) == 1


async def test_successful_wake_dedups_a_true_duplicate(tmp_path):
    """A successfully-delivered wake DOES suppress an immediate duplicate of the same message
    (the N-member amplification collapse still works)."""
    msg = InboundMessage(account_id=7, chat_id=11, msg_id=16, text="ping",
                         is_group=False, members=[], mentioned=[],
                         from_localpart="terafin", rfc724_mid="<dup@chatmail>")
    backend = FakeBackend(accounts={"bot-a": 7})
    wakes: list[dict] = []
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)

    assert await relay.handle_inbound(msg) == ["bot-a"]
    assert await relay.handle_inbound(msg) == [], "duplicate was not deduped"
    assert len(wakes) == 1


# --- reaction-no-self-exclusion --------------------------------------------------------------

async def test_reaction_by_own_bot_does_not_self_wake(tmp_path):
    """A bot reacting to its OWN message (e.g. the 👀 proof-of-life) must not self-wake.
    Discriminates: BEFORE, handle_reaction never checked whether the reactor == the bot."""
    backend = FakeBackend(accounts={"bot-a": 7})
    wakes: list[dict] = []
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)
    r = InboundReaction(account_id=7, chat_id=11, msg_id=16, emoji="👀",
                        from_addr="bot-a@deltachat.example.net", own_message=True,
                        rfc724_mid="<self@chatmail>")
    woken = await relay.handle_reaction(r)
    assert woken == [], "bot self-wakes on its own reaction (feedback loop)"
    assert len(wakes) == 0


async def test_reaction_by_other_still_wakes_author(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7})
    wakes: list[dict] = []
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)
    r = InboundReaction(account_id=7, chat_id=11, msg_id=16, emoji="👍",
                        from_addr="alice@human.example", own_message=True,
                        rfc724_mid="<react@chatmail>")
    assert await relay.handle_reaction(r) == ["bot-a"]


async def test_distinct_reactions_to_same_message_both_wake(tmp_path):
    """Two different reactions (different emoji) to the same message must each wake — the reaction
    dedup key includes emoji+reactor, not just the reacted-to message's shared rfc724_mid."""
    backend = FakeBackend(accounts={"bot-a": 7})
    wakes: list[dict] = []
    relay = make_relay(backend, [{"name": "bot-a", "url": "http://bot-a.live:8020"}], wakes, tmp_path)
    r1 = InboundReaction(account_id=7, chat_id=11, msg_id=16, emoji="👍",
                         from_addr="alice@human.example", own_message=True, rfc724_mid="<m@x>")
    r2 = InboundReaction(account_id=7, chat_id=11, msg_id=16, emoji="🎉",
                         from_addr="bob@human.example", own_message=True, rfc724_mid="<m@x>")
    assert await relay.handle_reaction(r1) == ["bot-a"]
    assert await relay.handle_reaction(r2) == ["bot-a"], "second distinct reaction was collapsed"
    assert len(wakes) == 2


# --- holdqueue-key-collision -----------------------------------------------------------------

def test_holdqueue_key_distinguishes_reaction_from_message(tmp_path):
    """A held reaction and a held message for the same (bot,chat,msg) must NOT collide.
    Discriminates: BEFORE, _key was (bot,chat,msg) so the second add() was silently dropped."""
    hq = HoldQueue(str(tmp_path))
    hq.add("bragi", {"chat_id": 5, "msg_id": 9, "rfc724_mid": "<m@x>", "text": "a message"})
    hq.add("bragi", {"chat_id": 5, "msg_id": 9, "rfc724_mid": "<m@x>",
                     "reaction": "👍", "from": "alice@x", "text": "reacted"})
    assert len(hq) == 2, "reaction wake collided with the message wake and was dropped"


def test_holdqueue_key_distinguishes_two_reactions(tmp_path):
    hq = HoldQueue(str(tmp_path))
    hq.add("bragi", {"chat_id": 5, "msg_id": 9, "rfc724_mid": "<m@x>", "reaction": "👍", "from": "a@x"})
    hq.add("bragi", {"chat_id": 5, "msg_id": 9, "rfc724_mid": "<m@x>", "reaction": "🎉", "from": "b@x"})
    assert len(hq) == 2


def test_holdqueue_true_duplicate_still_idempotent(tmp_path):
    hq = HoldQueue(str(tmp_path))
    payload = {"chat_id": 5, "msg_id": 9, "rfc724_mid": "<m@x>", "text": "dup"}
    hq.add("bragi", payload)
    hq.add("bragi", payload)
    assert len(hq) == 1, "an identical event should still dedup"


# --- holdqueue-unbounded ---------------------------------------------------------------------

def test_holdqueue_ages_out_stale_entries(tmp_path):
    """A wake for a departed bot must age out, not be retried forever. Discriminates: BEFORE,
    HoldQueue had no TTL (PeerMesh did) and pending() returned stale items indefinitely."""
    hq = HoldQueue(str(tmp_path), ttl=0.001)
    hq.add("gone-bot", {"chat_id": 5, "msg_id": 9, "rfc724_mid": "<old@x>", "text": "stale"})
    assert len(hq) == 1
    import time as _t
    _t.sleep(0.01)
    assert hq.pending() == [], "stale hold entry was not aged out"


# --- directory-refresh-no-throttle-on-empty --------------------------------------------------

async def test_refresh_advances_timestamp_on_empty(tmp_path):
    """_refresh must advance _refreshed_at even when the directory GET fails/returns empty, so a
    subsequent resolve() within the TTL does NOT re-fire a refresh storm. Discriminates: BEFORE,
    the timestamp only advanced on a non-empty mapping → every resolve re-fired _refresh."""
    config = make_config()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, json={})   # directory down

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    d = AgentDirectory(config, client)
    await d.resolve("bot-a")          # miss → 1 refresh (1 GET)
    n_after_first = calls["n"]
    await d.resolve("bot-a")          # within TTL → must NOT refresh again
    assert calls["n"] == n_after_first, "resolve re-fired _refresh during an outage (retry storm)"


async def test_refresh_merges_keeps_previous_url_on_partial_failure(tmp_path):
    """A bot whose card times out on a later refresh keeps its previously-known URL instead of
    vanishing from the map. Discriminates: BEFORE, _refresh rebuilt from scratch and dropped it."""
    config = make_config()
    state = {"pass": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/agents"):
            return httpx.Response(200, json={"agents": [
                {"name": "bot-a", "url": "http://bot-a.live:8020"},
            ]})
        # agent-card fetch: succeed on pass 0 (inline name is present anyway), but the inline
        # name in the directory entry means _agent_name never fetches the card, so simulate a
        # transient by returning 500 for the card GET — not reached given inline name.
        return httpx.Response(500, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    d = AgentDirectory(config, client)
    assert await d.resolve("bot-a") == "http://bot-a.live:8020"
    # Force a refresh where the directory now returns EMPTY (all bots timed out this pass).
    d._name_to_url  # sanity
    d._refreshed_at = 0.0  # force TTL expiry

    def empty_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"agents": []})

    d.client = httpx.AsyncClient(transport=httpx.MockTransport(empty_handler))
    # merge-not-replace: bot-a's previous URL survives an empty pass.
    assert await d.resolve("bot-a") == "http://bot-a.live:8020", "previous URL dropped on empty refresh"


# --- flush-with-suffix-inconsistent ----------------------------------------------------------

def test_holdqueue_flush_temp_name_multidot_safe(tmp_path):
    """The atomic temp must be <name>.tmp, not with_suffix('.json.tmp'). Assert the durable
    write round-trips (a multi-dot name would break with_suffix)."""
    hq = HoldQueue(str(tmp_path))
    hq.add("bot-a", {"chat_id": 1, "msg_id": 1, "rfc724_mid": "<m@x>", "text": "t"})
    hq2 = HoldQueue(str(tmp_path))  # re-read from disk
    assert len(hq2) == 1
