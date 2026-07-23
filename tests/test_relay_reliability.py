"""Tests for the messaging-reliability fixes (stable taskId, wake-dedup-on-success,
reaction self-exclusion, hold-queue key + TTL, directory refresh robustness).

Reuses the FakeBackend + httpx.MockTransport scaffolding from test_relay.py.
"""
from __future__ import annotations

import asyncio

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


class _YieldingWakeTransport(httpx.AsyncBaseTransport):
    """Async transport that SUSPENDS at the POST (wake) with ``await asyncio.sleep(0)`` so that
    concurrent handler copies genuinely overlap on the loop before any of them completes its POST
    (and therefore before any commits its dedup entry). GETs return the agent directory.

    This is the discriminator the plain sync ``httpx.MockTransport`` misses: its POST handler
    returns without a suspension point, so overlapping copies never actually interleave at the
    await and the check-then-commit race can't be observed."""

    def __init__(self, agents: list[dict], wake_sink: list[dict]):
        self._agents = agents
        self._wakes = wake_sink

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        import json
        if request.method == "GET":
            return httpx.Response(200, json={"agents": self._agents})
        # POST = wake delivery. Yield the loop FIRST so any sibling copy already past its dedup
        # gate is allowed to run — this is where a check-then-commit (rather than reserve-then-
        # commit) design leaks duplicate POSTs.
        await asyncio.sleep(0)
        body = json.loads(request.content.decode() or "{}")
        text = ""
        try:
            text = body["params"]["message"]["parts"][0]["text"]
        except Exception:
            pass
        self._wakes.append({"url": str(request.url), "body": body,
                            "method": body.get("method"), "text": text})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body.get("id"), "result": {}})


def _make_yielding_relay(backend: FakeBackend, agents: list[dict], wake_sink: list[dict],
                         tmp_path) -> Relay:
    config = make_config()
    client = httpx.AsyncClient(transport=_YieldingWakeTransport(agents, wake_sink))
    directory = AgentDirectory(config, client)
    hold = HoldQueue(str(tmp_path))
    return Relay(config, backend, directory, hold)


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


async def test_concurrent_member_copies_collapse_to_one_wake(tmp_path):
    """N per-member copies of ONE group message, dispatched fire-and-forget (main.py's
    _event_pump/_default_submit no longer awaits .result()), run concurrently on the loop. Each
    copy carries the SAME rfc724_mid and targets the SAME channel-main. They MUST collapse to a
    single wake POST.

    Discriminates: with a read-only 'seen' gate committed only AFTER the awaited POST, every
    overlapping copy clears the gate before ANY commits → N duplicate POSTs. The reserve-before-
    deliver design claims the dedup key synchronously, so exactly one copy wins.

    The _YieldingWakeTransport is essential: it suspends at the POST (await asyncio.sleep(0)) so
    the copies genuinely interleave — the plain MockTransport completes POSTs without a suspension
    point and would mask the race even in the broken design."""
    members = ["bot-lead", "bot-a", "bot-b"]
    # No @mention → wake only the channel main (bot-lead); every member-account copy sees the same
    # message and would independently wake bot-lead.
    msg = InboundMessage(account_id=0, chat_id=11, msg_id=16, text="team status?",
                         is_group=True, members=members, mentioned=[],
                         from_localpart="terafin", rfc724_mid="<group@chatmail>")
    backend = FakeBackend(accounts={"bot-lead": 1, "bot-a": 2, "bot-b": 3})
    wakes: list[dict] = []
    agents = [{"name": m, "url": f"http://{m}.live:8020"} for m in members]
    relay = _make_yielding_relay(backend, agents, wakes, tmp_path)

    # Pre-warm the directory so resolve() returns from cache with NO refresh await — otherwise the
    # directory's in-flight-refresh guard (one GET, siblings return early to an empty map) would
    # collapse the copies for the WRONG reason and mask the wake-dedup race. After this, the ONLY
    # suspension point inside handle_inbound is the wake POST — exactly where the race lives.
    assert await relay.directory.resolve("bot-lead") == "http://bot-lead.live:8020"

    # Fire N concurrent copies of the SAME message (one per member account surfacing it).
    results = await asyncio.gather(*[relay.handle_inbound(msg) for _ in members])

    assert len(wakes) == 1, f"N-member amplification not collapsed: {len(wakes)} wakes"
    # Exactly one copy reports having woken bot-lead; the rest collapse to [].
    woke_lead = [r for r in results if r == ["bot-lead"]]
    assert len(woke_lead) == 1, f"expected exactly one winning copy, got {results}"
    assert all(r in (["bot-lead"], []) for r in results), results



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
    """A bot whose agent-card fetch FAILS (500) on a later refresh keeps its previously-known URL
    instead of vanishing from the map. Discriminates: BEFORE, _refresh rebuilt from scratch and
    dropped it.

    Genuinely exercises the 500 card-GET handler: the second refresh lists bot-a WITHOUT an inline
    name, so _agent_name must fetch /.well-known/agent-card.json — which returns 500 → the name
    resolves to '' → bot-a is absent from THIS pass's freshly-derived names. Merge-not-replace must
    therefore be what preserves the previous URL. (The prior version listed an inline name on the
    second pass too, so the card GET was never reached and the assertion proved nothing about the
    failure path.)"""
    config = make_config()
    card_gets = {"n": 0}

    # Pass 0: directory lists bot-a WITH an inline name → resolves without any card fetch.
    def first_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"agents": [
            {"name": "bot-a", "url": "http://bot-a.live:8020"},
        ]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(first_handler))
    d = AgentDirectory(config, client)
    assert await d.resolve("bot-a") == "http://bot-a.live:8020"

    # Pass 1: directory lists TWO bots. bot-b resolves (inline name) so the freshly-derived mapping
    # is non-empty (the 'if mapping' short-circuit does NOT save us here) — bot-a is listed WITHOUT
    # an inline name so _agent_name fetches its card, which 500s → bot-a derives to '' this pass.
    # Only merge-not-replace can keep bot-a's previously-known URL.
    def partial_fail_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/agents"):
            return httpx.Response(200, json={"agents": [
                {"url": "http://bot-a.live:8020"},                       # no inline name → card GET
                {"name": "bot-b", "url": "http://bot-b.live:8020"},      # resolves fine this pass
            ]})
        # agent-card GET (bot-a): transient failure — this branch is now genuinely reached.
        card_gets["n"] += 1
        return httpx.Response(500, json={})

    d.client = httpx.AsyncClient(transport=httpx.MockTransport(partial_fail_handler))
    d._refreshed_at = 0.0  # force TTL expiry → next resolve refreshes

    # merge-not-replace: bot-a's previous URL survives its card fetch failing this pass, even though
    # the freshly-derived mapping (bot-b) is non-empty.
    assert await d.resolve("bot-a") == "http://bot-a.live:8020", \
        "previous URL dropped when the card fetch failed (merge-not-replace regressed)"
    assert await d.resolve("bot-b") == "http://bot-b.live:8020", "the healthy bot did not resolve"
    assert card_gets["n"] >= 1, "the 500 card-GET handler was never exercised (dead branch)"


# --- flush-with-suffix-inconsistent ----------------------------------------------------------

def test_holdqueue_flush_temp_name_multidot_safe(tmp_path, monkeypatch):
    """The atomic temp MUST be ``<full-name> + '.tmp'``, never ``path.with_suffix('.tmp')`` (which
    drops every suffix after the first dot). A plain round-trip can't catch a with_suffix
    regression — replace() targets self.path either way, so both variants round-trip. So point the
    queue at a genuinely MULTI-dotted filename and assert the actual atomic-temp path preserves the
    whole name: with_suffix('.tmp') on 'hold_queue.v2.json' would yield 'hold_queue.v2.tmp' (the
    '.json' data suffix dropped), which this asserts against.
    """
    from pathlib import Path

    hq = HoldQueue(str(tmp_path))
    # Force a multi-dotted durable filename (mirrors e.g. a versioned/dated queue name).
    hq.path = Path(str(tmp_path)) / "hold_queue.v2.json"

    seen_temp: list[str] = []
    real_replace = Path.replace

    def spy_replace(self, target):
        seen_temp.append(self.name)   # the temp file being atomically renamed onto the durable path
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", spy_replace)

    hq.add("bot-a", {"chat_id": 1, "msg_id": 1, "rfc724_mid": "<m@x>", "text": "t"})

    # The atomic temp must carry the ENTIRE durable name + ".tmp"; with_suffix('.tmp') would have
    # produced 'hold_queue.v2.tmp' (dropping the '.json' suffix) and this assertion would fail.
    assert seen_temp == ["hold_queue.v2.json.tmp"], f"non-multidot-safe temp name: {seen_temp}"

    hq2 = HoldQueue(str(tmp_path))  # default single-dot path — sanity: normal round-trip still works
    hq2.path = Path(str(tmp_path)) / "hold_queue.v2.json"
    hq2._items = hq2._read()
    assert len(hq2) == 1, "durable write did not round-trip"

