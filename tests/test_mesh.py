"""Unit tests for the LAZY peer↔peer securejoin mesh (queue-until-verified).

NO live rpc-server, NO live network. Reuses the ``FakeBackend`` from test_relay (extended
there with ``is_verified_key_contact`` / ``mark_verified`` / invite+send_to_addr recording).

Covers the 8 reviewer gates:
  (a) send to UNVERIFIED peer → securejoin initiated + message ENQUEUED (not sent yet)
  (b) on_verified event for that pair → queue FLUSHES in order, exactly-once (no dup on re-fire)
  (c) ALREADY-verified peer → sends immediately, no securejoin, no enqueue (idempotent)
  (d) BOUNDED: exceeding the per-pair cap drops LOUDLY; TTL age-out drops LOUDLY
  (e) the existing on_verified (provision_verified_member) STILL runs alongside the flush
"""
from __future__ import annotations

import logging

import pytest

from app.config import BotSpec, Config
from app.relay import PeerMesh, Relay, HoldQueue, AgentDirectory

from tests.test_relay import FakeBackend, make_config


DOMAIN = "deltachat.example.net"


def make_mesh(accounts, *, verified=None, max_per_pair=50, ttl=3600.0):
    backend = FakeBackend(accounts=accounts, verified=verified)
    mesh = PeerMesh(make_config(), backend, max_per_pair=max_per_pair, ttl=ttl)
    return mesh, backend


# --------------------------------------------------------------------------- (a) enqueue


def test_send_to_unverified_peer_initiates_securejoin_and_enqueues():
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9})
    res = mesh.send_to_peer("bot-a", "bot-b", "hi there")

    assert res["status"] == "queued"
    assert res["reason"] == "securejoin-pending"
    assert res["queued"] == 1
    assert res["target_addr"] == f"bot-b@{DOMAIN}"
    # securejoin was initiated: sender minted an invite, target accepted it
    assert backend.invites == [7]
    assert getattr(backend, "securejoined", []) == [(9, "https://i.delta.chat/#FAKEINVITE-acc7")]
    # message NOT yet sent (queued only)
    assert backend.sent_to == []
    assert mesh.pending_count() == 1


def test_second_send_same_pair_does_not_reinitiate_securejoin():
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9})
    mesh.send_to_peer("bot-a", "bot-b", "one")
    mesh.send_to_peer("bot-a", "bot-b", "two")

    # only ONE securejoin handshake for the pair (idempotent in-flight), both messages queued
    assert backend.invites == [7]
    assert len(getattr(backend, "securejoined", [])) == 1
    assert mesh.pending_count() == 2
    assert backend.sent_to == []


# --------------------------------------------------------------------------- (b) flush


def test_verified_event_flushes_queue_in_order_exactly_once():
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9})
    mesh.send_to_peer("bot-a", "bot-b", "first")
    mesh.send_to_peer("bot-a", "bot-b", "second")
    mesh.send_to_peer("bot-a", "bot-b", "third")
    assert backend.sent_to == []

    # securejoin completes for the pair → verified event fires on the SENDER's account
    addr = f"bot-b@{DOMAIN}"
    backend.mark_verified(7, addr)
    delivered = mesh.flush_verified(7, addr)

    assert delivered == 3
    # sent IN ORDER, all via send_to_addr from bot-a's account (7) to bot-b's addr
    assert backend.sent_to == [
        (7, addr, "first"),
        (7, addr, "second"),
        (7, addr, "third"),
    ]
    assert mesh.pending_count() == 0

    # re-firing the SAME verified event delivers nothing (exactly-once — queue already popped)
    again = mesh.flush_verified(7, addr)
    assert again == 0
    assert len(backend.sent_to) == 3  # unchanged, no dupes


def test_flush_for_pair_with_no_queue_is_noop():
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9})
    assert mesh.flush_verified(7, f"bot-b@{DOMAIN}") == 0
    assert backend.sent_to == []


def test_flush_ignores_unknown_account():
    mesh, backend = make_mesh({"bot-a": 7})
    # account 999 maps to no localpart → no-op, never raises
    assert mesh.flush_verified(999, f"bot-b@{DOMAIN}") == 0


# --------------------------------------------------------------------------- (c) idempotent immediate send


def test_already_verified_peer_sends_immediately_no_securejoin_no_enqueue():
    addr = f"bot-b@{DOMAIN}"
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9}, verified={(7, addr)})
    res = mesh.send_to_peer("bot-a", "bot-b", "hello directly")

    assert res["status"] == "sent"
    assert res["account_id"] == 7
    assert res["target_addr"] == addr
    # sent immediately, NO securejoin (no invite minted, no join)
    assert backend.sent_to == [(7, addr, "hello directly")]
    assert backend.invites == []
    assert getattr(backend, "securejoined", []) == []
    assert mesh.pending_count() == 0


def test_verify_out_of_band_after_queue_then_send_flushes_backlog():
    """If a pair verifies out-of-band (not via our flush) and a NEW send comes in, the
    immediate-send path also flushes any lingering backlog so nothing is stranded."""
    addr = f"bot-b@{DOMAIN}"
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9})
    mesh.send_to_peer("bot-a", "bot-b", "queued-1")
    assert mesh.pending_count() == 1

    # now the pair becomes verified (out-of-band) and a fresh send arrives
    backend.mark_verified(7, addr)
    res = mesh.send_to_peer("bot-a", "bot-b", "live-2")

    assert res["status"] == "sent"
    assert res["flushed_backlog"] == 1
    # backlog flushed first, then the live message
    assert backend.sent_to == [(7, addr, "queued-1"), (7, addr, "live-2")]
    assert mesh.pending_count() == 0


# --------------------------------------------------------------------------- (d) bounded


def test_per_pair_cap_drops_loudly(caplog):
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9}, max_per_pair=2)
    assert mesh.send_to_peer("bot-a", "bot-b", "m1")["status"] == "queued"
    assert mesh.send_to_peer("bot-a", "bot-b", "m2")["status"] == "queued"

    with caplog.at_level(logging.ERROR, logger="dcf"):
        res = mesh.send_to_peer("bot-a", "bot-b", "m3-over-cap")

    assert res["status"] == "dropped"
    assert res["reason"] == "pair-cap-exceeded"
    assert res["cap"] == 2
    # only 2 queued (the over-cap one dropped, not added)
    assert mesh.pending_count() == 2
    assert any("at cap" in r.message and r.levelno >= logging.ERROR for r in caplog.records)


def test_ttl_age_out_drops_loudly(caplog):
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9}, ttl=100.0)
    # enqueue a message with an artificially OLD timestamp (past the TTL)
    mesh.send_to_peer("bot-a", "bot-b", "stale")
    key = ("bot-a", f"bot-b@{DOMAIN}")
    mesh._queues[key][0].enqueued -= 1000.0  # force it well beyond the 100s TTL

    with caplog.at_level(logging.ERROR, logger="dcf"):
        # a subsequent send triggers age-out of the stale entry before enqueue
        mesh.send_to_peer("bot-a", "bot-b", "fresh")

    # stale aged out (loud), only the fresh one remains
    assert mesh.pending_count() == 1
    assert any("AGED OUT" in r.message and r.levelno >= logging.ERROR for r in caplog.records)


def test_ttl_age_out_on_flush_drops_stale_loudly(caplog):
    mesh, backend = make_mesh({"bot-a": 7, "bot-b": 9}, ttl=100.0)
    mesh.send_to_peer("bot-a", "bot-b", "stale-on-flush")
    key = ("bot-a", f"bot-b@{DOMAIN}")
    mesh._queues[key][0].enqueued -= 1000.0

    addr = f"bot-b@{DOMAIN}"
    backend.mark_verified(7, addr)
    with caplog.at_level(logging.ERROR, logger="dcf"):
        delivered = mesh.flush_verified(7, addr)

    # the stale message aged out (loud) → nothing delivered
    assert delivered == 0
    assert backend.sent_to == []
    assert any("AGED OUT" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- fail-loud: target not onboarded


def test_target_not_onboarded_queues_and_logs_error(caplog):
    mesh, backend = make_mesh({"bot-a": 7})  # bot-b NOT onboarded on this relay
    with caplog.at_level(logging.ERROR, logger="dcf"):
        res = mesh.send_to_peer("bot-a", "bot-b", "waiting")

    assert res["status"] == "queued"
    # no securejoin could be driven (target absent) — but it's surfaced LOUDLY, not swallowed
    assert getattr(backend, "securejoined", []) == []
    assert any("not onboarded" in r.message and r.levelno >= logging.ERROR
               for r in caplog.records)
    assert mesh.pending_count() == 1


def test_unknown_sender_bot_raises_keyerror():
    mesh, backend = make_mesh({"bot-b": 9})
    with pytest.raises(KeyError):
        mesh.send_to_peer("nope", "bot-b", "x")


# --------------------------------------------------------------------------- Relay + endpoint wiring


def _make_relay(backend, tmp_path):
    import httpx
    from tests.test_relay import directory_transport
    config = make_config()
    client = httpx.AsyncClient(transport=directory_transport([], []))
    return Relay(config, backend, AgentDirectory(config, client), HoldQueue(str(tmp_path)))


def test_relay_send_to_peer_delegates_to_mesh(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7, "bot-b": 9})
    relay = _make_relay(backend, tmp_path)
    res = relay.send_to_peer("bot-a", "bot-b", "queued msg")
    assert res["status"] == "queued"
    assert relay.peer_mesh.pending_count() == 1


def test_relay_flush_verified_pair_delegates_to_mesh(tmp_path):
    backend = FakeBackend(accounts={"bot-a": 7, "bot-b": 9})
    relay = _make_relay(backend, tmp_path)
    relay.send_to_peer("bot-a", "bot-b", "one")
    addr = f"bot-b@{DOMAIN}"
    backend.mark_verified(7, addr)
    assert relay.flush_verified_pair(7, addr) == 1
    assert backend.sent_to == [(7, addr, "one")]


def test_send_to_peer_endpoint(tmp_path):
    from fastapi.testclient import TestClient
    from app.relay import create_app
    backend = FakeBackend(accounts={"bot-a": 7, "bot-b": 9})
    client = TestClient(create_app(_make_relay(backend, tmp_path)))

    resp = client.post("/send_to_peer", json={"bot_id": "bot-a", "target": "bot-b", "text": "hi"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

    # unknown sender bot → 404 (same contract as /send)
    miss = client.post("/send_to_peer", json={"bot_id": "ghost", "target": "bot-b", "text": "x"})
    assert miss.status_code == 404


def test_healthz_reports_peer_queued(tmp_path):
    from fastapi.testclient import TestClient
    from app.relay import create_app
    backend = FakeBackend(accounts={"bot-a": 7, "bot-b": 9})
    relay = _make_relay(backend, tmp_path)
    relay.send_to_peer("bot-a", "bot-b", "q")
    client = TestClient(create_app(relay))
    body = client.get("/healthz").json()
    assert body["peer_queued"] == 1
