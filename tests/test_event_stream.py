"""The backend's inbound event stream must use the official client's per-account queues.

``deltachat_rpc_client.Rpc.start()`` runs its own events thread that drains the core's single
event channel (``get_next_event_batch``) into per-account queues read by
``Rpc.wait_for_event(accid)``. The relay used to ALSO call ``get_next_event()`` — a second
consumer on the same channel — so every event went to whichever consumer won: roughly two
thirds landed in the client's queues and were never read (lost wakes, reactions and
securejoin-verified events). These tests pin the documented pattern.
"""
import queue
import threading

from app.relay import DeltaChatBackend
from tests.test_relay import make_config


class _EventRpc:
    """Fake of the official client's event surface: per-account queues + account ids."""

    def __init__(self, accids):
        self.accids = list(accids)
        self.queues = {a: queue.Queue() for a in self.accids}
        self._lock = threading.Lock()

    def get_all_account_ids(self):
        return list(self.accids)

    def get_config(self, accid, key):
        return f"bot{accid}@deltachat.example.net" if key == "addr" else None

    def wait_for_event(self, accid):
        with self._lock:
            q = self.queues.setdefault(accid, queue.Queue())
        return q.get()

    def get_next_event(self):  # the client's events thread owns this — never call it
        raise AssertionError("get_next_event() competes with Rpc.events_loop")

    def emit(self, accid, event):
        with self._lock:
            q = self.queues.setdefault(accid, queue.Queue())
        q.put(event)

    def add(self, accid):
        self.accids.append(accid)


def _drain(backend, n, timeout=2.0):
    got = []
    backend._EVENT_POLL_SECONDS = 0.05
    for _ in range(int(timeout / 0.05) + n):
        item = backend._next_event()
        if item is not None:
            got.append(item)
            if len(got) == n:
                break
    return got


def test_events_from_every_account_reach_the_pump(tmp_path):
    rpc = _EventRpc([1, 2, 3])
    be = DeltaChatBackend(make_config(), str(tmp_path), _rpc=rpc)
    # emitted BEFORE the pump starts: the client queues them per account, nothing is lost
    for accid in (1, 2, 3):
        for i in range(5):
            rpc.emit(accid, {"kind": "Info", "i": i})
    got = _drain(be, 15)
    assert sorted((a, e["i"]) for a, e in got) == [(a, i) for a in (1, 2, 3) for i in range(5)]
    # per-account order is preserved
    for accid in (1, 2, 3):
        assert [e["i"] for a, e in got if a == accid] == list(range(5))


def test_newly_onboarded_account_joins_the_stream(tmp_path):
    rpc = _EventRpc([1])
    be = DeltaChatBackend(make_config(), str(tmp_path), _rpc=rpc)
    assert _drain(be, 1, timeout=0.1) == []      # pump running, idle
    rpc.add(7)
    be._reindex_accounts()                        # what ensure_account does after onboarding
    rpc.emit(7, {"kind": "IncomingMsg", "chat_id": 10, "msg_id": 20})
    assert _drain(be, 1) == [(7, {"kind": "IncomingMsg", "chat_id": 10, "msg_id": 20})]


def test_forwarders_are_not_duplicated(tmp_path):
    rpc = _EventRpc([1, 2])
    be = DeltaChatBackend(make_config(), str(tmp_path), _rpc=rpc)
    started = []
    be._forward_events = lambda accid: started.append(accid)  # count thread starts
    be._EVENT_POLL_SECONDS = 0.01
    be._next_event()
    for _ in range(3):
        be._reindex_accounts()
        be._ensure_event_forwarders()
    assert sorted(started) == [1, 2]
