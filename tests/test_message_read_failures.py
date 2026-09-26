"""`/messages` must distinguish "no messages" from "could not read messages".

A bare `except Exception: continue` in the backend's read loop makes a broken read path
return `[]` — the caller is told the chat is empty by a service that is erroring on every
message. That is a silent-failure shape: it hides a total read outage behind a
plausible-looking empty result.

The read loop therefore keeps its per-message resilience (one bad id must not blank a
chat) while refusing to report an empty chat when EVERY message it tried to read failed.
"""

from __future__ import annotations

import pytest

from app.relay import DeltaChat2Backend


class _FakeRpc:
    """Minimal RPC stand-in: exposes only what the read path touches."""

    def __init__(self, ids, *, fail_none: bool = False, fail_all: bool = False):
        self._ids = ids
        self._fail_none = fail_none
        self._fail_all = fail_all

    def get_all_account_ids(self):
        return [1]

    def get_config(self, accid, key):
        return "bot@example.net"

    def get_message_ids(self, account_id, chat_id, info_only, add_daymarker):
        return list(self._ids)

    def get_message(self, account_id, mid):
        if self._fail_all:
            raise ValueError('missing value for field "is_verified"')
        return type("M", (), {"text": f"msg-{mid}", "from_id": 7})()

    def get_message_reactions(self, account_id, mid):
        return None


def _backend(rpc) -> DeltaChat2Backend:
    from app.config import Config

    return DeltaChat2Backend(
        Config(mail_domain="deltachat.example.net", imap_host="mail.example.net"),
        "/tmp/unused",
        _rpc=rpc,
    )


def test_reads_all_messages_when_healthy():
    b = _backend(_FakeRpc([1, 2, 3]))
    out = b.list_messages(1, 55, limit=10)
    assert [m["text"] for m in out] == ["msg-1", "msg-2", "msg-3"]


def test_total_read_failure_raises_instead_of_returning_empty():
    """The regression this guards: a wholly broken read path must NOT look like an empty chat."""
    b = _backend(_FakeRpc([1, 2, 3], fail_all=True))
    with pytest.raises(RuntimeError, match="could not read any of 3 messages in chat 55"):
        b.list_messages(1, 55, limit=10)


def test_genuinely_empty_chat_still_returns_empty_list():
    """No ids at all is a real empty chat — that must stay a 200 with `[]`, not an error."""
    b = _backend(_FakeRpc([]))
    assert b.list_messages(1, 55, limit=10) == []


def test_partial_failure_returns_the_readable_messages():
    """One bad message must not blank the chat — partial success still returns data."""

    class _Partial(_FakeRpc):
        def get_message(self, account_id, mid):
            if mid == 2:
                raise ValueError("bad message")
            return type("M", (), {"text": f"msg-{mid}", "from_id": 7})()

    b = _backend(_Partial([1, 2, 3]))
    out = b.list_messages(1, 55, limit=10)
    assert [m["text"] for m in out] == ["msg-1", "msg-3"]
