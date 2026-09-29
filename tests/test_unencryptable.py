"""A send the core can never encrypt fails IMMEDIATELY with a clear error — never parked.

Seen live: heimdall -> contact 28 (an ADDRESS-contact for shadowfall with no key; heimdall's
key-contact for shadowfall is 29). chatmail requires end-to-end encryption, so every attempt
failed with "e2e encryption unavailable"; the relay parked it and retried 40 times over ~3
minutes before dropping it, while the caller only saw status=queued.
"""
import pytest
from deltachat_rpc_client import JsonRpcError
from fastapi.testclient import TestClient

from app.relay import (DeltaChatBackend, Outbox, UnencryptableRecipient, create_app)
from tests.test_messaging_fixes import _StubRpc
from tests.test_relay import FakeBackend, make_config, make_relay

CORE_ERR = {"code": -1, "message": "Failed to send created message: Failed to create send "
                                   "jobs: e2e encryption unavailable Msg#4164 - true"}


def test_backend_turns_core_e2e_failure_into_typed_error():
    class _NoKeyRpc(_StubRpc):
        def send_msg(self, accid, chat_id, _data):
            raise JsonRpcError(CORE_ERR)

    be = DeltaChatBackend(make_config(), "/tmp/df-unenc-test", _rpc=_NoKeyRpc())
    with pytest.raises(UnencryptableRecipient) as ei:
        be._send_msg(1, 28, "hi")
    msg = str(ei.value)
    assert "address-contact" in msg and "key-contact" in msg and "Not retried" in msg


def test_backend_other_send_errors_still_propagate_untyped():
    class _FlakyRpc(_StubRpc):
        def send_msg(self, accid, chat_id, _data):
            raise JsonRpcError({"code": -1, "message": "SMTP connection reset"})

    be = DeltaChatBackend(make_config(), "/tmp/df-unenc-test", _rpc=_FlakyRpc())
    with pytest.raises(JsonRpcError):   # transient → the relay still parks + retries these
        be._send_msg(1, 5, "hi")


class _NoKeyBackend(FakeBackend):
    def send_contact(self, account_id, contact_id, text):
        raise UnencryptableRecipient(f"cannot end-to-end encrypt to contact {contact_id}")


def test_relay_send_contact_fails_fast_not_parked(tmp_path):
    relay = make_relay(_NoKeyBackend(accounts={"heimdall": 1}), [], [], tmp_path,
                       outbox=Outbox(str(tmp_path)))
    with pytest.raises(UnencryptableRecipient):
        relay.send_contact("heimdall", 28, "hi")
    assert relay._get_outbox().pending() == []


def test_send_contact_endpoint_returns_422_with_the_reason(tmp_path):
    relay = make_relay(_NoKeyBackend(accounts={"heimdall": 1}), [], [], tmp_path,
                       outbox=Outbox(str(tmp_path)))
    r = TestClient(create_app(relay)).post(
        "/send_contact", json={"bot_id": "heimdall", "contact_id": 28, "text": "hi"})
    assert r.status_code == 422
    assert "encrypt" in r.json()["detail"]
    assert relay._get_outbox().pending() == []


def test_already_parked_unencryptable_send_is_dropped_on_first_retry(tmp_path):
    relay = make_relay(_NoKeyBackend(accounts={"heimdall": 1}), [], [], tmp_path,
                       outbox=Outbox(str(tmp_path)))
    relay._get_outbox().enqueue({"kind": "contact", "bot": "heimdall", "target": 28,
                                 "text": "hi", "key": "k28", "attempts": 0})
    relay.drain_outbox()
    assert relay._get_outbox().pending() == []
    assert relay.outbox.get("k28")["terminal"] == "dropped"
