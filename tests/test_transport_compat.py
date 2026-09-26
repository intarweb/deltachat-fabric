"""The compatibility shim must make the binding parse the core's actual replies.

The payloads below are the wire format the deployed core emits — taken from its JSON-RPC
type definitions, not hand-guessed. If these parse, the relay works against a core the
binding's own generated schema is behind.
"""

from __future__ import annotations

from app.transport_compat import ContactCompatTransport, _reconcile

# A ContactObject as the core emits it: no is_verified / name_and_addr / was_seen_recently,
# with `freshness` carrying the last-seen signal instead.
def contact(freshness: str = "RecentlySeen") -> dict:
    return {
        "address": "bot@chatmail.example.net",
        "color": "#ff0000",
        "authName": "",
        "status": "",
        "displayName": "Robot",
        "id": 7,
        "name": "Robot",
        "profileImage": None,
        "isBlocked": False,
        "isKeyContact": True,
        "e2eeAvail": True,
        "lastSeen": 0,
        "freshness": freshness,
        "isBot": True,
    }


# A MessageObject as the core emits it. `sender` is a full ContactObject; `is_pinned` is a
# field the binding does not declare and must simply be ignored.
def message() -> dict:
    return {
        "id": 1,
        "chatId": 22,
        "fromId": 7,
        "quote": None,
        "parentId": None,
        "text": "hello",
        "isEdited": False,
        "hasLocation": False,
        "hasHtml": False,
        "viewType": "Text",
        "state": 10,
        "error": None,
        "timestamp": 1,
        "sortTimestamp": 1,
        "receivedTimestamp": 1,
        "hasDeviatingTimestamp": False,
        "subject": "",
        "showPadlock": True,
        "isInfo": False,
        "isForwarded": False,
        "isBot": False,
        "systemMessageType": "Unknown",
        "infoContactId": None,
        "duration": 0,
        "dimensionsHeight": 0,
        "dimensionsWidth": 0,
        "overrideSenderName": None,
        "sender": contact(),
        "file": None,
        "fileMime": None,
        "fileBytes": 0,
        "fileName": None,
        "webxdcHref": None,
        "downloadState": "Done",
        "originalMsgId": None,
        "savedMessageId": None,
        "isPinned": False,
        "reactions": None,
        "vcardContact": None,
    }


class _FakeTransport:
    """Stands in for IOTransport; records the methods it was asked for."""

    def __init__(self, replies: dict):
        self._replies = replies
        self.calls: list[str] = []

    def call(self, method, *args):
        self.calls.append(method)
        return self._replies[method]

    def start(self):
        return "started"


# ---------------------------------------------------------------- the shim's own behaviour


def test_adds_the_three_missing_contact_fields():
    out = _reconcile("get_contact", contact())
    assert out["is_verified"] is False
    assert out["wasSeenRecently"] is True
    assert out["nameAndAddr"] == "Robot <bot@chatmail.example.net>"


def test_never_overwrites_a_field_the_core_did_send():
    """A core that carries these fields again must be passed through untouched."""
    given = dict(contact(), is_verified=True, wasSeenRecently=False, nameAndAddr="as sent")
    out = _reconcile("get_contact", given)
    assert out["is_verified"] is True
    assert out["wasSeenRecently"] is False
    assert out["nameAndAddr"] == "as sent"


def test_freshness_drives_was_seen_recently():
    assert _reconcile("get_contact", contact("RecentlySeen"))["wasSeenRecently"] is True
    for other in ("Normal", "Old"):
        assert _reconcile("get_contact", contact(other))["wasSeenRecently"] is False


def test_verification_is_never_claimed():
    """The dangerous direction: a shim must never assert a contact IS verified."""
    for freshness in ("Normal", "RecentlySeen", "Old"):
        assert _reconcile("get_contact", contact(freshness))["is_verified"] is False


def test_reconciles_contacts_in_a_list():
    out = _reconcile("get_contacts", [contact(), contact()])
    assert all(c["is_verified"] is False for c in out)


def test_reconciles_contacts_by_id():
    out = _reconcile("get_contacts_by_ids", {7: contact(), 8: contact()})
    assert all(c["is_verified"] is False for c in out.values())


def test_reconciles_a_nested_sender_on_a_message():
    out = _reconcile("get_message", message())
    assert out["sender"]["is_verified"] is False
    assert out["text"] == "hello"


def test_reconciles_senders_in_the_batch_message_form():
    out = _reconcile("get_messages", {1: message()})
    assert out[1]["sender"]["is_verified"] is False


def test_leaves_unrelated_results_alone():
    assert _reconcile("get_config", "value") == "value"
    assert _reconcile("get_contact", None) is None


def test_wrapper_delegates_non_intercepted_attributes():
    t = _FakeTransport({"get_contact": contact()})
    w = ContactCompatTransport(t)
    assert w.start() == "started"
    assert w.call("get_contact", 1, 7)["is_verified"] is False
    assert t.calls == ["get_contact"]


# ------------------------------------------------- the real assertion: the binding parses it

def test_binding_parses_the_cores_actual_replies():
    """End-to-end through deltachat2's own deserializer — the thing that was 502ing."""
    from deltachat2.types import Contact, Message, _from_dict

    t = _FakeTransport({"get_contacts": [contact()], "get_message": message()})
    w = ContactCompatTransport(t)

    contacts = [_from_dict(Contact, c) for c in w.call("get_contacts", 1, 0, None)]
    assert contacts[0].display_name == "Robot"
    assert contacts[0].is_verified is False

    msg = _from_dict(Message, w.call("get_message", 1, 1))
    assert msg.text == "hello"
    assert msg.sender.address == "bot@chatmail.example.net"


def test_binding_rejects_the_unreconciled_reply():
    """Guard the rationale: without the shim this payload does NOT parse."""
    import pytest

    from deltachat2.types import Contact, _from_dict

    with pytest.raises(Exception) as exc:
        _from_dict(Contact, contact())
    assert "is_verified" in str(exc.value)
