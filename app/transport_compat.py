"""Reconcile deltachat-rpc-server replies with the ``deltachat2`` binding's schema.

``deltachat2`` (github.com/adbenitez/deltachat2) is a third-party binding that turns the
core's JSON-RPC replies into typed dataclasses via ``dacite``, generated against ONE core
release. The core's JSON-RPC API is not frozen — it adds and removes fields freely — so a
runner newer than the binding replies with a type the binding cannot parse, and the caller
gets an exception instead of data.

Neither side is pinned: the image tracks the core forward and the binding is whatever PyPI
publishes. They are reconciled HERE, at the single seam every deserializing call passes
through (``Rpc.transport``), so the relay keeps working across the drift.

The gap this closes
-------------------
The core's ``ContactObject`` (deltachat-jsonrpc ``api/types/contact.rs``) emits exactly:

    address, color, auth_name, status, display_name, id, name, profile_image,
    is_blocked, is_key_contact, e2ee_avail, last_seen, freshness, is_bot

The binding's ``Contact`` dataclass additionally requires ``is_verified``,
``name_and_addr`` and ``was_seen_recently``. Each is supplied as the value that is TRUE FOR
A WIRE FORMAT THAT DOES NOT CARRY IT:

* ``is_verified``       → ``False``. The core neither tracks nor enforces contact
  verification, so nothing on this fabric is verified. False is the only correct answer —
  a tool that gates behaviour on verification must not be told "verified" by a shim.
* ``was_seen_recently`` → derived from the ``freshness`` enum the core does send
  (``RecentlySeen`` → True). A faithful translation, not a placeholder.
* ``name_and_addr``     → ``"<display_name> <<address>>"``. A cosmetic convenience string.

``Message`` has the same gap through its ``sender: ContactObject`` field, so this one
reconcile fixes message reads too. Message's own extra field ``is_pinned`` needs nothing:
``dacite`` ignores input keys the dataclass does not declare.

``_reconcile_contact`` only ADDS keys a payload is missing and never overwrites one the
core sent, so it is a no-op whenever the wire format already carries these fields.
"""

from __future__ import annotations

from typing import Any

# Methods whose result (or whose elements) deserialize into the binding's ``Contact``
# dataclass. The message methods are here because ``Message.sender`` is a ``ContactObject``
# — a missing field there fails the whole message read. ``create_contact`` is absent
# deliberately: it returns a plain contact id, and deserializes nothing.
_CONTACT_RESULT_METHODS = frozenset(
    {
        "get_contact",
        "get_contacts",
        "get_contacts_by_ids",
        "get_message",
        "get_messages",
    }
)

_FRESHNESS_RECENTLY_SEEN = "RecentlySeen"


def _reconcile_contact(obj: Any) -> Any:
    """Fill in the contact fields this wire format omits. Never overwrites a present key."""
    if not isinstance(obj, dict):
        return obj

    if "is_verified" not in obj:
        obj["is_verified"] = False

    if "wasSeenRecently" not in obj:
        obj["wasSeenRecently"] = obj.get("freshness") == _FRESHNESS_RECENTLY_SEEN

    if "nameAndAddr" not in obj:
        display_name = obj.get("displayName") or ""
        address = obj.get("address") or ""
        obj["nameAndAddr"] = f"{display_name} <{address}>" if address else display_name

    return obj


def _reconcile_message(obj: Any) -> Any:
    """A MessageObject carries a nested ContactObject under ``sender``."""
    if isinstance(obj, dict) and isinstance(obj.get("sender"), dict):
        _reconcile_contact(obj["sender"])
    return obj


def _reconcile(method: str, result: Any) -> Any:
    if method not in _CONTACT_RESULT_METHODS or result is None:
        return result

    if method == "get_contacts":
        # list[ContactObject]
        return [_reconcile_contact(item) for item in result] if isinstance(result, list) else result

    if method == "get_contacts_by_ids":
        # dict[contact_id -> ContactObject]
        return (
            {key: _reconcile_contact(value) for key, value in result.items()}
            if isinstance(result, dict)
            else result
        )

    if method == "get_messages":
        # dict[msg_id -> MessageLoadResult]; for kind == "message" the message fields — and
        # therefore ``sender`` — sit at the top level of the entry.
        if isinstance(result, dict):
            for value in result.values():
                _reconcile_message(value)
        return result

    if method == "get_contact":
        # The result IS the contact — no nesting.
        return _reconcile_contact(result)

    # get_message → a MessageObject, whose contact is nested under ``sender``.
    return _reconcile_message(result) if isinstance(result, dict) else result


class ContactCompatTransport:
    """Wrap a transport and reconcile the replies the binding cannot parse.

    Delegates everything to the wrapped transport; only ``call`` is intercepted. ``start``,
    ``close`` and any other attribute pass straight through, so this satisfies the
    ``RpcTransport`` interface the binding expects.
    """

    def __init__(self, transport: Any) -> None:
        self._transport = transport

    def call(self, method: str, *args) -> Any:  # noqa: ANN002
        return _reconcile(method, self._transport.call(method, *args))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._transport, name)
