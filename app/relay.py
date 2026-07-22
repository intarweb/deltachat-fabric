"""Relay — the generic Delta Chat Fabric engine (step 3).

ONE process manages ALL bot Delta accounts through deltachat's account-manager +
rpc-server, exposes an internal HTTP ``/send`` endpoint, and runs an inbound loop
that wakes the right sibling bot(s) over a2a.

Generic-engine rule (hard): ZERO fleet identity is baked in here. Roster, domain,
a2a-directory URL, and every address all come from ``app.config.Config`` (injected at
deploy from env / a mounted roster). This module is publishable as a generic image.

Everything that touches the outside world is behind a thin, INJECTABLE interface so the
whole engine is unit-testable with no live rpc-server and no live network:

  * ``DeltaBackend``   — wraps the deltachat2 account-manager + rpc-server (the ONLY
                         place deltachat2 is imported). Default = ``DeltaChat2Backend``
                         (constructed lazily). Tests inject a fake.
  * ``AgentDirectory`` — resolves a bot's LIVE a2a URL from the a2abridge directory and
                         POSTs the wake. Uses an injectable ``httpx.AsyncClient`` so tests
                         drive it with ``httpx.MockTransport``.
  * ``HoldQueue``      — durable per-bot pending-wake store (JSON on the local DATA_DIR).
                         Survives reload; drained + retried idempotently each inbound tick.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol

import httpx
from pydantic import AliasChoices, BaseModel, Field

from .config import Config

log = logging.getLogger("dcf")
from .routing import wake_targets

# a2abridge TERMINALIZATION (auto-close the wake task at delivery, keyed on the
# params.message.metadata.notification marker above) is LIVE fleet-wide (proven 2026-07-21:
# bridge #5 + hook #127 deployed; Brokkr's marker → task state=completed on send, no
# a2a_complete_task). So there's no completable task left to mis-manage → the wake text uses
# the crisp one-liner (no a2a-negation caveat). Default ON; set DCF_TERMINALIZED=0 to force the
# old caveat back if a host ever runs a pre-terminalize bridge.
TERMINALIZED = os.environ.get("DCF_TERMINALIZED", "1") not in ("0", "false", "False")


def _reply_hint(kind: str, own: str, chat_id: int) -> str:
    """The '↳ reply here' line appended to a wake. ``kind`` ∈ {'dm','channel'}. Names the EXACT
    single tool call with the chat/channel id PRE-FILLED — zero which-tool/which-id decision.
    Once TERMINALIZED, drops the a2a-negation caveat (no task left to mis-complete) for a crisp
    one-liner."""
    if kind == "channel":
        call = f"delta_send_channel(channel_id={chat_id}, text=<your reply>)"
        if TERMINALIZED:
            return f"[↳ Reply here on Delta: {call}]"
        return (f"[↳ To REPLY into this channel use the delta_send_channel "
                f"tool (channel_id={chat_id}, text=<your reply>) — "
                f"a2a_complete_task does NOT reach the channel.]")
    call = f'delta_send(bot_id="{own}", target={chat_id}, text=<your reply>)'
    if TERMINALIZED:
        return f"[↳ Reply here on Delta: {call}]"
    return (f'[↳ To REPLY on Delta use the delta_send tool (bot_id="{own}", '
            f"target={chat_id}, text=<your reply>) — a2a_complete_task "
            f"does NOT reach them on Delta.]")


def _reply_target(kind: str, own: str, chat_id: int) -> dict:
    """The STRUCTURED, machine-readable companion to ``_reply_hint`` — the same reply handle a
    consumer would otherwise have to parse out of the prose. ``kind`` ∈ {'dm','channel'}. Carries
    exactly the identifiers the reply primitive needs to address the reply:
      channel → {"kind":"channel","channel_id":<id>}  → delta_send_channel(channel_id, text=…)
      dm      → {"kind":"dm","bot_id":<own>,"chat_id":<id>} → delta_send(bot_id=own, target=chat_id, text=…)
    A consumer reads ``metadata.reply_target`` and dispatches on ``kind`` — no text parsing."""
    if kind == "channel":
        return {"kind": "channel", "channel_id": chat_id}
    return {"kind": "dm", "bot_id": own, "chat_id": chat_id}



# ---------------------------------------------------------------------------
# Delta backend — the ONLY place deltachat2 is touched. Injectable + swappable.
# ---------------------------------------------------------------------------


@dataclass
class InboundMessage:
    """Normalized inbound message, backend-agnostic (so routing never sees deltachat2).

    ``account_id`` is the receiving bot's Delta account id; ``chat_id`` the group/contact
    chat; ``members`` the localparts of the chat's other bot members (resolved by the
    backend); ``mentioned`` the localparts @-mentioned in ``text``.
    """
    account_id: int
    chat_id: int
    msg_id: int
    text: str
    is_group: bool
    members: list[str] = field(default_factory=list)
    mentioned: list[str] = field(default_factory=list)
    from_id: int = 0
    from_localpart: str = ""   # sender's localpart (resolved) — for sender-exclusion from wakes
    rfc724_mid: str = ""       # GLOBAL message-id (same across every member account) — wake dedup key


@dataclass
class InboundReaction:
    """Normalized inbound REACTION — a human reacted to one of the bot's messages.

    ``account_id`` is the bot whose message was reacted to; ``msg_id`` the reacted message;
    ``from_addr`` the reactor's email (resolved by the backend, so routing never sees
    deltachat2); ``emoji`` the reaction (never empty — an empty reaction = removal, dropped
    upstream in the backend).
    """
    account_id: int
    chat_id: int
    msg_id: int
    emoji: str
    from_id: int = 0
    from_addr: str = ""
    own_message: bool = False   # is the reacted-to message THIS account's own? (only the author wakes)
    rfc724_mid: str = ""        # GLOBAL id of the reacted-to message — wake dedup key



@dataclass
class InboundVerified:
    """Normalized securejoin-verified event — a member just became a VERIFIED KEY-CONTACT of a
    realm lead (the core emitted inviter-progress == complete on the lead's account).

    ``account_id`` is the LEAD's account (the securejoin inviter); ``addr`` the member's email
    (resolved by the backend, so provisioning never sees deltachat2). This is the event that
    drives channel provisioning — when it fires, the member can be added to the realm's
    encrypted channel — replacing the old reconcile-time wait/poll for verification.
    """
    account_id: int
    contact_id: int
    addr: str = ""


class DeltaBackend(Protocol):
    """Thin injectable seam over the deltachat account-manager + rpc-server.

    Kept deliberately small so a fake in tests is trivial and the real deltachat2
    binding is swappable without touching relay logic.
    """

    def account_id_for(self, localpart: str) -> Optional[int]:
        """Delta account id for a bot localpart (None if that bot has no account)."""
        ...

    def send(self, account_id: int, chat_id: int, text: str) -> int:
        """Send ``text`` from ``account_id`` into ``chat_id``; return the sent msg id."""
        ...

    def send_to_addr(self, account_id: int, addr: str, text: str) -> tuple[int, int]:
        """Send ``text`` from ``account_id`` to the person at email ``addr``, resolving their
        1:1 chat (address → contact → chat). Returns ``(chat_id, msg_id)``. Used to message a
        HUMAN by their address after securejoin makes them a verified key-contact. (Optional on
        fakes.)"""
        ...

    def next_inbound(self):
        """Pop the next inbound event from the stream: an ``InboundMessage``, an
        ``InboundReaction``, an ``InboundVerified`` (a member became a verified key-contact of a
        realm lead), or ``None`` if idle / not a routable event.

        Non-blocking: the inbound loop calls this repeatedly per tick. Returning None
        means "nothing routable waiting right now".
        """
        ...

    def localpart_for(self, account_id: int) -> Optional[str]:
        """The bot localpart owning ``account_id`` (reverse of ``account_id_for``), or None."""
        ...

    def list_contacts(self, account_id: int) -> list[dict]:
        """List a bot account's known contacts as ``[{id,address,display_name}, ...]``."""
        ...

    def list_channels(self, account_id: int) -> list[dict]:
        """List a bot account's group chats as ``[{id,name,members:[localpart,...]}, ...]``."""
        ...

    def create_channel(self, account_id: int, name: str, members: list[str]) -> int:
        """Create a group chat named ``name`` and add each address in ``members``.

        ``members`` are full addresses (or ids the backend can resolve). Returns the chat id.
        Generic: members come from the caller — nothing is baked.
        """
        ...

    def add_member(self, account_id: int, chat_id: int, contact: str) -> None:
        """Add ``contact`` (address the backend resolves to a contact) to ``chat_id``."""
        ...

    def react(self, account_id: int, msg_id: int, emoji: str) -> None:
        """Set the reaction ``emoji`` on message ``msg_id`` for this account."""
        ...

    def list_messages(self, account_id: int, chat_id: int, limit: int = 20) -> list[dict]:
        """Recent messages in a chat as ``[{id,text,from_id}, ...]`` (newest last). Used to
        confirm RECEIPT (the inbound side) — e.g. prove a bot-to-bot send round-trips."""
        ...

    def create_invite(self, account_id: int) -> str:
        """Generate the account's securejoin CONTACT-invite link (i.delta.chat/#...) — a human
        taps it in their Delta app to establish a verified contact with this bot. (Optional on fakes.)"""
        ...

    def ensure_account(self, localpart: str, password: str, *,
                       imap_host: str, imap_port: int,
                       smtp_host: str, smtp_port: int,
                       display_name: Optional[str] = None) -> bool:
        """Idempotently onboard a bot's mailbox into the deltachat core (create-on-login +
        configure) so it can send/receive. ``display_name`` sets the Delta name humans see
        (e.g. 'Robot' vs the raw email). BLOCKING — callers run it off the event loop.
        Returns True iff the account is configured after the call. (Optional on fakes.)"""
        ...

    def secure_join(self, account_id: int, invite: str) -> int:
        """Accept a securejoin/verified-invite (link or QR) → the inviter becomes a verified
        key-contact; returns the resulting chat id. BLOCKING. (Optional on fakes.)"""
        ...

    def is_verified_key_contact(self, account_id: int, addr: str) -> bool:
        """True iff ``addr`` is already a VERIFIED KEY-CONTACT of ``account_id`` (the state a
        completed securejoin establishes). Lets the securejoin-star skip an already-verified
        (lead, member) pair so reconcile is idempotent and doesn't re-handshake every run.
        (Optional on fakes.)"""
        ...

    def delete_chat(self, account_id: int, chat_id: int) -> None:
        """Delete ``chat_id`` from ``account_id`` (drops the chat + any in-progress securejoin
        half-handshake it holds). Used to clear a stale/tangled securejoin so a single clean
        one can complete. BLOCKING. (Optional on fakes.)"""
        ...


class DeltaChat2Backend:
    """Default backend over deltachat2 (account-manager + rpc-server on a LOCAL dir).

    deltachat2 / deltachat-rpc-client are imported LAZILY inside ``__init__`` so importing
    ``app.relay`` (and unit-testing it with a fake backend) needs neither the package nor a
    live rpc-server. Account DBs live under ``accounts_dir`` — MUST be a local volume
    (SQLCipher over NFS corrupts).

    deltachat2 API surface used (verified via deltachat-bot/deltabot-cli-py autodocs,
    context7 /deltachat-bot/deltabot-cli-py):
      * ``Rpc(IOTransport(accounts_dir=...))``      — account manager + rpc-server handle
      * ``rpc.get_all_account_ids() -> list[int]``
      * ``rpc.get_config(accid, "addr") -> str``    — configured address of an account
      * ``rpc.send_msg(accid, chatid, MsgData(text=...)) -> int``   — returns sent msg id
      * ``rpc.get_next_event() -> RawEvent``         — event stream
      * ``rpc.get_message(accid, msgid)`` / ``rpc.get_basic_chat_info`` /
        ``rpc.get_chat_contacts(accid, chatid) -> list[int]`` / ``rpc.get_contact(accid, cid)``
      * ``rpc.create_group_chat(accid, name, protect=False) -> int``  — new group (verified)

    ⚠ deltachat2 API used but NOT verifiable from the autodocs I could reach (context7
    /deltachat-bot/deltabot-cli-py returned no signature). These are the documented
    deltachat JSON-RPC names; the calls are isolated HERE behind ``DeltaBackend`` +
    ``# pragma: no cover`` with defensive getattr, so an API drift can be fixed in one
    place without touching relay logic. VERIFY against the deployed core before prod:
      * ``rpc.get_contacts(accid, listflags, query) -> list[int]``   — enumerate contacts
      * ``rpc.create_contact(accid, addr, name) -> int``             — addr → contact id
      * ``rpc.add_contact_to_chat(accid, chatid, contact_id)``       — add member
      * ``rpc.send_reaction(accid, msgid, [emoji]) -> int``          — react to a message
      * ``rpc.get_chatlist_entries`` / ``rpc.get_basic_chat_info``   — enumerate group chats
    Exact event-enum + message-snapshot field names differ across core versions; the
    normalization below is defensive (getattr/dict fallbacks). Anything version-fragile is
    isolated HERE, behind the ``DeltaBackend`` seam — never in relay logic.
    """

    def __init__(self, config: Config, accounts_dir: str, *, _rpc: Any = None,
                 _io_transport: Any = None):
        self.config = config
        self.accounts_dir = accounts_dir
        self._addr_to_accid: dict[str, int] = {}
        self._localpart_to_accid: dict[str, int] = {}
        if _rpc is not None:
            self.rpc = _rpc
        else:  # pragma: no cover - requires the deltachat2 package + rpc-server binary
            from deltachat2 import IOTransport, Rpc  # type: ignore

            Path(accounts_dir).mkdir(parents=True, exist_ok=True)
            trans = _io_transport or IOTransport(accounts_dir=accounts_dir)
            trans.start()
            self.rpc = Rpc(trans)
            self.rpc.start_io_for_all_accounts()
        self._reindex_accounts()

    # -- account index -----------------------------------------------------
    def _reindex_accounts(self) -> None:
        """Map configured account addresses → account ids, keyed by localpart."""
        self._addr_to_accid.clear()
        self._localpart_to_accid.clear()
        for accid in self.rpc.get_all_account_ids():
            try:
                addr = self.rpc.get_config(accid, "addr") or self.rpc.get_config(accid, "configured_addr")
            except Exception:  # pragma: no cover - defensive
                addr = None
            if not addr:
                continue
            self._addr_to_accid[addr] = accid
            self._localpart_to_accid[addr.split("@", 1)[0]] = accid

    def account_id_for(self, localpart: str) -> Optional[int]:
        accid = self._localpart_to_accid.get(localpart)
        if accid is None:
            self._reindex_accounts()
            accid = self._localpart_to_accid.get(localpart)
        return accid

    # -- send --------------------------------------------------------------
    def send(self, account_id: int, chat_id: int, text: str) -> int:  # pragma: no cover
        # deltachat2's message-data type is MessageData (NOT MsgData — verified vs the
        # installed package; send_msg(accid, chat_id, MessageData) -> int).
        from deltachat2 import MessageData  # type: ignore

        return self.rpc.send_msg(account_id, chat_id, MessageData(text=text))

    def send_to_addr(self, account_id: int, addr: str, text: str) -> tuple[int, int]:  # pragma: no cover
        """Message a HUMAN by email address: resolve addr → contact → 1:1 chat, then send.

        lookup_contact_id_by_addr → create_chat_by_contact_id (idempotent: returns the existing
        verified 1:1 chat if it exists, else creates it) → send_msg. Requires the contact to be
        a verified key-contact (post-securejoin) for the chat to be encryptable. Returns
        ``(chat_id, msg_id)``. Verified vs installed deltachat2. Raises KeyError if no contact
        resolves for ``addr``.
        """
        from deltachat2 import MessageData  # type: ignore

        cid = self.rpc.lookup_contact_id_by_addr(account_id, addr)
        if not cid:
            raise KeyError(f"no contact for address {addr}")
        chat_id = self.rpc.create_chat_by_contact_id(account_id, cid)
        msg_id = self.rpc.send_msg(account_id, chat_id, MessageData(text=text))
        return chat_id, msg_id

    # -- securejoin (accept a verified invite → inviter becomes a key-contact) ----
    def secure_join(self, account_id: int, invite: str) -> int:  # pragma: no cover
        """Accept a securejoin / verified-invite link (or QR content) for ``account_id``.

        This IS the key-exchange: on success the inviter becomes a VERIFIED KEY-CONTACT of
        this account (so they can then be added to an encrypted chat). Returns the resulting
        chat id. Verified vs the installed deltachat2 (``secure_join(account_id, qr) -> int``).
        Blocking (network handshake) — callers run it off the loop.
        """
        return self.rpc.secure_join(account_id, invite)

    def is_verified_key_contact(self, account_id: int, addr: str) -> bool:  # pragma: no cover
        """True iff ``addr`` is already a VERIFIED KEY-CONTACT of ``account_id``.

        Mirrors ``_resolve_contact``'s enumeration (get_contacts matching the address, keep the
        key-contacts) but only asks whether a VERIFIED one exists — the state a completed
        securejoin leaves behind. Used by the securejoin-star to skip an already-verified pair
        (idempotent reconcile). Defensive: False on any error (→ we attempt the securejoin,
        which is itself idempotent)."""
        try:
            matches = self.rpc.get_contacts(account_id, 0, addr) or []
        except Exception:
            return False
        want = addr.strip().lower()
        for c in matches:
            if (getattr(c, "is_key_contact", False)
                    and getattr(c, "is_verified", False)
                    and (getattr(c, "address", "") or "").strip().lower() == want):
                return True
        return False

    def delete_chat(self, account_id: int, chat_id: int) -> None:  # pragma: no cover - real rpc
        """Delete ``chat_id`` from ``account_id``. Clears the chat and any in-progress
        securejoin half-handshake it carries, so a single clean securejoin can complete.
        Verified vs installed deltachat2 (``delete_chat(account_id, chat_id) -> None``).
        """
        self.rpc.delete_chat(account_id, int(chat_id))

    # -- inbound -----------------------------------------------------------
    @staticmethod
    def incoming_ids(ev) -> tuple[Optional[int], Optional[int]]:
        """(chat_id, msg_id) if ``ev`` is an incoming-message event, else (None, None).

        🔴 deltachat2 deserializes each event to a TYPED dataclass — ``EventTypeIncomingMsg``
        carries ``chat_id`` + ``msg_id`` and has **NO ``kind`` attribute** (the type IS the
        discriminator). So we must select by ``isinstance``, NOT a ``kind`` string — a
        ``getattr(ev,"kind")`` check silently drops EVERY incoming message. Kept as a pure
        staticmethod so it's unit-tested against the real deltachat2 types (no rpc/network).
        Verified against the installed deltachat2 (Event.fields = context_id,event;
        EventTypeIncomingMsg.fields = chat_id,msg_id)."""
        try:
            from deltachat2 import EventTypeIncomingMsg  # type: ignore
            if isinstance(ev, EventTypeIncomingMsg):
                return ev.chat_id, ev.msg_id
        except Exception:  # pragma: no cover - deltachat2 always present in the image/tests
            pass
        # defensive fallbacks: raw dict-shaped events, or a like-named type from another core
        if isinstance(ev, dict) and ev.get("kind") == "IncomingMsg":
            return ev.get("chat_id"), ev.get("msg_id")
        if type(ev).__name__ == "EventTypeIncomingMsg":
            return getattr(ev, "chat_id", None), getattr(ev, "msg_id", None)
        return None, None

    @staticmethod
    def reaction_ids(ev):
        """(chat_id, contact_id, msg_id, emoji) if ``ev`` is an incoming-REACTION event with a
        non-empty reaction, else None.

        🔴 Reactions arrive on the deltachat CORE event stream as ``EventTypeIncomingReaction``
        (fields chat_id/contact_id/msg_id/reaction) — NOT in message bodies, so the IMAP/text
        path never sees them. An EMPTY ``reaction`` string = the reactor REMOVED their reaction
        → return None (nothing to forward). Select by isinstance (the type is the discriminator),
        mirroring incoming_ids. Verified vs installed deltachat2."""
        try:
            from deltachat2 import EventTypeIncomingReaction  # type: ignore
            if isinstance(ev, EventTypeIncomingReaction):
                return (ev.chat_id, ev.contact_id, ev.msg_id, ev.reaction) if ev.reaction else None
        except Exception:  # pragma: no cover - deltachat2 always present in the image/tests
            pass
        if type(ev).__name__ == "EventTypeIncomingReaction":
            emoji = getattr(ev, "reaction", "") or ""
            if not emoji:
                return None
            return getattr(ev, "chat_id", None), getattr(ev, "contact_id", None), getattr(ev, "msg_id", None), emoji
        return None

    @staticmethod
    def securejoin_ids(ev):
        """contact_id if ``ev`` is a securejoin INVITER-progress event that COMPLETED (the joiner
        became a verified key-contact of this inviter), else None.

        🔴 The realm lead is the securejoin INVITER (it creates the invite; members join), so
        completion surfaces as ``EventTypeSecurejoinInviterProgress`` with ``progress == 1000``
        (deltachat-core's "securejoin done" sentinel) on the LEAD's account. Select by isinstance
        (the type is the discriminator), mirroring incoming_ids/reaction_ids. This is the event
        that drives EVENT-DRIVEN channel provisioning — no wait, no poll. Verified vs the
        installed deltachat2 (EventTypeSecurejoinInviterProgress.fields =
        chat_id,chat_type,contact_id,progress)."""
        try:
            from deltachat2 import EventTypeSecurejoinInviterProgress  # type: ignore
            if isinstance(ev, EventTypeSecurejoinInviterProgress):
                if int(getattr(ev, "progress", 0) or 0) >= 1000:
                    return getattr(ev, "contact_id", None)
                return None
        except Exception:  # pragma: no cover - deltachat2 always present in the image/tests
            pass
        if (type(ev).__name__ == "EventTypeSecurejoinInviterProgress"
                and int(getattr(ev, "progress", 0) or 0) >= 1000):
            return getattr(ev, "contact_id", None)
        return None

    @staticmethod
    def core_diagnostic(ev):
        """(level, text) for a deltachat CORE event worth surfacing to the relay log, else None.

        Surfaces the diagnostically-useful core events so failures aren't silent — e.g. a
        human→bot securejoin that can't decrypt emits core Warnings ("Could not find symmetric
        secret" → "Fetched unencrypted message, ignoring"). Mapped to a log level (select by
        isinstance, mirroring incoming_ids/reaction_ids/securejoin_ids; verified vs installed
        deltachat2):
          Error → error · Warning → warning (covers decrypt/SMTP/IMAP-connect failures) ·
          Securejoin{Inviter,Joiner}Progress → info (shows a handshake advancing / stuck / silent).
        EventTypeInfo is intentionally NOT surfaced (too chatty). Returns None for everything else.
        """
        try:
            from deltachat2 import (EventTypeError, EventTypeWarning,  # type: ignore
                                    EventTypeSecurejoinInviterProgress,
                                    EventTypeSecurejoinJoinerProgress)
            if isinstance(ev, EventTypeError):
                return ("error", getattr(ev, "msg", "") or "")
            if isinstance(ev, EventTypeWarning):
                return ("warning", getattr(ev, "msg", "") or "")
            if isinstance(ev, EventTypeSecurejoinInviterProgress):
                return ("info", f"securejoin inviter: contact={getattr(ev, 'contact_id', None)} "
                                f"progress={getattr(ev, 'progress', None)}")
            if isinstance(ev, EventTypeSecurejoinJoinerProgress):
                return ("info", f"securejoin joiner: contact={getattr(ev, 'contact_id', None)} "
                                f"progress={getattr(ev, 'progress', None)}")
        except Exception:  # pragma: no cover - deltachat2 always present in the image/tests
            pass
        return None

    def next_inbound(self):  # pragma: no cover - real rpc entry
        raw = self.rpc.get_next_event()
        if raw is None:
            return None
        # deltachat2 Event: the account id is ``context_id`` (verified vs installed package);
        # older bindings used ``account_id``/``accid``.
        accid = (getattr(raw, "context_id", None) or getattr(raw, "account_id", None)
                 or getattr(raw, "accid", None) or 0)
        ev = getattr(raw, "event", raw)
        # Surface diagnostically-useful core events to the relay log (never invisible again).
        diag = self.core_diagnostic(ev)
        if diag is not None:
            level, text = diag
            getattr(log, level, log.info)("delta core [acct %s]: %s", accid, text)
        chat_id, msg_id = self.incoming_ids(ev)
        if chat_id is not None and msg_id is not None:
            return self._build_inbound(accid, chat_id, msg_id)
        react = self.reaction_ids(ev)
        if react is not None:
            r_chat, r_contact, r_msg, emoji = react
            return self._build_reaction(accid, r_chat, r_contact, r_msg, emoji)
        sj_contact = self.securejoin_ids(ev)
        if sj_contact is not None:
            return self._build_verified(accid, sj_contact)
        return None

    def _build_verified(self, accid: int, contact_id) -> InboundVerified:  # pragma: no cover
        addr = ""
        try:
            contact = self.rpc.get_contact(accid, contact_id)
            addr = getattr(contact, "address", None) or getattr(contact, "addr", None) or ""
        except Exception:
            pass
        return InboundVerified(account_id=accid, contact_id=contact_id or 0, addr=addr)

    def _build_reaction(self, accid: int, chat_id, contact_id, msg_id, emoji: str) -> InboundReaction:  # pragma: no cover
        from_addr = ""
        try:
            contact = self.rpc.get_contact(accid, contact_id)
            from_addr = getattr(contact, "address", None) or getattr(contact, "addr", None) or ""
        except Exception:
            pass
        # Only the AUTHOR of the reacted-to message should be woken; the reaction event is
        # delivered to every member account, so gate on "is this account the message author?"
        # (msg.from_id == self) + carry the reacted msg's GLOBAL id for cross-account dedup.
        own_message = False
        rfc724_mid = ""
        try:
            from deltachat2 import SpecialContactId  # type: ignore
            m = self.rpc.get_message(accid, msg_id)
            own_message = int(getattr(m, "from_id", 0) or 0) == int(SpecialContactId.SELF)
        except Exception:
            pass
        try:
            info = self.rpc.get_message_info_object(accid, msg_id)
            rfc724_mid = getattr(info, "rfc724_mid", "") or ""
        except Exception:
            pass
        return InboundReaction(
            account_id=accid, chat_id=chat_id or 0, msg_id=msg_id or 0,
            emoji=emoji, from_id=contact_id or 0, from_addr=from_addr,
            own_message=own_message, rfc724_mid=rfc724_mid,
        )

    def localpart_for(self, account_id: int) -> Optional[str]:  # pragma: no cover - real rpc
        for lp, accid in self._localpart_to_accid.items():
            if accid == account_id:
                return lp
        return None

    def _build_inbound(self, accid: int, chat_id: int, msg_id: int) -> InboundMessage:  # pragma: no cover
        msg = self.rpc.get_message(accid, msg_id)
        text = getattr(msg, "text", "") or ""
        chat = self.rpc.get_basic_chat_info(accid, chat_id)
        chat_type = getattr(chat, "chat_type", None) or getattr(chat, "type", None)
        is_group = chat_type in ("Group", "group", 120, 130) or bool(getattr(chat, "is_group", False))
        members: list[str] = []
        for cid in self.rpc.get_chat_contacts(accid, chat_id):
            try:
                contact = self.rpc.get_contact(accid, cid)
                addr = getattr(contact, "address", None) or getattr(contact, "addr", None)
                if addr:
                    members.append(addr.split("@", 1)[0])
            except Exception:
                continue
        from_id = getattr(msg, "from_id", 0) or 0
        # Sender localpart — used for sender-exclusion (a bot is never woken by its own post)
        # AND forwarded as the wake's "from" metadata so the inbox renders "From `<sender>`"
        # instead of "someone". Resolve via the sender Contact EMBEDDED in the message first,
        # then a get_contact(from_id) lookup — chatmail key-contacts (securejoin/PGP) can populate
        # .address on only one of the two. Warn-log if still unresolved so a real relayed DM that
        # fails leaves from_id/sender in the log to finish the diagnosis (verified vs installed
        # deltachat2: Message has .from_id + .sender, Contact has .address).
        def _localpart(c) -> str:
            if c is None:
                return ""
            addr = getattr(c, "address", None) or getattr(c, "addr", None) or ""
            return addr.split("@", 1)[0] if "@" in addr else ""
        from_localpart = _localpart(getattr(msg, "sender", None))
        if not from_localpart and from_id:
            try:
                from_localpart = _localpart(self.rpc.get_contact(accid, from_id))
            except Exception:
                pass
        if not from_localpart:
            log.warning("inbound sender UNRESOLVED (renders 'someone'): accid=%s msg=%s from_id=%s sender=%r",
                        accid, msg_id, from_id, getattr(msg, "sender", None))
        # GLOBAL message-id — identical across every member account's copy → wake dedup key
        rfc724_mid = ""
        try:
            info = self.rpc.get_message_info_object(accid, msg_id)
            rfc724_mid = getattr(info, "rfc724_mid", "") or ""
        except Exception:
            pass
        return InboundMessage(
            account_id=accid, chat_id=chat_id, msg_id=msg_id, text=text,
            is_group=is_group, members=members,
            mentioned=extract_mentions(text, members),
            from_id=from_id, from_localpart=from_localpart, rfc724_mid=rfc724_mid,
        )

    # -- contacts / channels ----------------------------------------------
    # deltachat2 signatures below verified against the installed package: get_contacts ->
    # list[Contact] (OBJECTS with .id/.address/.display_name, NOT ids); get_chatlist_entries
    # -> list[int]; get_chat_contacts -> list[int]; ChatType is a str-enum ("Group"/"Single").
    @staticmethod
    def _contact_to_dict(contact) -> dict:
        """Normalize a deltachat2 Contact object → {id,address,display_name}. Pure (no rpc),
        so it's unit-testable; the field fallbacks tolerate minor cross-version drift."""
        return {
            "id": getattr(contact, "id", None),
            "address": getattr(contact, "address", None) or getattr(contact, "addr", None) or "",
            "display_name": (getattr(contact, "display_name", None)
                             or getattr(contact, "name", None) or ""),
        }

    def _contact_dict(self, accid: int, cid: int) -> dict:  # pragma: no cover
        return self._contact_to_dict(self.rpc.get_contact(accid, cid))

    def list_contacts(self, account_id: int) -> list[dict]:  # pragma: no cover
        # 🔴 get_contacts returns list[Contact] OBJECTS (verified vs installed deltachat2), not
        # ids — build the dicts directly. Fall back to id-fetch for a legacy binding that
        # returns ints.
        items = self.rpc.get_contacts(account_id, 0, None)
        out: list[dict] = []
        for item in (items or []):
            if hasattr(item, "address") or hasattr(item, "id"):
                out.append(self._contact_to_dict(item))       # Contact object (deltachat2)
            else:
                out.append(self._contact_dict(account_id, item))  # int id (legacy)
        return out

    def list_channels(self, account_id: int) -> list[dict]:  # pragma: no cover
        # Enumerate chatlist entries → keep the group chats. get_chatlist_entries -> list[int]
        # (verified); tolerate a (chatid,msgid) tuple shape defensively. ChatType.GROUP == "Group".
        entries = self.rpc.get_chatlist_entries(account_id, 0, None, None)
        out: list[dict] = []
        for e in (entries or []):
            chat_id = e[0] if isinstance(e, (list, tuple)) else e
            info = self.rpc.get_basic_chat_info(account_id, chat_id)
            chat_type = getattr(info, "chat_type", None) or getattr(info, "type", None)
            is_group = str(chat_type) in ("Group", "ChatType.GROUP") or bool(getattr(info, "is_group", False))
            if not is_group:
                continue
            members: list[str] = []
            for cid in self.rpc.get_chat_contacts(account_id, chat_id):
                try:
                    members.append(self._contact_dict(account_id, cid)["address"].split("@", 1)[0])
                except Exception:
                    continue
            out.append({
                "id": chat_id,
                "name": getattr(info, "name", None) or "",
                "members": [m for m in members if m],
            })
        return out

    def _resolve_contact(self, account_id: int, contact: str) -> int:  # pragma: no cover
        """Resolve a member address to its KEY-CONTACT id (the encryptable contact established
        by securejoin), NOT an address-contact.

        create_contact(addr) ALWAYS makes an address-contact (no key) — the core refuses to add
        it to an encrypted chat ("Only key-contacts can be added to encrypted chats"). And
        lookup_contact_id_by_addr returns the most-recently-seen contact (may be the
        address-contact) per deltachat-core api.rs ("do not use to look them up"). So enumerate
        contacts matching the address and return the key-contact (prefer a verified one). Raise
        if none — the member must securejoin first.
        """
        matches = self.rpc.get_contacts(account_id, 0, contact) or []
        addr = contact.strip().lower()
        keyc = [c for c in matches
                if getattr(c, "is_key_contact", False)
                and (getattr(c, "address", "") or "").strip().lower() == addr]
        if not keyc:
            raise KeyError(
                f"no key-contact for {contact} — securejoin required before adding to an "
                f"encrypted group (create_contact would make an unaddable address-contact)")
        keyc.sort(key=lambda c: 0 if getattr(c, "is_verified", False) else 1)
        return keyc[0].id

    def create_channel(self, account_id: int, name: str, members: list[str]) -> int:  # pragma: no cover
        chat_id = self.rpc.create_group_chat(account_id, name, False)
        for m in members:
            self.rpc.add_contact_to_chat(account_id, chat_id, self._resolve_contact(account_id, m))
        return chat_id

    def add_member(self, account_id: int, chat_id: int, contact: str) -> None:  # pragma: no cover
        self.rpc.add_contact_to_chat(account_id, chat_id, self._resolve_contact(account_id, contact))

    def react(self, account_id: int, msg_id: int, emoji: str) -> None:  # pragma: no cover
        self.rpc.send_reaction(account_id, msg_id, [emoji])

    def list_messages(self, account_id: int, chat_id: int, limit: int = 20) -> list[dict]:  # pragma: no cover
        # get_message_ids(accid, chatid, info_only, add_daymarker) -> list[int] (verified);
        # read the newest `limit` via get_message. Defensive per-message so one bad id doesn't
        # abort the read.
        ids = self.rpc.get_message_ids(account_id, int(chat_id), False, False) or []
        out: list[dict] = []
        for mid in ids[-int(limit):]:
            try:
                m = self.rpc.get_message(account_id, mid)
                out.append({"id": mid, "text": getattr(m, "text", "") or "",
                            "from_id": getattr(m, "from_id", 0) or 0,
                            "reactions": self._reactions_for(account_id, mid)})
            except Exception:
                continue
        return out

    def _reactions_for(self, account_id: int, msg_id: int) -> list[dict]:  # pragma: no cover - real rpc
        """Reactions on a message as ``[{emoji,count,is_from_self}, ...]`` (read side, so a
        caller can see inbound reactions in /messages). Best-effort: [] on any error."""
        try:
            reactions = self.rpc.get_message_reactions(account_id, msg_id)
            if not reactions:
                return []
            return [{"emoji": getattr(r, "emoji", ""), "count": getattr(r, "count", 0),
                     "is_from_self": bool(getattr(r, "is_from_self", False))}
                    for r in (getattr(reactions, "reactions", None) or [])]
        except Exception:
            return []

    def create_invite(self, account_id: int) -> str:  # pragma: no cover - real rpc
        # get_chat_securejoin_qr_code(accid, None) -> the account's securejoin CONTACT-invite
        # link (i.delta.chat/#...); a human taps it to become a verified contact. Verified vs
        # installed deltachat2.
        return self.rpc.get_chat_securejoin_qr_code(account_id, None)

    # -- onboarding (create-on-login + configure into the deltachat CORE) ---
    def ensure_account(self, localpart: str, password: str, *,
                       imap_host: str, imap_port: int,
                       smtp_host: str, smtp_port: int,
                       display_name: Optional[str] = None) -> bool:
        """Idempotently onboard a bot's mailbox INTO THE DELTACHAT CORE so it can send/receive.

        add_account() → add_or_update_transport(EnteredLoginParam(...)). The transport login
        create-on-logins the mailbox on a chatmail/Dovecot server AND configures the core
        account (writes the SQLCipher dc.db). ``display_name`` sets the deltachat ``displayname``
        config (the name humans see, e.g. 'Robot' vs the raw address) — applied idempotently
        on every pass, including already-onboarded accounts. BLOCKING — callers must run it off
        the event loop (main._make_onboard uses asyncio.to_thread).

        Verified against adbenitez/deltachat2: ``add_or_update_transport`` supersedes the
        deprecated (2025-02) ``configure``; ``EnteredLoginParam``/``Socket`` field names +
        ``is_configured``/``start_io`` signatures. Isolated here behind DeltaBackend so an API
        drift is a one-place fix. Returns True iff the account is configured after the call.
        """
        from deltachat2 import EnteredLoginParam, Socket  # type: ignore

        desired_addr = f"{localpart}@{self.config.mail_domain}"
        existing = self._localpart_to_accid.get(localpart)
        if existing is not None:
            try:
                if self.rpc.is_configured(existing):
                    current = (self.rpc.get_config(existing, "configured_addr")
                               or self.rpc.get_config(existing, "addr"))
                    if current == desired_addr:
                        self._set_displayname(existing, display_name)  # apply to already-onboarded
                        return True  # already onboarded on the right address — idempotent no-op
                    # address/domain changed (e.g. deltachat.* → stalwart.* migration):
                    # fall through to re-run add_or_update_transport onto the new address.
                    log.info("re-onboarding %s: address changed %s -> %s",
                             localpart, current, desired_addr)
            except Exception:
                pass
        accid = existing if existing is not None else self.rpc.add_account()
        try:
            self.rpc.set_config(accid, "bot", "1")  # mark as a bot account (best-effort)
        except Exception:
            pass
        self._set_displayname(accid, display_name)  # name humans see (best-effort)
        param = EnteredLoginParam(
            addr=desired_addr,
            password=password,
            imap_server=imap_host, imap_port=int(imap_port), imap_security=Socket.SSL,
            smtp_server=smtp_host, smtp_port=int(smtp_port), smtp_security=Socket.STARTTLS,
        )
        self.rpc.add_or_update_transport(accid, param)  # blocks until configuration finishes
        try:
            ok = bool(self.rpc.is_configured(accid))
        except Exception:
            ok = True  # older cores lack is_configured; no exception above ⇒ assume configured
        if ok:
            try:
                self.rpc.start_io(accid)  # begin receiving for the freshly-configured account
            except Exception:
                pass
            self._reindex_accounts()
        return ok

    def _set_displayname(self, account_id: int, display_name: Optional[str]) -> None:  # pragma: no cover - real rpc
        """Set the deltachat ``displayname`` config (the name humans see) — best-effort,
        idempotent. No-op when ``display_name`` is falsy."""
        if not display_name:
            return
        try:
            self.rpc.set_config(account_id, "displayname", display_name)
        except Exception:
            pass


def extract_mentions(text: str, members: list[str]) -> list[str]:
    """Parse ``@localpart`` mentions from text, filtered to known member localparts.

    Order-preserving, de-duped, CASE-INSENSITIVE. ``@Mimir``/``@MIMIR``/``@mimir`` all
    resolve to the same member and return its canonical localpart, so a capitalized
    mention wakes the bot. Generic: knows no bot names — only what's in ``members``.
    """
    tokens = re.findall(r"@([A-Za-z0-9._-]+)", text or "")
    by_lower = {m.lower(): m for m in members}  # lowercased mention -> canonical localpart
    seen, out = set(), []
    for t in tokens:
        m = by_lower.get(t.lower())
        if m is not None and m not in seen:
            seen.add(m)
            out.append(m)
    return out


# ---------------------------------------------------------------------------
# a2a directory — resolve LIVE bot url + POST the wake. Injectable http client.
# ---------------------------------------------------------------------------


class AgentDirectory:
    """Resolves a bot's live a2a URL from the a2abridge directory and POSTs a wake.

    The directory GET + the wake POST both go through an injectable ``httpx.AsyncClient``
    (tests supply one built on ``httpx.MockTransport`` — no live network). No fleet
    address is hardcoded; the directory URL comes from ``config.a2a_directory_url``.
    """

    def __init__(self, config: Config, client: httpx.AsyncClient):
        self.config = config
        self.client = client
        self._name_to_url: dict[str, str] = {}   # cached bot-name(lower) → live a2a url
        self._refreshed_at: float = 0.0
        self._ttl = float(os.environ.get("A2A_DIRECTORY_TTL", "30"))

    async def _agent_name(self, entry: dict, agent_url: str) -> str:
        """Identity for a directory entry. 🔴 The a2abridge /agents endpoint lists
        ``{url,lastSeen}`` WITHOUT a name — the name lives in each agent's
        ``/.well-known/agent-card.json``. Tolerate an inline ``name`` (tests / a future
        directory), else fetch the card. Returns '' on any failure."""
        inline = entry.get("name") or (entry.get("card") or {}).get("name")
        if inline:
            return str(inline)
        try:
            resp = await self.client.get(agent_url.rstrip("/") + "/.well-known/agent-card.json",
                                         timeout=5.0)
            resp.raise_for_status()
            return str((resp.json() or {}).get("name") or "")
        except Exception:
            return ""  # dead/unreachable agent → skip it; must not break the whole resolve

    async def _refresh(self) -> None:
        """Rebuild the name→url map from the directory. Fetches agent cards CONCURRENTLY (the
        directory has dead entries — stale worktree bots — that must be skipped, not serialize
        or fail resolve). On a duplicate name, prefer the entry with the freshest lastSeen."""
        url = self.config.a2a_directory_url
        if not url:
            return
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning("a2a directory GET %s failed: %s", url, e)
            return
        entries = data.get("agents", data) if isinstance(data, dict) else data
        if not isinstance(entries, list):
            log.warning("a2a directory returned non-list (%s)", type(data).__name__)
            return
        valid = [e for e in entries if isinstance(e, dict) and e.get("url")]
        names = await asyncio.gather(
            *(self._agent_name(e, e["url"]) for e in valid), return_exceptions=True
        )
        mapping: dict[str, str] = {}
        freshest: dict[str, str] = {}
        for e, name in zip(valid, names):
            if isinstance(name, BaseException) or not name:
                continue
            key = str(name).lower()
            last_seen = str(e.get("lastSeen") or "")
            if key in mapping and freshest.get(key, "") >= last_seen:
                continue  # already have a fresher-or-equal entry for this name
            mapping[key] = e["url"]
            freshest[key] = last_seen
        if mapping:
            self._name_to_url = mapping
            self._refreshed_at = time.monotonic()

    async def resolve(self, bot_id: str) -> Optional[str]:
        """Return the live a2a URL for ``bot_id`` from the a2abridge directory, or None.

        The directory lists urls only (no names); identity comes from each agent's card, so we
        build+cache a name→url map (TTL ``A2A_DIRECTORY_TTL``, default 30s) and refresh on a
        cache MISS so a newly-appeared bot resolves promptly. Logs the result — resolve()
        silently returning None (the old name-inline assumption) is exactly what pinned every
        wake in the hold-queue."""
        key = bot_id.lower()
        if key not in self._name_to_url or (time.monotonic() - self._refreshed_at) > self._ttl:
            await self._refresh()
        target = self._name_to_url.get(key)
        if target is None:
            log.warning("resolve(%s) → None (not in a2a directory: %d agents known)",
                        bot_id, len(self._name_to_url))
        else:
            log.info("resolve(%s) → %s", bot_id, target)
        return target

    async def wake(self, agent_url: str, bot_id: str, payload: dict) -> bool:
        """Wake a bot by POSTing an A2A ``message/send`` to its agent endpoint.

        🔴 a2abridge speaks the A2A JSON-RPC protocol — the wake must be a ``message/send``
        request to the agent's OWN url (NOT a plain JSON POST to ``<url>/a2a``; that never lands
        in the bot's a2a inbox). The human-readable notification is ``payload['text']`` (e.g.
        "[alice@example.com reacted 👍 on msg 27 via Delta Chat]"), delivered as the message's
        text part — matching the proven single-bot pattern. True iff the request was accepted (2xx).
        """
        text = payload.get("text") or f"[Delta Chat] wake for {bot_id}"
        mid = uuid.uuid4().hex
        # Metadata a2abridge-lite reads off the message (msg.Metadata[...]):
        #   notification=True → lite TERMINALIZES the wake task at delivery (live fleet-wide,
        #     proven 2026-07-21) so there's no completable task to mis-manage. All wakes are
        #     fire-and-forget. Message-level placement (params.message.metadata), confirmed.
        #   from=<sender localpart> → lite carries it onto the inbox item so the UserPromptSubmit
        #     hook renders "From `<sender>`" instead of the "someone" fallback. Forwarded ONLY
        #     when resolved (see handle_inbound); the text still carries a human-readable label.
        #   sender_kind=human|bot → lets the hook prioritize a real person over peer-bot chatter.
        #   reply_target=<dict> → the STRUCTURED, machine-readable reply handle (mirrors what
        #     delta_send / delta_send_channel need to address a reply): a consumer replies
        #     WITHOUT parsing the "[↳ Reply here …]" prose out of the text. Carries the exact
        #     identifiers — a "dm" target {kind,bot_id,chat_id} → delta_send(bot_id, target=chat_id);
        #     a "channel" target {kind,channel_id} → delta_send_channel(channel_id). Built in
        #     handle_inbound (the only place that knows own-bot + dm-vs-group); the human-readable
        #     hint stays in the text (backward-compatible), this is the parse-free companion.
        meta = {"notification": True}
        sender = payload.get("from")
        if sender:
            meta["from"] = sender
        sender_kind = payload.get("sender_kind")
        if sender_kind:
            meta["sender_kind"] = sender_kind
        reply_target = payload.get("reply_target")
        if reply_target:
            meta["reply_target"] = reply_target
        envelope = {
            "jsonrpc": "2.0", "id": mid, "method": "message/send",
            "params": {"message": {
                "role": "user", "messageId": mid, "kind": "message",
                "parts": [{"kind": "text", "text": text}],
                "metadata": meta,
            }},
        }
        try:
            resp = await self.client.post(agent_url.rstrip("/"), json=envelope)
            resp.raise_for_status()
            log.info("wake %s → %s (%s)", bot_id, agent_url, resp.status_code)
            return True
        except Exception as e:
            log.warning("wake %s → %s FAILED: %s", bot_id, agent_url, e)
            return False


# ---------------------------------------------------------------------------
# Durable per-bot hold-queue — JSON on the local DATA_DIR. Survives reload.
# ---------------------------------------------------------------------------


class HoldQueue:
    """Durable per-bot pending-wake queue (JSON on the local data dir).

    A wake that can't be delivered (URL unresolvable or POST failed) is persisted here and
    retried on the next inbound tick / drain. Idempotent: a (bot_id, chat_id, msg_id)
    triple is stored at most once, so repeated holds of the same event don't pile up.
    Survives a process reload because it's read back from disk on construction.
    """

    def __init__(self, data_dir: str):
        self.path = Path(data_dir) / "hold_queue.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items: list[dict] = self._read()

    def _read(self) -> list[dict]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text()) or []
            except Exception:
                return []
        return []

    def _flush(self) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._items))
        tmp.replace(self.path)  # atomic

    @staticmethod
    def _key(item: dict) -> tuple:
        return (item.get("bot_id"), item.get("chat_id"), item.get("msg_id"))

    def add(self, bot_id: str, payload: dict) -> None:
        """Persist a pending wake for ``bot_id``. Idempotent on (bot_id,chat_id,msg_id)."""
        item = {"bot_id": bot_id, **payload}
        k = self._key(item)
        if any(self._key(existing) == k for existing in self._items):
            return
        self._items.append(item)
        self._flush()

    def pending(self) -> list[dict]:
        return list(self._items)

    def remove(self, item: dict) -> None:
        k = self._key(item)
        before = len(self._items)
        self._items = [i for i in self._items if self._key(i) != k]
        if len(self._items) != before:
            self._flush()

    def __len__(self) -> int:
        return len(self._items)


# ---------------------------------------------------------------------------
# PeerMesh — LAZY per-pair securejoin + queue-until-verified for bot↔bot 1:1.
# ---------------------------------------------------------------------------


@dataclass
class _PendingPeerMsg:
    """One queued bot→bot 1:1 message, awaiting the pair's securejoin verification.

    ``text`` is the message body; ``enqueued`` the monotonic timestamp at enqueue (for TTL
    age-out). Runtime-ephemeral — never persisted (the queue is in-memory, see PeerMesh)."""
    text: str
    enqueued: float


class PeerMesh:
    """LAZY, on-demand peer↔peer securejoin mesh with a QUEUE-UNTIL-VERIFIED buffer.

    The a2a→Delta cutover requires every pair of bots to be VERIFIED KEY-CONTACTS of each
    other before ``send_to_addr`` can resolve a 1:1 chat (an unverified Autocrypt-gossip
    contact 404s "no contact for address"). Establishing every pair upfront is wasteful; this
    establishes a pair on its FIRST 1:1 send and buffers the message until verification lands.

    Mechanism (mirrors ``main.securejoin_star`` for the per-pair op, but on-demand + buffered):
      * ``send_to_peer(sender, target)``:
          - target already a verified key-contact of sender → send IMMEDIATELY (no securejoin,
            no enqueue) — IDEMPOTENT.
          - else → initiate the securejoin ONCE per pair (idempotent; skipped if already
            in-flight), ENQUEUE the message keyed on (sender, target_addr), return "queued".
            Never blocks on verification.
      * ``flush_verified(sender_accid, target_addr)`` (called from the securejoin-verified
        EVENT, composed with the existing on_verified): sends the pair's queued messages
        IN-ORDER, EXACTLY-ONCE (pop-then-send; the queue is cleared atomically before send so a
        re-fired event can't double-send), then clears the in-flight marker.

    BOUNDED: a per-pair cap (``max_per_pair``) and an age-out TTL (``ttl``) prevent unbounded
    growth if a securejoin never verifies. Dropped/aged-out messages are logged LOUDLY
    (warning/error) — never silently swallowed.

    Runtime-ephemeral: the queue + in-flight set live only in memory. Securejoin invites are
    created + consumed inline and never stored (secret-hygiene: nothing persisted/committed).
    v1 tradeoff: in-memory means a process restart drops still-pending (unverified) messages —
    acceptable because an un-verified pair's send hasn't landed anyway and the sender can retry;
    durability (a HoldQueue-style JSON store) is a follow-up if warranted.

    All state is mutated only from the single asyncio loop (send_to_peer via /send_to_peer's
    to_thread wrapper resolves+mutates, flush from on_verified) — kept simple, no lock; the
    ops are short and the queue dict is only touched on that path.
    """

    def __init__(self, config: Config, backend: DeltaBackend, *,
                 max_per_pair: int = 50, ttl: float = 3600.0):
        self.config = config
        self.backend = backend
        self.max_per_pair = max_per_pair
        self.ttl = ttl
        # (sender_localpart, target_addr) -> [_PendingPeerMsg, ...] in enqueue order
        self._queues: dict[tuple[str, str], list[_PendingPeerMsg]] = {}
        # pairs whose securejoin has been initiated and not yet verified (in-flight) —
        # dedups re-invocation so we never re-handshake / dup contacts.
        self._inflight: set[tuple[str, str]] = set()

    def _peer_addr(self, target_bot: str) -> str:
        """Full Delta address for a roster bot id/localpart: ``{localpart}@{domain}``.
        Resolves via the roster (id or localpart) and falls back to using the given token as
        the localpart — generic, no names baked."""
        domain = self.config.mail_domain
        for b in self.config.roster:
            if target_bot in (b.id, b.localpart):
                return f"{b.localpart}@{domain}"
        return f"{target_bot}@{domain}"

    def _age_out(self, key: tuple[str, str], now: float) -> None:
        """Drop any queued messages for ``key`` older than TTL — LOUDLY (never silent)."""
        q = self._queues.get(key)
        if not q:
            return
        kept = [m for m in q if (now - m.enqueued) < self.ttl]
        dropped = len(q) - len(kept)
        if dropped:
            log.error("peer-mesh: AGED OUT %d message(s) for pair %s→%s — securejoin never "
                      "verified within %.0fs TTL (DROPPED, not delivered)",
                      dropped, key[0], key[1], self.ttl)
        if kept:
            self._queues[key] = kept
        else:
            self._queues.pop(key, None)

    def send_to_peer(self, sender_bot: str, target_bot: str, text: str) -> dict:
        """LAZY-establish the (sender→target) pair and send-or-queue ``text``.

        Returns one of:
          {"status":"sent", ...}                       — pair already verified, sent now
          {"status":"queued", "reason":"securejoin-pending", "queued": N, ...}
          {"status":"dropped", "reason":"pair-cap-exceeded", ...}   — cap hit, dropped LOUDLY

        Raises KeyError if the SENDER bot has no Delta account.
        """
        sender_accid = self.backend.account_id_for(sender_bot)
        if sender_accid is None:
            raise KeyError(f"no delta account for bot {sender_bot!r}")
        target_addr = self._peer_addr(target_bot)
        key = (sender_bot, target_addr)
        now = time.monotonic()

        # GATE 4 (idempotent): already a verified key-contact → send immediately, no securejoin.
        if self.backend.is_verified_key_contact(sender_accid, target_addr):
            # If a stale in-flight marker/queue lingers (verified out-of-band), flush the
            # backlog FIRST so the queued-earlier messages precede this live one (order-preserving).
            self._inflight.discard(key)
            flushed = self._flush_queue(sender_accid, key, now)
            chat_id, msg_id = self.backend.send_to_addr(sender_accid, target_addr, text)
            result = {"status": "sent", "account_id": sender_accid,
                      "chat_id": chat_id, "msg_id": msg_id, "target_addr": target_addr}
            if flushed:
                result["flushed_backlog"] = flushed
            return result

        # GATE (fail-fast, per review): a target with no Delta account on this relay is NOT
        # onboarded (not in the roster / never logged in) — REJECT fast + loud rather than enqueue
        # for a securejoin verification that will never arrive (slow-fail via age-out).
        target_accid = self.backend.account_id_for(target_bot)
        if target_accid is None:
            log.error("peer-mesh: target bot %r not onboarded (no Delta account on this relay) — "
                      "REJECTING send from %s (not enqueued; nothing to verify against)",
                      target_bot, sender_bot)
            return {"status": "rejected", "reason": "target-not-onboarded",
                    "target_addr": target_addr}

        # Onboarded but not yet verified → age out stale entries, then enforce the per-pair cap (GATE 2).
        self._age_out(key, now)
        q = self._queues.setdefault(key, [])
        if len(q) >= self.max_per_pair:
            log.error("peer-mesh: pair %s→%s at cap (%d) — DROPPING new message (securejoin "
                      "not yet verified); backlog not delivered", sender_bot, target_addr,
                      self.max_per_pair)
            return {"status": "dropped", "reason": "pair-cap-exceeded",
                    "cap": self.max_per_pair, "target_addr": target_addr}

        # GATE 4 (idempotent handshake): initiate the securejoin ONCE per pair.
        if key not in self._inflight:
            try:
                invite = self.backend.create_invite(sender_accid)  # ephemeral, never stored
                self.backend.secure_join(target_accid, invite)     # target accepts sender's invite
                self._inflight.add(key)
                log.info("peer-mesh: initiated securejoin for pair %s→%s (verification "
                         "completes async → verified event flushes the queue)",
                         sender_bot, target_addr)
            except Exception:
                log.exception("peer-mesh: securejoin initiation failed for pair %s→%s "
                              "(message still QUEUED for retry on next send/verify)",
                              sender_bot, target_addr)

        q.append(_PendingPeerMsg(text=text, enqueued=now))
        return {"status": "queued", "reason": "securejoin-pending",
                "queued": len(q), "account_id": sender_accid, "target_addr": target_addr}

    def _flush_queue(self, sender_accid: int, key: tuple[str, str], now: float) -> int:
        """Pop the pair's queue ATOMICALLY, then send each in order — EXACTLY-ONCE.

        The queue entry is removed BEFORE sending so a re-fired verified event (which re-calls
        this) finds nothing to flush → no dupes. Returns the count delivered. Aged-out entries
        are dropped LOUDLY first. On a send failure the remaining un-sent messages are dropped
        LOUDLY (they can't be re-flushed — the queue was already popped)."""
        self._age_out(key, now)          # drop TTL-expired (loud) before flushing
        q = self._queues.pop(key, None)  # ATOMIC pop → exactly-once
        if not q:
            return 0
        target_addr = key[1]
        sent = 0
        for i, m in enumerate(q):
            try:
                self.backend.send_to_addr(sender_accid, target_addr, m.text)
                sent += 1
            except Exception:
                remaining = len(q) - i
                log.error("peer-mesh: flush send failed for pair %s→%s after %d/%d delivered "
                          "— %d message(s) DROPPED (queue already popped, not re-flushable)",
                          key[0], target_addr, sent, len(q), remaining)
                break
        if sent:
            log.info("peer-mesh: flushed %d queued message(s) for verified pair %s→%s",
                     sent, key[0], target_addr)
        return sent

    def flush_verified(self, sender_accid: int, target_addr: str) -> int:
        """Flush the (sender_accid→target_addr) pair's queue — invoked from the securejoin
        VERIFIED event. Clears the in-flight marker and sends the backlog in-order/exactly-once.

        Returns the number of messages delivered (0 if no pair matches). Never raises for a
        non-matching event (composes with the existing on_verified handler)."""
        sender_lp = self.backend.localpart_for(sender_accid)
        if sender_lp is None:
            return 0
        key = (sender_lp, target_addr)
        self._inflight.discard(key)
        return self._flush_queue(sender_accid, key, time.monotonic())

    def pending_count(self) -> int:
        """Total queued (un-verified) messages across all pairs — for /healthz visibility."""
        return sum(len(q) for q in self._queues.values())

    def log_dropped_backlog(self) -> int:
        """GATE 3c: on shutdown/restart, LOUDLY log any un-flushed in-memory backlog (which pairs,
        how many) — an in-memory queue must NEVER silently drop on restart (that IS the silent
        swallow we gate against). Returns the total message count that would be lost. Durable
        persistence (the HoldQueue JSON pattern) is a nice-to-have follow-on if this proves painful."""
        pairs = [(k, len(q)) for k, q in self._queues.items() if q]
        total = sum(n for _, n in pairs)
        for (sender, target_addr), n in pairs:
            log.error("peer-mesh: SHUTDOWN with %d undelivered queued message(s) for pair %s→%s "
                      "— securejoin never verified; backlog LOST on restart (in-memory queue)",
                      n, sender, target_addr)
        if total:
            log.error("peer-mesh: SHUTDOWN dropped %d total queued message(s) across %d pair(s)",
                      total, len(pairs))
        return total


# ---------------------------------------------------------------------------
# Relay engine — ties backend + directory + hold-queue together.
# ---------------------------------------------------------------------------


class Relay:
    """The engine. Owns the send path, the inbound wake path, and the hold-queue drain.

    All collaborators are injected → fully unit-testable with no rpc-server, no network.
    """

    def __init__(self, config: Config, backend: DeltaBackend, directory: AgentDirectory,
                 hold_queue: HoldQueue, peer_mesh: "Optional[PeerMesh]" = None):
        self.config = config
        self.backend = backend
        self.directory = directory
        self.hold = hold_queue
        # LAZY peer↔peer securejoin mesh (queue-until-verified) for bot↔bot 1:1 sends.
        # Additive: does NOT touch send/send_to_addr/securejoin_star/the All-Hands group.
        self.peer_mesh = peer_mesh or PeerMesh(config, backend)
        # Wake dedup: the same group message is delivered to EVERY member account in this one
        # process, so each account's handle_inbound would independently wake the same target
        # (N× amplification in an N-member room). Key on (rfc724_mid, target) — the GLOBAL
        # message-id is identical across copies — and wake each target at most once per TTL.
        # Touched only from handle_inbound/handle_reaction on the single asyncio loop → no lock.
        self._wake_dedup: dict[tuple[str, str], float] = {}
        self._wake_dedup_ttl = 300.0

    def _wake_once(self, mid: str, target: str) -> bool:
        """True if (mid, target) should wake now (records it); False if already woken within TTL.
        Empty mid (shouldn't happen) → fail-open (always wake), never suppress on missing id."""
        if not mid:
            return True
        now = time.monotonic()
        cache = self._wake_dedup
        if len(cache) > 4096:  # opportunistic prune of expired entries
            for k, ts in list(cache.items()):
                if now - ts >= self._wake_dedup_ttl:
                    del cache[k]
        key = (mid, target)
        seen = cache.get(key)
        if seen is not None and (now - seen) < self._wake_dedup_ttl:
            return False
        cache[key] = now
        return True

    # -- outbound (/send) --------------------------------------------------
    def send(self, bot: str, target: int, text: str) -> dict:
        """Send ``text`` from bot ``bot`` (id or localpart) into chat/contact ``target``.

        Returns {"status","msg_id","account_id"}. Raises KeyError if the bot has no account.
        """
        accid = self.backend.account_id_for(bot)
        if accid is None:
            raise KeyError(f"no delta account for bot {bot!r}")
        msg_id = self.backend.send(accid, int(target), text)
        return {"status": "sent", "msg_id": msg_id, "account_id": accid}

    def send_to_addr(self, bot: str, addr: str, text: str) -> dict:
        """Send ``text`` from ``bot`` to the HUMAN at email ``addr`` (resolves their 1:1 chat).

        Returns {"status","account_id","chat_id","msg_id"}. Raises KeyError if the bot has no
        account or no contact resolves for ``addr`` (map to 404 at the endpoint)."""
        accid = self.backend.account_id_for(bot)
        if accid is None:
            raise KeyError(f"no delta account for bot {bot!r}")
        chat_id, msg_id = self.backend.send_to_addr(accid, addr, text)
        return {"status": "sent", "account_id": accid, "chat_id": chat_id, "msg_id": msg_id}

    def send_to_peer(self, sender_bot: str, target_bot: str, text: str) -> dict:
        """Send ``text`` from ``sender_bot`` to a ROSTER peer ``target_bot`` over the LAZY
        securejoin mesh: sends immediately if the pair is already a verified key-contact, else
        initiates the per-pair securejoin (idempotent) and QUEUES the message until verified.

        Distinct from ``send_to_addr`` (reach a HUMAN by raw address, no queue) — this is the
        bot↔bot path that self-establishes verification on first use. Returns the PeerMesh
        result dict ({"status":"sent"|"queued"|"dropped", ...}). Raises KeyError if the sender
        bot has no Delta account (map to 404 at the endpoint)."""
        return self.peer_mesh.send_to_peer(sender_bot, target_bot, text)

    def flush_verified_pair(self, account_id: int, addr: str) -> int:
        """On a securejoin-VERIFIED event, flush any peer-mesh messages queued for the
        (account_id→addr) pair — in order, exactly-once. Returns the count delivered.
        Composed alongside provision_verified_member (does not replace it)."""
        return self.peer_mesh.flush_verified(account_id, addr)

    def _accid(self, bot: str) -> int:
        """Resolve a bot id/localpart to its Delta account id. KeyError if none."""
        accid = self.backend.account_id_for(bot)
        if accid is None:
            raise KeyError(f"no delta account for bot {bot!r}")
        return accid

    # -- contacts / channels ----------------------------------------------
    def list_contacts(self, bot: str) -> dict:
        """List ``bot``'s known contacts. Returns {"account_id","contacts":[...]}."""
        accid = self._accid(bot)
        return {"account_id": accid, "contacts": self.backend.list_contacts(accid)}

    def list_channels(self, bot: str) -> dict:
        """List ``bot``'s group chats. Returns {"account_id","channels":[...]}."""
        accid = self._accid(bot)
        return {"account_id": accid, "channels": self.backend.list_channels(accid)}

    def send_channel(self, bot: str, channel_id: int, text: str) -> dict:
        """Send ``text`` from ``bot`` into group chat ``channel_id`` (same path as send)."""
        accid = self._accid(bot)
        msg_id = self.backend.send(accid, int(channel_id), text)
        return {"status": "sent", "msg_id": msg_id, "account_id": accid, "channel_id": int(channel_id)}

    def create_channel(self, bot: str, name: str, members: list[str]) -> dict:
        """Create a group chat owned by ``bot`` with ``members`` (from args — never baked)."""
        accid = self._accid(bot)
        chat_id = self.backend.create_channel(accid, name, list(members or []))
        return {"status": "created", "channel_id": chat_id, "account_id": accid,
                "name": name, "members": list(members or [])}

    def add_member(self, bot: str, channel_id: int, contact: str) -> dict:
        """Add ``contact`` (from args) to ``bot``'s group chat ``channel_id``."""
        accid = self._accid(bot)
        self.backend.add_member(accid, int(channel_id), contact)
        return {"status": "added", "channel_id": int(channel_id), "account_id": accid,
                "contact": contact}

    def react(self, bot: str, chat_id: int, msg_id: int, emoji: str) -> dict:
        """Set reaction ``emoji`` on message ``msg_id`` (in ``chat_id``) as ``bot``."""
        accid = self._accid(bot)
        self.backend.react(accid, int(msg_id), emoji)
        return {"status": "reacted", "account_id": accid, "chat_id": int(chat_id),
                "msg_id": int(msg_id), "emoji": emoji}

    def list_messages(self, bot: str, chat_id: int, limit: int = 20) -> dict:
        """Recent messages in ``chat_id`` for ``bot``. Returns {"account_id","chat_id",
        "messages":[{id,text,from_id}, ...]} — the read-back side (confirm receipt)."""
        accid = self._accid(bot)
        return {"account_id": accid, "chat_id": int(chat_id),
                "messages": self.backend.list_messages(accid, int(chat_id), int(limit))}

    def create_invite(self, bot: str) -> dict:
        """Generate ``bot``'s securejoin contact-invite link (a human taps it to verify-contact
        the bot). Returns {"account_id","invite"}."""
        accid = self._accid(bot)
        return {"account_id": accid, "invite": self.backend.create_invite(accid)}

    def secure_join(self, bot: str, invite: str) -> dict:
        """Accept a securejoin/verified invite as ``bot`` → the inviter becomes a verified
        key-contact (E2E key-exchange), so they can then be added to an encrypted channel.
        Returns {"status","account_id","chat_id"}."""
        accid = self._accid(bot)
        chat_id = self.backend.secure_join(accid, invite)
        return {"status": "securejoin-initiated", "account_id": accid, "chat_id": chat_id}

    def delete_chat(self, bot: str, chat_id: int) -> dict:
        """Delete ``chat_id`` for ``bot`` — clears a stale/tangled securejoin so a single clean
        one can complete. Returns {"status","account_id","chat_id"}."""
        accid = self._accid(bot)
        self.backend.delete_chat(accid, int(chat_id))
        return {"status": "deleted", "account_id": accid, "chat_id": int(chat_id)}


    # -- wake routing ------------------------------------------------------
    def _channel_main(self, members: list[str]) -> Optional[str]:
        """Pick the channel 'main' (realm lead) for a set of members, generically.

        Uses ``config.realm_leads`` (realm → lead bot id). Returns the first lead that is a
        member. Knows no bot names itself.
        """
        member_set = set(members)
        by_id = {b.id: b for b in self.config.roster}
        for lead in self.config.realm_leads.values():
            if lead in member_set:
                return lead
        # fall back: a member whose realm has a lead equal to itself
        for m in members:
            spec = by_id.get(m)
            if spec and self.config.realm_leads.get(spec.realm) == m:
                return m
        return None

    def _sender_kind(self, localpart: str) -> str:
        """Classify a resolved sender as 'bot' (a fleet-roster localpart) or 'human' (anyone
        else, including an unresolved sender). Drives the wake's metadata.sender_kind so a
        consumer's hook can prioritize a real person over peer-bot chatter. Roster membership is
        the deterministic fleet signal — humans (Justin/Elene/external) are never in the roster."""
        if localpart and any(localpart == b.localpart for b in self.config.roster):
            return "bot"
        return "human"

    async def handle_inbound(self, msg: InboundMessage) -> list[str]:
        """Route one inbound message → wake the right bot(s). Returns woken bot ids.

        GROUP message → the anti-thundering-herd ``routing.wake_targets`` (mentioned + main).
        1:1 (non-group, e.g. a human DM) → wake the RECEIVING bot (the account owner) so it
        sees direct messages, not just group traffic. Undeliverable targets are held.
        """
        payload = {"chat_id": msg.chat_id, "msg_id": msg.msg_id,
                   "text": f"[Delta Chat] {msg.text}\n" + _reply_hint("channel", "", msg.chat_id),
                   "reply_target": _reply_target("channel", "", msg.chat_id)}
        # Self-skip: a bot's own message echoed back to its own account never wakes anyone.
        own = self.backend.localpart_for(msg.account_id)
        if msg.from_localpart and own and msg.from_localpart == own:
            return []
        # Carry the resolved sender so consumers can label the wake ("who sent this") instead of
        # each rendering its own default for a missing sender. ``wake()`` forwards only
        # ``payload['text']`` into the a2a envelope, so the sender is baked into the text below;
        # ``from`` is kept as a structured mirror. from_localpart is resolved in _build_inbound;
        # fall back to "someone" when a contact address doesn't resolve.
        sender = msg.from_localpart or "someone"
        payload["from"] = sender
        # sender_kind (human|bot): forwarded in the wake metadata so the hook can prioritize a
        # real person over peer-bot chatter. "bot" iff the resolved sender is a fleet-roster
        # localpart; anyone else — incl. an unresolved sender — is "human" (err toward attention,
        # never silently deprioritize a possible person as unknown).
        payload["sender_kind"] = self._sender_kind(msg.from_localpart)
        if not msg.is_group:
            if not own:
                return []
            # Dedup the 1:1 wake too: a re-delivery/re-fetch of an already-handled DM (same global
            # rfc724_mid) must not re-wake the recipient. Group had this; the DM path didn't.
            if not self._wake_once(msg.rfc724_mid, own):
                return []
            payload["direct"] = True
            payload["text"] = (
                f"[Delta Chat DM from {sender}] {msg.text}\n"
                + _reply_hint("dm", own, msg.chat_id)
            )
            # Override the channel-default reply_target with the DM handle now that ``own`` (the
            # account to reply AS) is known — mirrors the text hint above, parse-free.
            payload["reply_target"] = _reply_target("dm", own, msg.chat_id)
            return [own] if await self._deliver(own, payload) else []
        main = self._channel_main(msg.members)
        targets = wake_targets(msg.mentioned, msg.members, main)
        # Sender-exclusion: never wake the bot that SENT the message (covers the leader posting
        # untagged → main==sender, and self-@mention) — regardless of which account surfaced it.
        if msg.from_localpart:
            targets = [t for t in targets if t != msg.from_localpart]
        woken: list[str] = []
        for bot in targets:
            # Global dedup: the same group message hits every member account in this process;
            # wake each target at most once (keyed on the global rfc724_mid).
            if not self._wake_once(msg.rfc724_mid, bot):
                continue
            if await self._deliver(bot, payload):
                woken.append(bot)
        return woken

    async def handle_reaction(self, r: InboundReaction) -> list[str]:
        """Route one inbound REACTION → wake the AUTHOR of the reacted-to message, with an
        envelope carrying {who, emoji, msg_id}. Returns woken bot ids (the single owner, or []).

        The reaction event is delivered to every member account in this process, so wake ONLY the
        account that AUTHORED the reacted-to message (``own_message``), and dedup on the reacted
        message's global id — otherwise a single reaction wakes every member (N× amplification)."""
        bot = self.backend.localpart_for(r.account_id)
        if not bot:
            return []
        if not r.own_message:
            return []  # not the author's account — the author's copy handles it
        if not self._wake_once(r.rfc724_mid, bot):
            return []  # already woken for this reaction via another account's copy
        who = r.from_addr or "someone"
        payload = {
            "chat_id": r.chat_id, "msg_id": r.msg_id, "reaction": r.emoji, "from": who,
            "text": f"[{who} reacted {r.emoji} on msg {r.msg_id} via Delta Chat]",
        }
        return [bot] if await self._deliver(bot, payload) else []

    async def _deliver(self, bot: str, payload: dict) -> bool:
        """Resolve ``bot``'s live url + POST the wake; hold it on any failure."""
        agent_url = await self.directory.resolve(bot)
        if agent_url and await self.directory.wake(agent_url, bot, payload):
            return True
        self.hold.add(bot, payload)
        return False

    async def drain_holds(self) -> int:
        """Retry every held wake; return how many were delivered this pass. Idempotent."""
        delivered = 0
        for item in self.hold.pending():
            bot = item["bot_id"]
            payload = {k: v for k, v in item.items() if k != "bot_id"}
            agent_url = await self.directory.resolve(bot)
            if agent_url and await self.directory.wake(agent_url, bot, payload):
                self.hold.remove(item)
                delivered += 1
            else:
                log.info("drain: held wake for %s NOT delivered (resolve=%s) — stays queued",
                         bot, agent_url)
        return delivered

    # -- inbound tick ------------------------------------------------------
    async def tick(self) -> dict:
        """One inbound pass: drain any pending events, then retry held wakes.

        ⚠ ``backend.next_inbound()`` (deltachat ``get_next_event``) BLOCKS the caller until an
        event exists — so this is NOT run on the asyncio loop in production. The service runs
        the blocking event stream in a dedicated thread (``main._event_pump``) and the
        hold-queue retry in ``main.drain_loop``. ``tick`` is kept as a composable unit for
        tests/manual drains (tests inject a non-blocking fake backend).

        Returns a small summary {"processed","woken","drained"} for observability/tests.
        """
        processed, woken = 0, 0
        while True:
            item = self.backend.next_inbound()
            if item is None:
                break
            processed += 1
            if isinstance(item, InboundReaction):
                woken += len(await self.handle_reaction(item))
            else:
                woken += len(await self.handle_inbound(item))
        drained = await self.drain_holds()
        return {"processed": processed, "woken": woken, "drained": drained}


# ---------------------------------------------------------------------------
# FastAPI app — internal /send endpoint (what the delta_send MCP tool calls).
# ---------------------------------------------------------------------------


class SendRequest(BaseModel):
    """/send request body — the published contract the delta_send MCP tool builds against.

    ``bot_id`` also accepts the field name ``localpart`` (same thing). ``target`` is a Delta
    chat id (a group) or contact id.
    """
    bot_id: str = Field(..., validation_alias=AliasChoices("bot_id", "localpart"))
    target: int
    text: str

    model_config = {"populate_by_name": True}


class SendResponse(BaseModel):
    status: str
    msg_id: int
    account_id: int


# The new operations reuse the same bot_id/localpart alias convention as SendRequest.
_BOT_ALIAS = AliasChoices("bot_id", "localpart")


class SendChannelRequest(BaseModel):
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    channel_id: int
    text: str
    model_config = {"populate_by_name": True}


class CreateChannelRequest(BaseModel):
    """Create a group chat. ``members`` are supplied by the CALLER (generic — never baked)."""
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    name: str
    members: list[str] = Field(default_factory=list)
    model_config = {"populate_by_name": True}


class AddMemberRequest(BaseModel):
    """Add ONE caller-supplied ``contact`` to a channel (generic — never baked)."""
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    channel_id: int
    contact: str
    model_config = {"populate_by_name": True}


class SendToRequest(BaseModel):
    """Send to a HUMAN by email ``addr`` (resolves their 1:1 chat) — the reach-a-person path."""
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    addr: str
    text: str
    model_config = {"populate_by_name": True}


class SendToPeerRequest(BaseModel):
    """Send to a ROSTER peer bot over the lazy securejoin mesh (queue-until-verified).

    ``target`` is the peer's bot id/localpart (NOT a raw address — the relay resolves it to
    ``{localpart}@{domain}`` from the roster). On first 1:1 to an unverified peer the relay
    initiates the per-pair securejoin and queues the message until verified."""
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    target: str
    text: str
    model_config = {"populate_by_name": True}


class ReactRequest(BaseModel):
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    chat_id: int
    msg_id: int
    emoji: str
    model_config = {"populate_by_name": True}


class SecureJoinRequest(BaseModel):
    """Accept a securejoin/verified invite (link or QR content) as ``bot_id``."""
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    invite: str
    model_config = {"populate_by_name": True}


class DeleteChatRequest(BaseModel):
    """Delete a chat (e.g. clear a stale/tangled securejoin) for ``bot_id``."""
    bot_id: str = Field(..., validation_alias=_BOT_ALIAS)
    chat_id: int
    model_config = {"populate_by_name": True}


def create_app(relay: Relay):
    """Build the internal FastAPI app around a ``Relay``. Imported lazily so tests that
    don't need HTTP don't pay for it."""
    from fastapi import FastAPI, HTTPException

    app = FastAPI(title="Delta Chat Fabric Relay", version="1.0")

    @app.on_event("shutdown")
    def _log_peer_backlog_on_shutdown():
        # GATE 3c: never silently drop the in-memory peer-mesh backlog on restart — log it loudly.
        relay.peer_mesh.log_dropped_backlog()

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "held": len(relay.hold),
                "peer_queued": relay.peer_mesh.pending_count()}

    @app.post("/send", response_model=SendResponse)
    async def send(req: SendRequest) -> SendResponse:
        # relay.send → sync (blocking) deltachat rpc; run it OFF the event loop.
        try:
            result = await asyncio.to_thread(relay.send, req.bot_id, req.target, req.text)
            return SendResponse(**result)
        except KeyError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:  # pragma: no cover - backend send failure
            raise HTTPException(status_code=502, detail=f"send failed: {e}")

    @app.post("/drain")
    async def drain():
        return {"drained": await relay.drain_holds(), "held": len(relay.hold)}

    # -- contacts / channels (mirror /send: 404 on unknown bot, 502 on backend error) --
    async def _run(fn):
        """Run a blocking relay/backend call OFF the event loop (deltachat rpc is
        synchronous), mapping errors to the same 404/502 contract."""
        try:
            return await asyncio.to_thread(fn)
        except KeyError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:  # pragma: no cover - backend failure
            raise HTTPException(status_code=502, detail=str(e))

    @app.get("/contacts")
    async def contacts(bot_id: str):
        return await _run(lambda: relay.list_contacts(bot_id))

    @app.get("/invite")
    async def invite(bot_id: str):
        return await _run(lambda: relay.create_invite(bot_id))

    @app.get("/channels")
    async def channels(bot_id: str):
        return await _run(lambda: relay.list_channels(bot_id))

    @app.get("/messages")
    async def messages(bot_id: str, chat_id: int, limit: int = 20):
        return await _run(lambda: relay.list_messages(bot_id, chat_id, limit))

    @app.post("/send_channel")
    async def send_channel(req: SendChannelRequest):
        return await _run(lambda: relay.send_channel(req.bot_id, req.channel_id, req.text))

    @app.post("/send_to")
    async def send_to(req: SendToRequest):
        """Message a HUMAN by email address (resolves their 1:1 chat) — 404 on unknown bot/addr."""
        return await _run(lambda: relay.send_to_addr(req.bot_id, req.addr, req.text))

    @app.post("/send_to_peer")
    async def send_to_peer(req: SendToPeerRequest):
        """Send to a ROSTER peer bot over the lazy securejoin mesh (queue-until-verified).
        Sends now if the pair is verified, else initiates the securejoin + queues. 404 on
        unknown sender bot; 200 with status queued/sent/dropped otherwise."""
        return await _run(lambda: relay.send_to_peer(req.bot_id, req.target, req.text))

    @app.post("/channel")
    async def create_channel(req: CreateChannelRequest):
        return await _run(lambda: relay.create_channel(req.bot_id, req.name, req.members))

    @app.post("/channel/member")
    async def add_member(req: AddMemberRequest):
        return await _run(lambda: relay.add_member(req.bot_id, req.channel_id, req.contact))

    @app.post("/react")
    async def react(req: ReactRequest):
        return await _run(lambda: relay.react(req.bot_id, req.chat_id, req.msg_id, req.emoji))

    @app.post("/secure_join")
    async def secure_join(req: SecureJoinRequest):
        return await _run(lambda: relay.secure_join(req.bot_id, req.invite))

    @app.post("/delete_chat")
    async def delete_chat(req: DeleteChatRequest):
        return await _run(lambda: relay.delete_chat(req.bot_id, req.chat_id))

    return app


def build_default(config: Optional[Config] = None) -> Relay:  # pragma: no cover
    """Wire the production Relay from env/config. Not exercised by unit tests (needs a live
    rpc-server + network); every collaborator here is injectable for tests."""
    config = config or Config.load()
    accounts_dir = os.environ.get("ACCOUNTS_DIR", "/data/accounts")
    data_dir = os.environ.get("DATA_DIR", "/data")
    backend = DeltaChat2Backend(config, accounts_dir)
    client = httpx.AsyncClient(timeout=10.0)
    directory = AgentDirectory(config, client)
    hold = HoldQueue(data_dir)
    return Relay(config, backend, directory, hold)
