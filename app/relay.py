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
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol

import httpx
from pydantic import AliasChoices, BaseModel, Field

from .config import Config
from .routing import wake_targets

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

    def next_inbound(self) -> Optional[InboundMessage]:
        """Pop the next incoming message from the event stream, or None if idle.

        Non-blocking: the inbound loop calls this repeatedly per tick. Returning None
        means "nothing waiting right now".
        """
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
                       smtp_host: str, smtp_port: int) -> bool:
        """Idempotently onboard a bot's mailbox into the deltachat core (create-on-login +
        configure) so it can send/receive. BLOCKING — callers run it off the event loop.
        Returns True iff the account is configured after the call. (Optional on fakes.)"""
        ...

    def secure_join(self, account_id: int, invite: str) -> int:
        """Accept a securejoin/verified-invite (link or QR) → the inviter becomes a verified
        key-contact; returns the resulting chat id. BLOCKING. (Optional on fakes.)"""
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

    # -- securejoin (accept a verified invite → inviter becomes a key-contact) ----
    def secure_join(self, account_id: int, invite: str) -> int:  # pragma: no cover
        """Accept a securejoin / verified-invite link (or QR content) for ``account_id``.

        This IS the key-exchange: on success the inviter becomes a VERIFIED KEY-CONTACT of
        this account (so they can then be added to an encrypted chat). Returns the resulting
        chat id. Verified vs the installed deltachat2 (``secure_join(account_id, qr) -> int``).
        Blocking (network handshake) — callers run it off the loop.
        """
        return self.rpc.secure_join(account_id, invite)

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

    def next_inbound(self) -> Optional[InboundMessage]:  # pragma: no cover - real rpc entry
        raw = self.rpc.get_next_event()
        if raw is None:
            return None
        # deltachat2 Event: the account id is ``context_id`` (verified vs installed package);
        # older bindings used ``account_id``/``accid``.
        accid = (getattr(raw, "context_id", None) or getattr(raw, "account_id", None)
                 or getattr(raw, "accid", None) or 0)
        ev = getattr(raw, "event", raw)
        chat_id, msg_id = self.incoming_ids(ev)
        if chat_id is None or msg_id is None:
            return None
        return self._build_inbound(accid, chat_id, msg_id)

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
        return InboundMessage(
            account_id=accid, chat_id=chat_id, msg_id=msg_id, text=text,
            is_group=is_group, members=members,
            mentioned=extract_mentions(text, members),
            from_id=getattr(msg, "from_id", 0) or 0,
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
        """Resolve a member address to a contact id (create_contact is create-or-get)."""
        return self.rpc.create_contact(account_id, contact, None)

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
                            "from_id": getattr(m, "from_id", 0) or 0})
            except Exception:
                continue
        return out

    def create_invite(self, account_id: int) -> str:  # pragma: no cover - real rpc
        # get_chat_securejoin_qr_code(accid, None) -> the account's securejoin CONTACT-invite
        # link (i.delta.chat/#...); a human taps it to become a verified contact. Verified vs
        # installed deltachat2.
        return self.rpc.get_chat_securejoin_qr_code(account_id, None)

    # -- onboarding (create-on-login + configure into the deltachat CORE) ---
    def ensure_account(self, localpart: str, password: str, *,
                       imap_host: str, imap_port: int,
                       smtp_host: str, smtp_port: int) -> bool:
        """Idempotently onboard a bot's mailbox INTO THE DELTACHAT CORE so it can send/receive.

        add_account() → add_or_update_transport(EnteredLoginParam(...)). The transport login
        create-on-logins the mailbox on a chatmail/Dovecot server AND configures the core
        account (writes the SQLCipher dc.db). BLOCKING — callers must run it off the event
        loop (main._make_onboard uses asyncio.to_thread).

        Verified against adbenitez/deltachat2: ``add_or_update_transport`` supersedes the
        deprecated (2025-02) ``configure``; ``EnteredLoginParam``/``Socket`` field names +
        ``is_configured``/``start_io`` signatures. Isolated here behind DeltaBackend so an API
        drift is a one-place fix. Returns True iff the account is configured after the call.
        """
        from deltachat2 import EnteredLoginParam, Socket  # type: ignore

        existing = self._localpart_to_accid.get(localpart)
        if existing is not None:
            try:
                if self.rpc.is_configured(existing):
                    return True  # already onboarded — idempotent no-op
            except Exception:
                pass
        accid = existing if existing is not None else self.rpc.add_account()
        try:
            self.rpc.set_config(accid, "bot", "1")  # mark as a bot account (best-effort)
        except Exception:
            pass
        param = EnteredLoginParam(
            addr=f"{localpart}@{self.config.mail_domain}",
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


def extract_mentions(text: str, members: list[str]) -> list[str]:
    """Parse ``@localpart`` mentions from text, filtered to known member localparts.

    Order-preserving, de-duped. Generic: knows no bot names — only what's in ``members``.
    """
    tokens = re.findall(r"@([A-Za-z0-9._-]+)", text or "")
    member_set = set(members)
    seen, out = set(), []
    for t in tokens:
        if t in member_set and t not in seen:
            seen.add(t)
            out.append(t)
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

    async def resolve(self, bot_id: str) -> Optional[str]:
        """Return the live a2a URL for ``bot_id`` from the directory, or None.

        Expects the directory to return either ``[{name,url}, ...]`` or
        ``{"agents": [{name,url}, ...]}``. Matches on ``name`` (case-insensitive).
        Any error / miss → None (caller then holds the wake).
        """
        url = self.config.a2a_directory_url
        if not url:
            return None
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return None
        agents = data.get("agents", data) if isinstance(data, dict) else data
        if not isinstance(agents, list):
            return None
        for a in agents:
            if not isinstance(a, dict):
                continue
            name = str(a.get("name", "")).lower()
            if name == bot_id.lower() and a.get("url"):
                return a["url"]
        return None

    async def wake(self, agent_url: str, bot_id: str, payload: dict) -> bool:
        """POST a wake/message to a resolved agent URL. True iff it was accepted (2xx)."""
        try:
            resp = await self.client.post(
                agent_url.rstrip("/") + "/a2a",
                json={"bot_id": bot_id, "kind": "wake", **payload},
            )
            resp.raise_for_status()
            return True
        except Exception:
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
# Relay engine — ties backend + directory + hold-queue together.
# ---------------------------------------------------------------------------


class Relay:
    """The engine. Owns the send path, the inbound wake path, and the hold-queue drain.

    All collaborators are injected → fully unit-testable with no rpc-server, no network.
    """

    def __init__(self, config: Config, backend: DeltaBackend, directory: AgentDirectory,
                 hold_queue: HoldQueue):
        self.config = config
        self.backend = backend
        self.directory = directory
        self.hold = hold_queue

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

    async def handle_inbound(self, msg: InboundMessage) -> list[str]:
        """Route one inbound group message → wake the right bot(s). Returns woken bot ids.

        Non-group messages are ignored (returns []). Delegates target selection to
        ``routing.wake_targets`` (the anti-thundering-herd rule). Undeliverable targets are
        parked in the hold-queue.
        """
        if not msg.is_group:
            return []
        main = self._channel_main(msg.members)
        targets = wake_targets(msg.mentioned, msg.members, main)
        payload = {"chat_id": msg.chat_id, "msg_id": msg.msg_id, "text": msg.text}
        woken: list[str] = []
        for bot in targets:
            if await self._deliver(bot, payload):
                woken.append(bot)
        return woken

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
            msg = self.backend.next_inbound()
            if msg is None:
                break
            processed += 1
            woken += len(await self.handle_inbound(msg))
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


def create_app(relay: Relay):
    """Build the internal FastAPI app around a ``Relay``. Imported lazily so tests that
    don't need HTTP don't pay for it."""
    from fastapi import FastAPI, HTTPException

    app = FastAPI(title="Delta Chat Fabric Relay", version="1.0")

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "held": len(relay.hold)}

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
