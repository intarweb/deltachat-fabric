"""Service entrypoint — wires the generic Delta Chat Fabric into one asyncio process.

Runs these concurrently in one process (all gated so a unit test never fires real IMAP/rpc/net):
  1. uvicorn serving the relay's FastAPI app (the /send + channel/contact/react contract)
  2. a SECOND uvicorn serving the MCP server at ``/mcp`` (streamable-HTTP)
  3. the periodic reconciler  (onboard every roster bot's account INTO THE DELTACHAT CORE via
                               add_account + add_or_update_transport = create-on-login+configure,
                               minting+storing a per-bot password to a local secrets file)
  4. the inbound EVENT PUMP   (a DEDICATED THREAD — deltachat's get_next_event() long-polls /
                               BLOCKS, so it must NOT run on the asyncio loop or it freezes
                               uvicorn; the thread bridges each incoming msg onto the loop)
  5. the hold-queue drain loop (retry undeliverable wakes)
  6. the nightly backup loop   (app.backup — deltachat imex export per account)

Generic-engine rule (hard): ZERO fleet identity is baked here. Domain, imap host, roster,
directory URL, ports, dirs, intervals — ALL from ``app.config.Config`` + env. This file
only imports+wires config/reconciler/routing/relay/backup; it adds no fleet-specific logic.

Env contract (all optional-with-defaults except the domain):
  DELTA_MAIL_DOMAIN         (config)  mail domain accounts live under          — REQUIRED
  DELTA_IMAP_HOST/_PORT     (config)  IMAP endpoint (create-on-login)
  DELTA_SUBMISSION_HOST/_PORT (config) SMTP submission endpoint
  DELTA_ROSTER_PATH         (config)  mounted roster YAML          default /config/roster.yaml
  A2A_DIRECTORY_URL         (config)  a2abridge directory for live wake URLs
  DATA_DIR                            LOCAL account-DB + hold-queue dir        default /data
  ACCOUNTS_DIR                        deltachat accounts dir       default $DATA_DIR/accounts
  DELTA_SECRETS_PATH                  local per-bot password store default $DATA_DIR/secrets.json
  DELTA_BACKUP_DIR                    imex backup dir              default /backup
  DELTA_BACKUP_RETAIN                 backups kept per account     default 7
  DELTA_BACKUP_INTERVAL               backup loop seconds          default 86400
  DELTA_RECONCILE_INTERVAL            reconciler loop seconds      default 3600
  RELAY_HOST / RELAY_PORT             relay uvicorn bind       default 0.0.0.0 / 8080
  DELTA_RECONCILE_ON_START            "1" to reconcile once at boot    default 1
  DELTA_MCP_HOST / DELTA_MCP_PORT     MCP /mcp bind   default 0.0.0.0 / 8000
  DELTA_LOG_LEVEL                     stdlib log level (stdout)    default INFO

The MCP server (app.mcp_server) is served as a SECOND uvicorn in the same asyncio process,
exposing the 7 delta tools over streamable-HTTP at ``/mcp`` for an MCP gateway. It talks to the
relay over loopback HTTP (RELAY_URL), so the relay's internal contract stays unchanged.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

from . import backup as backup_mod
from . import reconciler
from .config import Config
from .relay import InboundReaction, InboundVerified, Relay, build_default, create_app

log = logging.getLogger("dcf")


# ---------------------------------------------------------------------------
# Local secrets store — per-bot Delta password (mode-600 JSON).
# Generic: it's just a keyed file at a path from env; no fleet identity.
# ---------------------------------------------------------------------------


class SecretsStore:
    """Local per-bot password store — a mode-600 JSON at ``DELTA_SECRETS_PATH``.

    ``get_or_create`` mints a random password (via reconciler.gen_password, honoring the
    server's min length) the first time a bot is seen and persists it, so onboarding
    (create-on-login) is idempotent across restarts. This is the local, LOCAL-volume
    secrets file the spec calls for (a real deploy can point it at a locket-mounted path)."""

    def __init__(self, path: str, password_min_length: int = 9):
        self.path = Path(path)
        self.password_min_length = password_min_length
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, str] = self._read()

    def _read(self) -> dict[str, str]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text()) or {}
            except Exception:
                return {}
        return {}

    def _flush(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data))
        os.chmod(tmp, 0o600)
        tmp.replace(self.path)

    def get_or_create(self, localpart: str,
                      _gen: Callable[[], str] | None = None) -> str:
        """Return the stored password for ``localpart``, minting+persisting one if absent."""
        if localpart not in self._data:
            gen = _gen or (lambda: reconciler.gen_password(24, self.password_min_length))
            self._data[localpart] = gen()
            self._flush()
        return self._data[localpart]


# ---------------------------------------------------------------------------
# Onboarded-registry + state-loss guard.
#
# 🔴 The registry MUST live on a store that is MORE DURABLE than the deltachat accounts DB —
# i.e. next to the mounted roster (/config), NOT under DATA_DIR/ACCOUNTS_DIR. That is the whole
# point: if the accounts volume is replaced (recreate onto a fresh dir), the accounts + their
# keypairs are lost but the registry survives, so we can DETECT the loss. A registry on the same
# volume as the accounts would be wiped with them and could never signal.
# ---------------------------------------------------------------------------


class OnboardRegistry:
    """Durable record of bot localparts that have been successfully onboarded at least once.

    Anchors the state-loss guard: a roster bot that is ``known()`` here but whose deltachat
    account is absent at startup means the accounts DB was lost/replaced (a fresh keypair on
    re-onboard silently breaks every prior securejoin invite + verified contact). Default path is
    a sibling of the mounted roster (durable /config), overridable via ``DELTA_ONBOARD_REGISTRY``.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        self._data: set[str] = self._read()

    def _read(self) -> set[str]:
        if self.path.exists():
            try:
                return set(json.loads(self.path.read_text()) or [])
            except Exception:
                return set()
        return set()

    def _flush(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(sorted(self._data)))
        tmp.replace(self.path)

    def known(self) -> set[str]:
        return set(self._data)

    def mark(self, localparts) -> None:
        """Record one or more localparts as onboarded (idempotent; persists only on change)."""
        lps = [localparts] if isinstance(localparts, str) else list(localparts)
        new = [lp for lp in lps if lp not in self._data]
        if new:
            self._data.update(new)
            self._flush()

    @staticmethod
    def default_path(config: Config) -> str:
        """Registry path: env override, else a sibling of the roster (its durable /config mount)."""
        env = os.environ.get("DELTA_ONBOARD_REGISTRY")
        if env:
            return env
        roster = os.environ.get("DELTA_ROSTER_PATH", "/config/roster.yaml")
        return str(Path(roster).with_name(".dcf-onboarded.json"))


def detect_state_loss(config: Config, backend, registry: OnboardRegistry) -> list[str]:
    """Roster bots that were onboarded before (in the registry) but whose account is absent NOW.

    Non-empty = state loss: the accounts DB/keypairs were lost while the durable registry
    survived (typically a fresh/replaced ACCOUNTS_DIR). Run at STARTUP, before reconcile
    re-onboards them (which would silently re-create with fresh keypairs). Pure over the injected
    backend, so it's unit-testable. First-ever boot → registry empty → []; normal restart with
    durable data → accounts present → []; wiped data → the known bots show absent → alarm list.
    """
    known = registry.known()
    return [lp for lp in desired_localparts(config)
            if lp in known and backend.account_id_for(lp) is None]



# ---------------------------------------------------------------------------
# Reconciler loop — pure schedule decision + one testable pass.
# ---------------------------------------------------------------------------


def desired_localparts(config: Config) -> list[str]:
    """The roster's desired account localparts (de-duped, order-preserving). Pure."""
    seen: set[str] = set()
    out: list[str] = []
    for spec in config.roster:
        if spec.localpart not in seen:
            seen.add(spec.localpart)
            out.append(spec.localpart)
    return out


def desired_channels(config: Config) -> list[dict]:
    """One group chat PER REALM that has a lead, from the roster. Pure (Vikunja proj-23 5b).

    Returns ``[{realm, name, lead, members:[localpart,...]}]`` — the lead is the channel's
    "main" (owns/creates it + routing wakes it on an unaddressed message; see routing.py).
    A realm with no ``realm_leads`` entry is skipped (no main → can't route unaddressed msgs).
    Order-preserving by first appearance; members sorted + de-duped.

    🔴 ``lead`` is normalized to the lead's LOCALPART, not its roster bot-id. ``realm_leads``
    maps realm → bot **id** (e.g. ``ka``), but ``members`` (and the whole account/securejoin/
    channel path) key by **localpart** (e.g. ``keytesta1``). Without this normalization the
    downstream ``account_id_for(lead)`` (localpart-keyed) returns None and the realm is
    silently skipped whenever id ≠ localpart. When id == localpart it's a no-op, so existing
    same-id rosters are unaffected.
    """
    order: list[str] = []
    by_realm: dict[str, list[str]] = {}
    id_to_localpart: dict[str, str] = {}
    for spec in config.roster:
        id_to_localpart[spec.id] = spec.localpart
        if spec.realm not in by_realm:
            by_realm[spec.realm] = []
            order.append(spec.realm)
        if spec.localpart not in by_realm[spec.realm]:
            by_realm[spec.realm].append(spec.localpart)
    out: list[dict] = []
    for realm in order:
        lead_id = config.realm_leads.get(realm)
        if not lead_id:
            continue
        lead = id_to_localpart.get(lead_id, lead_id)  # bot-id → localpart (no-op if id==lp)
        out.append({"realm": realm, "name": realm, "lead": lead,
                    "members": sorted(by_realm[realm])})
    return out



def should_reconcile(now: float, last_run: Optional[float], interval: float,
                     run_on_start: bool = True) -> bool:
    """Pure loop-scheduling decision: should the reconciler fire this pass?

    True when never-run-and-run_on_start, or the interval has elapsed since last_run.
    Lets the loop be unit-tested without sleeping / touching IMAP.
    """
    if last_run is None:
        return run_on_start
    return (now - last_run) >= interval


# An onboard callable: (localpart, password) -> awaitable[bool]. Injected so reconcile_once
# is unit-testable with no live core; production wiring runs the blocking deltachat core
# onboarding (backend.ensure_account) in a thread via asyncio.to_thread.
Onboard = Callable[[str, str], Awaitable[bool]]


async def reconcile_once(config: Config, secrets: SecretsStore,
                         existing: list[str], *, onboard: Onboard,
                         registry: "Optional[OnboardRegistry]" = None) -> dict:
    """One reconcile pass: onboard each desired account into the deltachat core, then diff.

    ``existing`` = localparts already onboarded (caller supplies; the prune list is reported
    only — server-side removal is the operator's lane). ``onboard`` is the injectable
    onboarding seam (production = add_account+add_or_update_transport in a thread; tests
    inject a fake) so this is unit-testable with no live core. ``registry`` (opt) is the durable
    onboarded-registry: each successfully-provisioned bot is recorded so the startup state-loss
    guard can later tell "this bot was onboarded before" from "first-ever onboarding".

    Returns {"provisioned":[...],"failed":[...],"prune":[...],"to_provision":[...]}.
    """
    desired = desired_localparts(config)
    to_provision, to_prune = reconciler.reconcile(desired, existing)
    provisioned: list[str] = []
    failed: list[str] = []
    # (Re)assert every desired account (idempotent onboarding), not just new ones.
    for lp in desired:
        pw = secrets.get_or_create(lp)
        try:
            ok = await onboard(lp, pw)
        except Exception:
            log.exception("onboard raised for %s", lp)
            ok = False
        (provisioned if ok else failed).append(lp)
    if registry is not None and provisioned:
        registry.mark(provisioned)  # durable "known-onboarded" record (anchors the state-loss guard)
    if provisioned:
        log.info("reconcile: %d/%d account(s) onboarded", len(provisioned), len(desired))
    if failed:
        log.warning("reconcile: %d account(s) failed to onboard: %s", len(failed), failed)
    return {"provisioned": provisioned, "failed": failed, "prune": to_prune,
            "to_provision": to_provision}


def provision_channels(config: Config, backend) -> list[dict]:
    """Ensure ONE group chat per realm exists, created by the realm lead, with all realm
    members synced in (Vikunja proj-23 step 5b). Idempotent: matches an existing channel by
    name among the lead's channels and only adds MISSING members.

    Synchronous (deltachat rpc is blocking) — the loop runs it via ``asyncio.to_thread``.
    Uses only the injectable ``DeltaBackend`` surface (account_id_for/list_channels/
    create_channel/add_member), so it's unit-tested with a fake backend, no live core.
    A realm whose lead isn't onboarded yet is skipped this pass (retried next pass).
    """
    domain = config.mail_domain
    results: list[dict] = []
    for ch in desired_channels(config):
        lead, name = ch["lead"], ch["name"]
        lead_accid = backend.account_id_for(lead)
        if lead_accid is None:
            results.append({"realm": ch["realm"], "skipped": "lead-not-onboarded"})
            continue
        member_addrs = [f"{m}@{domain}" for m in ch["members"] if m != lead]
        try:
            existing = backend.list_channels(lead_accid) or []
            match = next((c for c in existing if c.get("name") == name), None)
            if match is None:
                chat_id = backend.create_channel(lead_accid, name, member_addrs)
                results.append({"realm": ch["realm"], "created": chat_id,
                                "members": len(member_addrs)})
            else:
                have = set(match.get("members", []))
                added = 0
                for m in ch["members"]:
                    if m != lead and m not in have:
                        backend.add_member(lead_accid, match["id"], f"{m}@{domain}")
                        added += 1
                results.append({"realm": ch["realm"], "channel_id": match["id"],
                                "added": added})
        except Exception:
            log.exception("channel provision failed for realm %s", ch["realm"])
            results.append({"realm": ch["realm"], "error": True})
    created = sum(1 for r in results if "created" in r)
    if created:
        log.info("reconcile: created %d per-realm channel(s)", created)
    return results


def securejoin_star(config: Config, backend) -> list[dict]:
    """Fire the securejoin STAR to each realm's lead, so the lead can populate the realm's
    encrypted group (Vikunja proj-23 step 5a — the prerequisite for 5b/``provision_channels``).

    🔴 Why the securejoin must happen: with break #1's key-contact fix, the lead can only
    ``create_channel``/``add_member`` a member who is a VERIFIED KEY-CONTACT of the lead
    (deltachat refuses "Only key-contacts can be added to encrypted chats"). Onboarding only
    create-on-logins each account — it never securejoins bots to each other. So for each realm
    we drive a star: the LEAD creates a securejoin invite and each non-lead member
    ``secure_join``s it. Both accounts live on THIS relay process, so we drive both sides locally.

    🔴 FULLY EVENT-DRIVEN, NO WAIT: this is fire-and-forget — it INITIATES each secure_join and
    returns immediately. It does NOT block re-checking verification (no sleep, no poll, no
    force_poll). The vc-request/vc-auth key-exchange completes asynchronously over the mail
    server's NATIVE IMAP IDLE push; when a member becomes a verified key-contact the core emits
    a securejoin-verified event, and THAT event (via the event pump → provision_verified_member)
    adds the member to the realm's encrypted channel. Verification is never waited on inline.

    A star (every member ↔ lead) is sufficient: once all members are in the lead-created group,
    members learn each OTHER's keys via Autocrypt gossip in the group — no full mesh needed.

    Idempotent: a (lead, member) pair whose member is already a verified key-contact of the lead
    is skipped (no re-handshake). Blocking rpc only (secure_join initiation) — the loop runs it
    via ``asyncio.to_thread``. Uses only the injectable ``DeltaBackend`` surface
    (account_id_for/create_invite/secure_join/is_verified_key_contact).

    Returns ``[{realm, initiated:[member,...], skipped_verified:[member,...], pending:[member,...]}]``
    where ``initiated`` = securejoin fired (verification arrives async → verified event provisions),
    ``pending`` = member not onboarded yet (retry next reconcile pass).
    """
    domain = config.mail_domain
    results: list[dict] = []
    for ch in desired_channels(config):
        lead = ch["lead"]
        lead_accid = backend.account_id_for(lead)
        if lead_accid is None:
            results.append({"realm": ch["realm"], "skipped": "lead-not-onboarded"})
            continue
        members = [m for m in ch["members"] if m != lead]
        initiated: list[str] = []
        skipped_verified: list[str] = []
        pending: list[str] = []
        for m in members:
            m_accid = backend.account_id_for(m)
            if m_accid is None:
                pending.append(m)  # member not onboarded yet → retry next pass
                continue
            m_addr = f"{m}@{domain}"
            if backend.is_verified_key_contact(lead_accid, m_addr):
                skipped_verified.append(m)  # already verified → idempotent no-op
                continue
            try:
                invite = backend.create_invite(lead_accid)
                backend.secure_join(m_accid, invite)  # member accepts the lead's invite
            except Exception:
                log.exception("securejoin-star %s → %s failed", m, lead)
                pending.append(m)
                continue
            # Fire-and-forget: the vc-request/vc-auth round-trips complete async over native IDLE.
            # The core's securejoin-verified event (handled by provision_verified_member) adds the
            # member to the channel — we do NOT wait/re-check here.
            initiated.append(m)
        results.append({"realm": ch["realm"], "initiated": initiated,
                        "skipped_verified": skipped_verified, "pending": pending})
        if initiated:
            log.info("reconcile: securejoin-star realm %s — %d member(s) initiated to lead %s "
                     "(verification completes async → event provisions)", ch["realm"],
                     len(initiated), lead)
        if pending:
            log.warning("reconcile: securejoin-star realm %s — %d member(s) not onboarded yet: %s",
                        ch["realm"], len(pending), pending)
    return results


def provision_verified_member(config: Config, backend, lead_accid: int,
                              addr: str) -> Optional[dict]:
    """Add a just-VERIFIED member to their realm's encrypted channel — the EVENT-DRIVEN half of
    provisioning (triggered by the core's securejoin-verified event, NOT a reconcile wait/poll).

    Given the lead account that emitted the verified event and the member's ``addr``, find the
    realm this bot leads where that member belongs, ensure the channel exists, and add the member
    (create-with-member if the channel doesn't exist yet, else add_member). Idempotent: a no-op if
    the member is already in the channel. Returns a small result dict, or None if the addr isn't a
    member of any realm this bot leads. Blocking rpc — the caller runs it off the loop.
    """
    lead_lp = backend.localpart_for(lead_accid)
    if lead_lp is None:
        return None
    member_lp = addr.split("@", 1)[0]
    domain = config.mail_domain
    for ch in desired_channels(config):
        if ch["lead"] != lead_lp or member_lp == lead_lp or member_lp not in ch["members"]:
            continue
        name = ch["name"]
        try:
            existing = backend.list_channels(lead_accid) or []
            match = next((c for c in existing if c.get("name") == name), None)
            if match is None:
                chat_id = backend.create_channel(lead_accid, name, [f"{member_lp}@{domain}"])
                log.info("provision(event): realm %s — created channel %s with %s",
                         ch["realm"], chat_id, member_lp)
                return {"realm": ch["realm"], "created": chat_id, "added": member_lp}
            if member_lp not in set(match.get("members", [])):
                backend.add_member(lead_accid, match["id"], f"{member_lp}@{domain}")
                log.info("provision(event): realm %s — added %s to channel %s",
                         ch["realm"], member_lp, match["id"])
                return {"realm": ch["realm"], "channel_id": match["id"], "added": member_lp}
            return {"realm": ch["realm"], "channel_id": match["id"], "already": member_lp}
        except Exception:
            log.exception("provision(event) failed: realm %s member %s", ch["realm"], member_lp)
            return {"realm": ch["realm"], "error": True}
    return None


async def reconciler_loop(config: Config, secrets: SecretsStore,
                          interval: float, run_on_start: bool,
                          existing_fn: Callable[[], list[str]],
                          *, onboard: Onboard,
                          after_reconcile: Optional[Callable[[], Awaitable]] = None,
                          registry: "Optional[OnboardRegistry]" = None,
                          _should_stop: Optional[Callable[[], bool]] = None) -> None:  # pragma: no cover - loop
    """Periodic reconciler. ``existing_fn`` supplies current onboarded localparts each pass
    (default in build wiring = the relay backend's account index). ``after_reconcile`` (opt)
    runs once per pass AFTER account onboarding — production wires it to per-realm channel
    provisioning (accounts must exist before their channels can be created). ``registry`` (opt)
    records successfully-onboarded bots for the state-loss guard."""
    last_run: Optional[float] = None
    while not (_should_stop and _should_stop()):
        now = asyncio.get_event_loop().time()
        if should_reconcile(now, last_run, interval, run_on_start):
            try:
                await reconcile_once(config, secrets, existing_fn(), onboard=onboard,
                                     registry=registry)
                if after_reconcile is not None:
                    await after_reconcile()
            except Exception:
                log.exception("reconcile pass failed")
            last_run = asyncio.get_event_loop().time()
        await asyncio.sleep(min(interval, 60.0))


# ---------------------------------------------------------------------------
# Inbound event pump — runs the BLOCKING deltachat event stream OFF the loop.
#
# 🔴 This is the fix for the startup-freeze bug: deltachat2's get_next_event()
# long-polls (blocks the calling thread until a core event). Running it on the
# asyncio event loop froze both uvicorns at "Waiting for application startup"
# (they could never finish lifespan startup → never bound). The canonical
# deltachat-rpc-client integration is a dedicated thread that blocks on the event
# stream and bridges each incoming message onto the asyncio loop via
# asyncio.run_coroutine_threadsafe. (Verified: adbenitez/deltachat2 +
# deltachat-bot/deltabot-cli-py both use the thread model.)
# ---------------------------------------------------------------------------


def _event_pump(backend, relay: Relay, loop: asyncio.AbstractEventLoop,
                *, on_verified: Optional[Callable[["InboundVerified"], Awaitable]] = None,
                _should_stop: Optional[Callable[[], bool]] = None,
                _submit: Optional[Callable[[Awaitable], object]] = None) -> None:
    """Consume the blocking deltachat event stream in THIS thread; dispatch each incoming
    message to ``relay.handle_inbound`` on the asyncio ``loop``. Never call this on the loop.

    ``on_verified`` (opt) is invoked with each ``InboundVerified`` — a member became a verified
    key-contact of a realm lead — so provisioning is EVENT-DRIVEN (no reconcile-time wait/poll).

    ``_submit`` (test seam) schedules a coroutine on the loop and waits for it; the default
    uses ``asyncio.run_coroutine_threadsafe`` — the standard cross-thread → asyncio bridge.
    """
    def _default_submit(coro):
        # FIRE-AND-FORGET: schedule the handler on the loop and return immediately. The old
        # ``.result(timeout=30)`` BLOCKED this pump thread up to 30s per message on a slow
        # directory/target — the pump only calls get_next_event again once submit returns, so a
        # slow a2a target stalled the WHOLE fleet's inbound and, on timeout, the coroutine's result
        # was dropped (wake never POSTed = silent loss). handle_inbound already holds+retries any
        # undeliverable wake via the HoldQueue, so we don't need the pump thread to wait. A rejected
        # schedule (loop closing) is logged, not raised.
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        def _log_err(f):
            try:
                f.result()
            except Exception:
                log.exception("inbound handler raised (scheduled fire-and-forget)")
        fut.add_done_callback(_log_err)
        return fut

    submit = _submit or _default_submit
    log.info("event pump thread started")
    while not (_should_stop and _should_stop()):
        try:
            msg = backend.next_inbound()  # BLOCKS until an event; None for non-incoming
        except StopIteration:  # test seam: exhausted fake stream
            break
        except Exception:
            log.exception("event stream read failed")
            time.sleep(1.0)
            continue
        if msg is None:
            continue
        try:
            if isinstance(msg, InboundReaction):
                submit(relay.handle_reaction(msg))  # human reacted → wake bot w/ {who,emoji,msg_id}
            elif isinstance(msg, InboundVerified):
                # a member became a verified key-contact of a realm lead → EVENT-DRIVEN provision
                # (add them to the realm's encrypted channel); no reconcile-time wait/poll.
                if on_verified is not None:
                    submit(on_verified(msg))
            else:
                # Relay is wake+deliver+surface ONLY — it must NOT react on the bot's behalf.
                # The 👀 proof-of-life is AGENT-side: the woken bot calls delta_react as its
                # first step. Auto-reacting here would fake liveness (👀 without a real wake).
                submit(relay.handle_inbound(msg))
        except Exception:
            log.exception("inbound dispatch failed")


async def drain_loop(relay: Relay, interval: float = 5.0,
                     *, _should_stop: Optional[Callable[[], bool]] = None) -> None:  # pragma: no cover - loop
    """Periodically retry undeliverable wakes parked in the hold-queue (async, loop-safe)."""
    while not (_should_stop and _should_stop()):
        try:
            await relay.drain_holds()
        except Exception:
            log.exception("hold-drain failed")
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Wiring — construct every collaborator. Importable + inspectable without I/O.
# ---------------------------------------------------------------------------


class Service:
    """Holds the wired collaborators so ``main`` (and tests) can inspect the wiring
    without starting any loop. Nothing here touches IMAP/rpc/network on construction
    beyond what the injected backend does."""

    def __init__(self, config: Config, relay: Relay, secrets: SecretsStore,
                 *, backup_backend: Optional[backup_mod.BackupBackend] = None,
                 registry: "Optional[OnboardRegistry]" = None):
        self.config = config
        self.relay = relay
        self.secrets = secrets
        self.backup_backend = backup_backend
        self.registry = registry
        self.app = create_app(relay)


def select_secret_backend(env: Optional[dict] = None) -> str:
    """Which per-bot credential store the reconciler uses — env-selectable so the chatmaild→
    Stalwart cutover is an ATOMIC env flip, not a code change:
      'opconnect'  → read/mint each bot's pw on the shared op-connect item (Stalwart era;
                     Stalwart validates it via LDAP→Authentik; the sync set_passwords the same)
      'local'      → mint-to-disk secrets.json (chatmaild era, create-on-login) — the DEFAULT
    Chosen by DELTA_SECRET_BACKEND=opconnect|local, or auto-'opconnect' when OP_CONNECT_URL +
    DELTA_BOT_CREDS_ITEM are both set. Pure → unit-tested; build_service builds the store.
    """
    e = env if env is not None else os.environ
    explicit = e.get("DELTA_SECRET_BACKEND", "").strip().lower()
    if explicit in ("opconnect", "local"):
        return explicit
    if e.get("OP_CONNECT_URL") and e.get("DELTA_BOT_CREDS_ITEM"):
        return "opconnect"
    return "local"


def build_service(config: Optional[Config] = None,
                  *, relay: Optional[Relay] = None) -> Service:  # pragma: no cover - real wiring
    """Production wiring: Config.load() → backend + directory + hold-queue → Relay →
    secrets store → Service. ``relay`` can be injected (tests). Not unit-run because the
    default relay needs a live rpc-server; every part is injectable so tests build their own.

    The secret store is env-selectable (``select_secret_backend``): local mint-to-disk today
    (chatmaild), op-connect read on the Stalwart cutover — both expose ``get_or_create(bot)``
    so the reconciler/onboard path is identical either way.
    """
    config = config or Config.load()
    data_dir = os.environ.get("DATA_DIR", "/data")
    secrets_path = os.environ.get("DELTA_SECRETS_PATH", str(Path(data_dir) / "secrets.json"))
    relay = relay or build_default(config)
    if select_secret_backend() == "opconnect":
        from .opconnect import OpConnectStore
        secrets = OpConnectStore(password_min_length=config.password_min_length)
        log.info("secret store: op-connect (Stalwart-era; reads/mints deltachat-bot-creds)")
    else:
        secrets = SecretsStore(secrets_path, config.password_min_length)
        log.info("secret store: local mint-to-disk (chatmaild-era)")
    backup_backend = backup_mod.DeltaChat2BackupBackend(relay.backend)
    registry = OnboardRegistry(OnboardRegistry.default_path(config))
    return Service(config, relay, secrets, backup_backend=backup_backend, registry=registry)


def _make_onboard(service: Service) -> Onboard:  # pragma: no cover - real core onboarding
    """Production onboard seam: run the BLOCKING deltachat core onboarding
    (backend.ensure_account) in a worker thread so the reconciler never blocks the loop."""
    cfg = service.config
    backend = service.relay.backend

    async def onboard(localpart: str, password: str) -> bool:
        return await asyncio.to_thread(
            backend.ensure_account, localpart, password,
            imap_host=cfg.imap_host, imap_port=cfg.imap_port,
            smtp_host=cfg.submission_host or cfg.imap_host, smtp_port=cfg.submission_port,
            display_name=cfg.display_name_for(localpart),
        )

    return onboard


async def _serve(service: Service) -> None:  # pragma: no cover - real uvicorn + loops
    """Start both uvicorns + the reconciler + the drain loop + the backup loop on the asyncio
    loop, and the BLOCKING deltachat event stream in a dedicated thread. One process."""
    import uvicorn

    cfg = service.config
    host = os.environ.get("RELAY_HOST", "0.0.0.0")
    port = int(os.environ.get("RELAY_PORT", os.environ.get("PORT", "8080")))
    reconcile_interval = float(os.environ.get("DELTA_RECONCILE_INTERVAL", "3600"))
    run_on_start = os.environ.get("DELTA_RECONCILE_ON_START", "1") == "1"
    backup_dir = os.environ.get("DELTA_BACKUP_DIR", "/backup")
    backup_retain = int(os.environ.get("DELTA_BACKUP_RETAIN", "7"))
    backup_interval = float(os.environ.get("DELTA_BACKUP_INTERVAL", "86400"))

    server = uvicorn.Server(uvicorn.Config(service.app, host=host, port=port, log_level="info"))

    # Second uvicorn: the MCP server at /mcp (streamable-HTTP). It reaches the relay over
    # loopback HTTP; the relay's own /send contract is untouched.
    from .mcp_server import build_mcp_app

    mcp_host = os.environ.get("DELTA_MCP_HOST", "0.0.0.0")
    mcp_port = int(os.environ.get("DELTA_MCP_PORT", "8000"))
    relay_url = os.environ.get("RELAY_URL", f"http://127.0.0.1:{port}")
    mcp_app = build_mcp_app(relay_url)
    mcp_server = uvicorn.Server(
        uvicorn.Config(mcp_app, host=mcp_host, port=mcp_port, log_level="info")
    )

    def existing_fn() -> list[str]:
        # onboarded localparts we know about = the relay backend's account index
        idx = getattr(service.relay.backend, "_localpart_to_accid", {})
        return list(idx.keys())

    async def after_reconcile() -> None:
        # securejoin_star FIRES the per-realm handshakes (fire-and-forget — verification completes
        # async over native IDLE and the verified EVENT provisions each member). provision_channels
        # is the idempotent catch-up for members ALREADY verified from persisted state (e.g. after a
        # restart, when no new event will fire for an already-verified contact). Neither waits/polls.
        await asyncio.to_thread(securejoin_star, cfg, service.relay.backend)
        await asyncio.to_thread(provision_channels, cfg, service.relay.backend)

    async def on_verified(ev: InboundVerified) -> None:
        # EVENT-DRIVEN provisioning: a member just became a verified key-contact of a realm lead →
        # add them to the realm's encrypted channel now. Runs off the loop (blocking rpc).
        # COMPOSED (not replaced): also flush any LAZY peer-mesh 1:1 messages queued for this
        # pair (queue-until-verified). Both run on every verified event — the channel provision
        # is a no-op for a non-realm pair, and the peer-mesh flush is a no-op for a pair with no
        # queued messages, so they compose cleanly.
        await asyncio.to_thread(provision_verified_member, cfg, service.relay.backend,
                                ev.account_id, ev.addr)
        await asyncio.to_thread(service.relay.flush_verified_pair, ev.account_id, ev.addr)

    loop = asyncio.get_running_loop()
    # 🔴 STATE-LOSS GUARD — run BEFORE reconcile re-onboards anything: any roster bot that was
    # onboarded before (durable registry) but whose account is absent NOW = the accounts DB was
    # lost/replaced (a fresh-keypair re-onboard would silently break every prior securejoin
    # invite + verified contact). Loud alarm; reconcile still proceeds to re-establish them.
    if service.registry is not None:
        lost = detect_state_loss(cfg, service.relay.backend, service.registry)
        if lost:
            log.error("🔴 STATE-LOSS: %d roster bot(s) were onboarded before but their account is "
                      "GONE now (accounts dir replaced?) — prior securejoin invites/verified "
                      "contacts for these are INVALID; they will re-onboard with FRESH keypairs: %s",
                      len(lost), lost)
    # 🔴 blocking deltachat event stream → dedicated daemon thread (NEVER on the loop).
    threading.Thread(
        target=_event_pump, args=(service.relay.backend, service.relay, loop),
        kwargs={"on_verified": on_verified},
        daemon=True, name="dcf-event-pump",
    ).start()

    log.info("serving relay on %s:%s and MCP /mcp on %s:%s", host, port, mcp_host, mcp_port)
    await asyncio.gather(
        server.serve(),
        mcp_server.serve(),
        drain_loop(service.relay),
        reconciler_loop(cfg, service.secrets, reconcile_interval, run_on_start, existing_fn,
                        onboard=_make_onboard(service),
                        after_reconcile=after_reconcile, registry=service.registry),
        backup_mod.run_forever(cfg, service.backup_backend, backup_dir,
                               retain=backup_retain, interval=backup_interval),
    )


def main() -> None:  # pragma: no cover - process entry
    logging.basicConfig(
        level=os.environ.get("DELTA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log.info("Delta Chat Fabric starting")
    service = build_service()
    asyncio.run(_serve(service))


if __name__ == "__main__":  # pragma: no cover
    main()
