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
from .relay import Relay, build_default, create_app

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
    """
    order: list[str] = []
    by_realm: dict[str, list[str]] = {}
    for spec in config.roster:
        if spec.realm not in by_realm:
            by_realm[spec.realm] = []
            order.append(spec.realm)
        if spec.localpart not in by_realm[spec.realm]:
            by_realm[spec.realm].append(spec.localpart)
    out: list[dict] = []
    for realm in order:
        lead = config.realm_leads.get(realm)
        if not lead:
            continue
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
                         existing: list[str], *, onboard: Onboard) -> dict:
    """One reconcile pass: onboard each desired account into the deltachat core, then diff.

    ``existing`` = localparts already onboarded (caller supplies; the prune list is reported
    only — server-side removal is the operator's lane). ``onboard`` is the injectable
    onboarding seam (production = add_account+add_or_update_transport in a thread; tests
    inject a fake) so this is unit-testable with no live core.

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


async def reconciler_loop(config: Config, secrets: SecretsStore,
                          interval: float, run_on_start: bool,
                          existing_fn: Callable[[], list[str]],
                          *, onboard: Onboard,
                          after_reconcile: Optional[Callable[[], Awaitable]] = None,
                          _should_stop: Optional[Callable[[], bool]] = None) -> None:  # pragma: no cover - loop
    """Periodic reconciler. ``existing_fn`` supplies current onboarded localparts each pass
    (default in build wiring = the relay backend's account index). ``after_reconcile`` (opt)
    runs once per pass AFTER account onboarding — production wires it to per-realm channel
    provisioning (accounts must exist before their channels can be created)."""
    last_run: Optional[float] = None
    while not (_should_stop and _should_stop()):
        now = asyncio.get_event_loop().time()
        if should_reconcile(now, last_run, interval, run_on_start):
            try:
                await reconcile_once(config, secrets, existing_fn(), onboard=onboard)
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
                *, _should_stop: Optional[Callable[[], bool]] = None,
                _submit: Optional[Callable[[Awaitable], object]] = None) -> None:
    """Consume the blocking deltachat event stream in THIS thread; dispatch each incoming
    message to ``relay.handle_inbound`` on the asyncio ``loop``. Never call this on the loop.

    ``_submit`` (test seam) schedules a coroutine on the loop and waits for it; the default
    uses ``asyncio.run_coroutine_threadsafe`` — the standard cross-thread → asyncio bridge.
    """
    def _default_submit(coro):
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=30)

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
                 *, backup_backend: Optional[backup_mod.BackupBackend] = None):
        self.config = config
        self.relay = relay
        self.secrets = secrets
        self.backup_backend = backup_backend
        self.app = create_app(relay)


def build_service(config: Optional[Config] = None,
                  *, relay: Optional[Relay] = None) -> Service:  # pragma: no cover - real wiring
    """Production wiring: Config.load() → backend + directory + hold-queue → Relay →
    secrets store → Service. ``relay`` can be injected (tests). Not unit-run because the
    default relay needs a live rpc-server; every part is injectable so tests build their own.
    """
    config = config or Config.load()
    data_dir = os.environ.get("DATA_DIR", "/data")
    secrets_path = os.environ.get("DELTA_SECRETS_PATH", str(Path(data_dir) / "secrets.json"))
    relay = relay or build_default(config)
    secrets = SecretsStore(secrets_path, config.password_min_length)
    backup_backend = backup_mod.DeltaChat2BackupBackend(relay.backend)
    return Service(config, relay, secrets, backup_backend=backup_backend)


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

    loop = asyncio.get_running_loop()
    # 🔴 blocking deltachat event stream → dedicated daemon thread (NEVER on the loop).
    threading.Thread(
        target=_event_pump, args=(service.relay.backend, service.relay, loop),
        daemon=True, name="dcf-event-pump",
    ).start()

    log.info("serving relay on %s:%s and MCP /mcp on %s:%s", host, port, mcp_host, mcp_port)
    await asyncio.gather(
        server.serve(),
        mcp_server.serve(),
        drain_loop(service.relay),
        reconciler_loop(cfg, service.secrets, reconcile_interval, run_on_start, existing_fn,
                        onboard=_make_onboard(service),
                        after_reconcile=lambda: asyncio.to_thread(
                            provision_channels, cfg, service.relay.backend)),
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
