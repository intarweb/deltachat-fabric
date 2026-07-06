"""Service entrypoint — wires the generic Delta Chat Fabric into one asyncio process.

Runs THREE things in one process (all gated so a unit test never fires real IMAP/rpc/net):
  1. the relay inbound loop      (relay.run_forever  — event stream → wake routing)
  2. the periodic reconciler     (ensure every roster bot's account via IMAP create-on-login,
                                  minting+storing a per-bot password to a local locket-style
                                  secrets file, then diff vs desired for prune-logging)
  3. uvicorn serving the relay's FastAPI app (the /send + channel/contact/react contract)
  4. the nightly backup loop     (app.backup — deltachat imex export per account)

Generic-engine rule (hard): ZERO fleet identity is baked here. Domain, imap host, roster,
directory URL, ports, dirs, intervals — ALL from ``app.config.Config`` + env. This file
only imports+wires config/reconciler/routing/relay/backup; it adds no fleet-specific logic.

Env contract (all optional-with-defaults except the domain):
  DELTA_MAIL_DOMAIN         (config)  mail domain accounts live under          — REQUIRED
  DELTA_IMAP_HOST/_PORT     (config)  IMAP endpoint (create-on-login)
  DELTA_ROSTER_PATH         (config)  mounted roster YAML          default /config/roster.yaml
  A2A_DIRECTORY_URL         (config)  a2abridge directory for live wake URLs
  DATA_DIR                            LOCAL account-DB + hold-queue dir        default /data
  ACCOUNTS_DIR                        deltachat accounts dir       default $DATA_DIR/accounts
  DELTA_SECRETS_PATH                  local locket-style per-bot password store
                                                                   default $DATA_DIR/secrets.json
  DELTA_BACKUP_DIR                    imex backup dir              default /backup
  DELTA_BACKUP_RETAIN                 backups kept per account     default 7
  DELTA_BACKUP_INTERVAL               backup loop seconds          default 86400
  DELTA_RECONCILE_INTERVAL            reconciler loop seconds      default 3600
  RELAY_HOST / RELAY_PORT             uvicorn bind             default 0.0.0.0 / 8080
  DELTA_RECONCILE_ON_START            "1" to reconcile once at boot    default 1
  DELTA_MCP_HOST / DELTA_MCP_PORT     MCP /mcp bind   default 0.0.0.0 / 8000

The MCP server (app.mcp_server) is served as a SECOND uvicorn in the same asyncio process,
exposing the 7 delta tools over streamable-HTTP at ``/mcp`` for an MCP gateway. It talks to the
relay over loopback HTTP (RELAY_URL), so the relay's internal contract stays unchanged.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Callable, Optional

from . import backup as backup_mod
from . import reconciler
from .config import Config
from .relay import Relay, build_default, create_app


# ---------------------------------------------------------------------------
# Local locket-style secrets store — per-bot Delta password (mode-600 JSON).
# Generic: it's just a keyed file at a path from env; no fleet identity.
# ---------------------------------------------------------------------------


class SecretsStore:
    """Local per-bot password store — a mode-600 JSON at ``DELTA_SECRETS_PATH``.

    ``get_or_create`` mints a random password (via reconciler.gen_password, honoring the
    server's min length) the first time a bot is seen and persists it, so create-on-login
    is idempotent across restarts. This is the local, LOCAL-volume secrets file the spec
    calls for (a real deploy can point it at a locket-mounted path via env)."""

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


def should_reconcile(now: float, last_run: Optional[float], interval: float,
                     run_on_start: bool = True) -> bool:
    """Pure loop-scheduling decision: should the reconciler fire this pass?

    True when never-run-and-run_on_start, or the interval has elapsed since last_run.
    Lets the loop be unit-tested without sleeping / touching IMAP.
    """
    if last_run is None:
        return run_on_start
    return (now - last_run) >= interval


async def reconcile_once(config: Config, secrets: SecretsStore,
                         existing: list[str],
                         *, _login=None) -> dict:
    """One reconcile pass: ensure each desired account via IMAP create-on-login, then diff.

    ``existing`` = server-side localparts (caller supplies; live discovery is the operator/server
    lane — we compute the prune list, never delete). ``_login`` is the injectable IMAP login
    (reconciler.ensure_account's seam) so this is unit-testable with no live IMAP.

    Returns {"provisioned":[...],"failed":[...],"prune":[...]}.
    """
    desired = desired_localparts(config)
    to_provision, to_prune = reconciler.reconcile(desired, existing)
    provisioned: list[str] = []
    failed: list[str] = []
    # (Re)assert every desired account (idempotent create-on-login), not just new ones.
    for lp in desired:
        pw = secrets.get_or_create(lp)
        ok = await reconciler.ensure_account(
            config.imap_host, config.imap_port, lp, config.mail_domain, pw, _login=_login,
        )
        (provisioned if ok else failed).append(lp)
    return {"provisioned": provisioned, "failed": failed, "prune": to_prune,
            "to_provision": to_provision}


async def reconciler_loop(config: Config, secrets: SecretsStore,
                          interval: float, run_on_start: bool,
                          existing_fn: Callable[[], list[str]],
                          *, _should_stop: Optional[Callable[[], bool]] = None,
                          _login=None) -> None:  # pragma: no cover - loop
    """Periodic reconciler. ``existing_fn`` supplies current server-side localparts each
    pass (default in build wiring = the relay backend's account index)."""
    last_run: Optional[float] = None
    while not (_should_stop and _should_stop()):
        now = asyncio.get_event_loop().time()
        if should_reconcile(now, last_run, interval, run_on_start):
            try:
                await reconcile_once(config, secrets, existing_fn(), _login=_login)
            except Exception:
                pass
            last_run = asyncio.get_event_loop().time()
        await asyncio.sleep(min(interval, 60.0))


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


async def _serve(service: Service) -> None:  # pragma: no cover - real uvicorn + loops
    """Start uvicorn + the reconciler loop + the relay inbound loop + the backup loop,
    concurrently, in one asyncio process."""
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

    # Second uvicorn: the MCP server at /mcp (streamable-HTTP). It reaches
    # the relay over loopback HTTP; the relay's own /send contract is untouched.
    from .mcp_server import build_mcp_app

    mcp_host = os.environ.get("DELTA_MCP_HOST", "0.0.0.0")
    mcp_port = int(os.environ.get("DELTA_MCP_PORT", "8000"))
    relay_url = os.environ.get("RELAY_URL", f"http://127.0.0.1:{port}")
    mcp_app = build_mcp_app(relay_url)
    mcp_server = uvicorn.Server(
        uvicorn.Config(mcp_app, host=mcp_host, port=mcp_port, log_level="info")
    )

    def existing_fn() -> list[str]:
        # server-side localparts we know about = the relay backend's account index
        idx = getattr(service.relay.backend, "_localpart_to_accid", {})
        return list(idx.keys())

    await asyncio.gather(
        server.serve(),
        mcp_server.serve(),
        service.relay.run_forever(interval=1.0),
        reconciler_loop(cfg, service.secrets, reconcile_interval, run_on_start, existing_fn),
        backup_mod.run_forever(cfg, service.backup_backend, backup_dir,
                               retain=backup_retain, interval=backup_interval),
    )


def main() -> None:  # pragma: no cover - process entry
    service = build_service()
    asyncio.run(_serve(service))


if __name__ == "__main__":  # pragma: no cover
    main()
