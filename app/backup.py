"""Nightly account backup (step 6) — deltachat ``imex`` export per account.

Uses the deltachat JSON-RPC ``export_backup(accid, folder, passphrase)`` (the imex
backup export — a portable ``.tar`` written into a folder), NOT a raw SQLCipher copy.
That real RPC call is isolated behind an injectable backend (same seam pattern relay.py
uses for its DeltaBackend) so this module is unit-testable with no live rpc-server.

Generic-engine rule: ZERO fleet identity. Which accounts, where, and retention all come
from ``app.config.Config`` + env (``DELTA_BACKUP_DIR``, default ``/backup``).

The pure schedule/rotation helpers (``accounts_to_backup`` / ``prune_old_backups``) carry
all the decision logic and are fully unit-tested. The one line that talks to the real
deltachat core is behind ``BackupBackend`` + ``# pragma: no cover``.
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

from .config import Config

# A backup filename we own: "<localpart>-<YYYYmmddTHHMMSSZ>.tar" — lets us group + sort
# per account for retention without trusting mtimes.
_STAMP = "%Y%m%dT%H%M%SZ"
_NAME_RE = re.compile(r"^(?P<localpart>.+)-(?P<stamp>\d{8}T\d{6}Z)\.(?P<ext>tar|bak)$")


class BackupBackend(Protocol):
    """Thin injectable seam over the deltachat imex export. The ONLY thing that touches
    the live rpc-server; a fake makes the scheduler fully unit-testable."""

    def account_id_for(self, localpart: str) -> Optional[int]:
        """Delta account id for a bot localpart (None if that bot has no account)."""
        ...

    def export_backup(self, account_id: int, folder: str) -> None:
        """Export ``account_id`` (deltachat imex) into ``folder``. Folder must exist."""
        ...


class DeltaChat2BackupBackend:
    """Real backend: reuses a relay ``DeltaChat2Backend``'s account index + its live rpc.

    ``export_backup`` calls the verified deltachat JSON-RPC ``rpc.export_backup(accid,
    folder, passphrase)`` (imex backup export; passphrase None = unencrypted portable
    tar). Isolated here behind ``# pragma: no cover`` so an API drift is a one-line fix.
    """

    def __init__(self, relay_backend: Any):
        self._backend = relay_backend

    def account_id_for(self, localpart: str) -> Optional[int]:
        return self._backend.account_id_for(localpart)

    def export_backup(self, account_id: int, folder: str) -> None:  # pragma: no cover - real rpc
        # Verified via context7 /deltachat-bot/deltabot-cli-py:
        #   rpc.export_backup(accid: int, folder: str, passphrase: str | None) -> None
        Path(folder).mkdir(parents=True, exist_ok=True)
        self._backend.rpc.export_backup(account_id, folder, None)


def _timestamp(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).strftime(_STAMP)


def accounts_to_backup(config: Config, backend: BackupBackend) -> list[str]:
    """Which roster localparts actually have a live account → are eligible for backup.

    Pure decision logic (no I/O beyond the injected backend's index lookup): a roster bot
    is backed up iff the backend can resolve it to an account id. Order-preserving,
    de-duped by localpart. Knows no bot names — everything comes from ``config.roster``.
    """
    seen: set[str] = set()
    out: list[str] = []
    for spec in config.roster:
        lp = spec.localpart
        if lp in seen:
            continue
        if backend.account_id_for(lp) is not None:
            seen.add(lp)
            out.append(lp)
    return out


def prune_old_backups(files: list[str], retain: int) -> list[str]:
    """Given backup file paths, decide which to DELETE to keep only ``retain`` newest
    PER account. Pure: takes a list of paths, returns the subset to remove.

    Files are grouped by the ``<localpart>`` in their name; within each group the newest
    ``retain`` (by embedded timestamp, lexicographically sortable) are kept and the rest
    returned for deletion. ``retain <= 0`` means "keep none" (delete all recognized).
    Unrecognized names are never touched.
    """
    by_bot: dict[str, list[tuple[str, str]]] = {}
    for f in files:
        m = _NAME_RE.match(Path(f).name)
        if not m:
            continue  # not ours — leave it alone
        by_bot.setdefault(m.group("localpart"), []).append((m.group("stamp"), f))
    to_delete: list[str] = []
    for _bot, entries in by_bot.items():
        entries.sort(key=lambda e: e[0], reverse=True)  # newest stamp first
        keep = max(retain, 0)
        to_delete.extend(f for _stamp, f in entries[keep:])
    return sorted(to_delete)


def list_backup_files(backup_dir: str) -> list[str]:
    """List the backup files we own (matching our naming) under ``backup_dir``."""
    p = Path(backup_dir)
    if not p.exists():
        return []
    return [str(f) for f in p.iterdir() if f.is_file() and _NAME_RE.match(f.name)]


def run_backup(config: Config, backend: BackupBackend, backup_dir: str,
               retain: int = 7, *, now: Optional[datetime] = None) -> dict:
    """Back up every eligible account, then prune to ``retain`` newest per account.

    Each account is exported (imex) into a per-run temp folder, then the produced file(s)
    are renamed to our ``<localpart>-<stamp>.<ext>`` scheme so ``prune_old_backups`` can
    group them. Returns a summary {"exported":[...],"pruned":[...],"errors":{...}}.

    The only non-pure step is ``backend.export_backup`` (the real deltachat call, itself
    behind ``# pragma: no cover`` in the real backend). All scheduling/rotation is pure.
    """
    Path(backup_dir).mkdir(parents=True, exist_ok=True)
    stamp = _timestamp(now)
    exported: list[str] = []
    errors: dict[str, str] = {}
    for lp in accounts_to_backup(config, backend):
        accid = backend.account_id_for(lp)
        assert accid is not None  # accounts_to_backup already filtered
        staging = Path(backup_dir) / f".staging-{lp}-{stamp}"
        try:
            staging.mkdir(parents=True, exist_ok=True)
            backend.export_backup(accid, str(staging))
            produced = sorted(p for p in staging.iterdir() if p.is_file())
            if not produced:
                errors[lp] = "export produced no file"
                continue
            # deltachat imex writes one backup tar; take the first, name it ours.
            dest = Path(backup_dir) / f"{lp}-{stamp}.tar"
            produced[0].replace(dest)
            exported.append(str(dest))
        except Exception as e:  # pragma: no cover - defensive; real export failure
            errors[lp] = str(e)
        finally:
            _cleanup_dir(staging)

    to_delete = prune_old_backups(list_backup_files(backup_dir), retain)
    pruned: list[str] = []
    for f in to_delete:
        try:
            os.remove(f)
            pruned.append(f)
        except OSError:  # pragma: no cover - defensive
            pass
    return {"exported": exported, "pruned": pruned, "errors": errors}


def _cleanup_dir(d: Path) -> None:
    """Best-effort remove a staging dir + its contents (no external deps)."""
    if not d.exists():
        return
    for child in d.iterdir():  # pragma: no cover - staging normally emptied by run_backup
        try:
            child.unlink()
        except OSError:
            pass
    try:
        d.rmdir()
    except OSError:  # pragma: no cover - defensive
        pass


def next_backup_delay(interval: float, last_run: Optional[float], now: float) -> float:
    """Seconds to sleep before the next backup, given the interval + last-run epoch.

    Pure scheduling helper (unit-tested): if never run, or the interval has already
    elapsed, returns 0 (run now); otherwise the remaining time. Never negative.
    """
    if last_run is None:
        return 0.0
    elapsed = now - last_run
    return max(0.0, interval - elapsed)


async def run_forever(config: Config, backend: BackupBackend, backup_dir: str,
                      retain: int = 7, interval: float = 86400.0,
                      _should_stop: Optional[Any] = None) -> None:  # pragma: no cover - loop
    """Nightly backup loop. Runs a backup, then sleeps ``interval`` until stopped.

    ``_should_stop`` (callable) lets a test drive one pass; the real service passes None.
    """
    import asyncio

    last_run: Optional[float] = None
    while not (_should_stop and _should_stop()):
        delay = next_backup_delay(interval, last_run, time.time())
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            run_backup(config, backend, backup_dir, retain)
        except Exception:
            pass
        last_run = time.time()
        await asyncio.sleep(interval)
