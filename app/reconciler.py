"""0-maintenance roster reconciler (pure logic + provisioning I/O).

Contract (per the chatmail (Dovecot) server contract): a Delta account is provisioned by a successful
IMAP LOGIN (create-on-login via Dovecot passdb) — idempotent. Reconcile = ensure
every desired bot exists, and report which server accounts are no longer desired
(prune is server-side via `doveadm kick`, operator-owned — we only compute the diff).

The pure functions here are unit-tested with no live services.
"""
from __future__ import annotations

import secrets
import string

_ALPHABET = string.ascii_letters + string.digits


def gen_password(length: int = 24, min_length: int = 9) -> str:
    """Random bot password; never shorter than the server's minimum."""
    n = max(length, min_length)
    return "".join(secrets.choice(_ALPHABET) for _ in range(n))


def address_for(localpart: str, domain: str) -> str:
    return f"{localpart}@{domain}"


def valid_username(localpart: str, min_length: int, max_length: int) -> bool:
    return min_length <= len(localpart) <= max_length


def reconcile(desired: list[str], existing: list[str]) -> tuple[list[str], list[str]]:
    """Diff desired-vs-existing account localparts.

    Returns (to_provision, to_prune), both sorted:
      to_provision = desired − existing  (IMAP-login to create-on-login)
      to_prune     = existing − desired  (report only; server-side removal = the operator)
    """
    d, e = set(desired), set(existing)
    return sorted(d - e), sorted(e - d)


async def ensure_account(imap_host: str, imap_port: int, localpart: str, domain: str,
                         password: str, *, _login=None) -> bool:
    """Idempotently ensure a Delta account exists via IMAP create-on-login.

    Returns True on successful auth (account exists/created). ``_login`` is an
    injectable async login callable ``(host, port, user, password) -> bool`` so the
    logic is unit-testable without a live IMAP server.
    """
    user = address_for(localpart, domain)
    login = _login or _imap_login
    return await login(imap_host, imap_port, user, password)


async def _imap_login(host: str, port: int, user: str, password: str) -> bool:
    """Real IMAP login over implicit TLS (:993). Imported lazily so tests need no imaplib TLS."""
    import asyncio
    import imaplib

    def _do() -> bool:
        try:
            c = imaplib.IMAP4_SSL(host, port, timeout=20)
            try:
                c.login(user, password)
                return True
            finally:
                try:
                    c.logout()
                except Exception:
                    pass
        except Exception:
            return False

    return await asyncio.get_event_loop().run_in_executor(None, _do)
