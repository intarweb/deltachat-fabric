"""0-maintenance roster reconciler — PURE logic (diff + password minting + validation).

Contract (per the chatmail (Dovecot) server model): a Delta account is provisioned by a
successful IMAP LOGIN (create-on-login via Dovecot passdb) — idempotent. The actual
onboarding (add_account + add_or_update_transport, which performs that login AND configures
the deltachat core) lives in the backend (``relay.DeltaChat2Backend.ensure_account``); this
module holds only the pure, unit-testable pieces: the desired-vs-existing diff, per-bot
password generation, and username validation. Prune is server-side (operator-owned) — we
only compute the diff.

The functions here are pure and unit-tested with no live services.
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
      to_provision = desired − existing  (onboard into the core → create-on-login)
      to_prune     = existing − desired  (report only; server-side removal = the operator)
    """
    d, e = set(desired), set(existing)
    return sorted(d - e), sorted(e - d)
