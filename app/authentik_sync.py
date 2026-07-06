"""roster → Authentik service-account sync (Vikunja proj-23; replaces authentik-user-manager).

authentik-user-manager was verified as a HUMAN-invite tool (can't create service accounts,
set passwords, or set attributes) — so this is the direct Authentik-API sync the auth owner confirmed:
each roster bot becomes an Authentik ``type=service_account`` with a fixed password (from the
shared op-connect store) + its email, tagged ``is_bot:true`` + ``realm`` and placed in the
``Bots`` group. Idempotent (find→create-or-reuse→set_password→set email+attrs→add_to_group),
so it runs as a periodic ofelia cron: roster change → next tick → Authentik → LDAP → mail server.

Endpoints verified against the Authentik OpenAPI + source (goauthentik.io, /api/v3):
  find    GET  /core/users/?username=<u>            -> {results:[{pk}]}
  create  POST /core/users/service_account/ {name}  -> {user_pk}
  passwd  POST /core/users/{pk}/set_password/ {password}   -> 204
  update  PATCH /core/users/{pk}/ {email, attributes:{...}}  (attributes replaced wholesale)
  group   POST /core/groups/{uuid}/add_user/ {pk}   (idempotent, additive)

The fixed set_password (NOT the auto-minted app-token) is the LDAP bind credential; the email
is REQUIRED (the mail server binds/looks up by ``mail=<bot>@domain``). Config from env
(injected at deploy); the Authentik URL + token are READ FROM op-connect (never plaintext env):
  OP_CONNECT_URL / OP_CONNECT_TOKEN / OP_CONNECT_VAULT   (op-connect access)
  DELTA_AUTHENTIK_TOKEN_ITEM  op-connect item holding {authentik_url, credential}
  DELTA_BOT_CREDS_ITEM        op-connect item holding per-bot passwords
  BOTS_GROUP_UUID             the Bots group uuid
  DELTA_ROSTER_PATH / DELTA_MAIL_DOMAIN  (via app.config.Config)
Every HTTP call goes through an injectable ``httpx.Client`` → unit-tested with MockTransport.
"""
from __future__ import annotations

import logging
import os
from typing import Optional, Protocol

import httpx

from .config import Config

log = logging.getLogger("dcf.authentik_sync")

SERVICE_ACCOUNT_TYPE = "service_account"


class PasswordStore(Protocol):
    """Source of each bot's fixed password (op-connect-backed in prod; fake in tests)."""

    def get_or_create(self, bot: str) -> str: ...


class AuthentikClient:
    """Thin Authentik REST client (the verified endpoints). Injectable http client for tests."""

    def __init__(self, base_url: str, token: str, *, client: Optional[httpx.Client] = None):
        self.api = base_url.rstrip("/") + "/api/v3"
        self._client = client or httpx.Client(
            timeout=15.0, headers={"Authorization": f"Bearer {token}"}
        )

    def find_user_pk(self, username: str) -> Optional[int]:
        r = self._client.get(f"{self.api}/core/users/", params={"username": username})
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0]["pk"] if results else None

    def create_service_account(self, name: str) -> int:
        r = self._client.post(f"{self.api}/core/users/service_account/", json={"name": name})
        r.raise_for_status()
        return r.json()["user_pk"]

    def set_password(self, pk: int, password: str) -> None:
        r = self._client.post(f"{self.api}/core/users/{pk}/set_password/",
                              json={"password": password})
        r.raise_for_status()

    def update_user(self, pk: int, body: dict) -> None:
        # PATCH the user — used for email + attributes. NB PATCH replaces the whole
        # attributes dict, so callers pass the full desired attribute set.
        r = self._client.patch(f"{self.api}/core/users/{pk}/", json=body)
        r.raise_for_status()

    def add_to_group(self, group_uuid: str, pk: int) -> None:
        # Idempotent, additive — re-adding an existing member is a no-op server-side.
        r = self._client.post(f"{self.api}/core/groups/{group_uuid}/add_user/", json={"pk": pk})
        r.raise_for_status()


def sync_bot(client: AuthentikClient, store: PasswordStore, username: str, email: str,
             realm: str, bots_group_uuid: str) -> dict:
    """Idempotently ensure one bot = service_account with pw + email + attrs + Bots group.

    🔴 email is REQUIRED: the mail server LDAP-binds/looks up the bot by ``mail=<email>``, so a
    service account with no email can't log in. Set it alongside the ``{is_bot,realm}`` attrs.
    """
    pk = client.find_user_pk(username)
    created = pk is None
    if pk is None:
        pk = client.create_service_account(username)
    client.set_password(pk, store.get_or_create(username))
    client.update_user(pk, {"email": email, "attributes": {"is_bot": True, "realm": realm}})
    client.add_to_group(bots_group_uuid, pk)
    return {"bot": username, "pk": pk, "created": created}


def sync_roster(config: Config, store: PasswordStore, client: AuthentikClient,
                bots_group_uuid: str) -> list[dict]:
    """Sync every roster bot into Authentik. Per-bot errors are caught + reported (one bad
    bot never aborts the pass — same resilience as the reconciler)."""
    results: list[dict] = []
    seen: set[str] = set()
    for spec in config.roster:
        if spec.localpart in seen:
            continue
        seen.add(spec.localpart)
        email = f"{spec.localpart}@{config.mail_domain}"
        try:
            results.append(sync_bot(client, store, spec.localpart, email, spec.realm,
                                    bots_group_uuid))
        except Exception:
            log.exception("authentik sync failed for %s", spec.localpart)
            results.append({"bot": spec.localpart, "error": True})
    created = sum(1 for r in results if r.get("created"))
    failed = [r["bot"] for r in results if r.get("error")]
    log.info("authentik sync: %d bots (%d created)%s", len(results), created,
             f", {len(failed)} FAILED: {failed}" if failed else "")
    return results


def main() -> None:  # pragma: no cover - process entry (real op-connect + Authentik)
    logging.basicConfig(level=os.environ.get("DELTA_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from .opconnect import OpConnectStore, read_authentik_creds

    config = Config.load()
    # Authentik url + token come FROM op-connect (never plaintext env/compose):
    op_url = os.environ["OP_CONNECT_URL"]
    op_token = os.environ["OP_CONNECT_TOKEN"]
    vault = os.environ["OP_CONNECT_VAULT"]
    token_item = os.environ.get("DELTA_AUTHENTIK_TOKEN_ITEM", "deltachat-authentik-sync-token")
    authentik_url, authentik_token = read_authentik_creds(op_url, op_token, vault, token_item)
    bots_group_uuid = os.environ["BOTS_GROUP_UUID"]

    client = AuthentikClient(authentik_url, authentik_token)
    store = OpConnectStore(password_min_length=config.password_min_length)
    log.info("authentik sync starting: %d roster bots -> %s", len(config.roster), authentik_url)
    sync_roster(config, store, client, bots_group_uuid)


if __name__ == "__main__":  # pragma: no cover
    main()
