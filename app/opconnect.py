"""op-connect access — the SINGLE durable source for per-bot mail credentials + the
Authentik sync token (per the fleet secrets contract; the secrets owner owns the items).

Each bot's fixed IMAP/SMTP password lives as one CONCEALED field (label = bot id) on the
op-connect item ``deltachat-bot-creds``. Three consumers share it: the reconciler
mints-if-absent, the Authentik sync ``set_password``s from it, the relay reads it for
``add_or_update_transport`` — so the credential Authentik provisions == what the fabric
configures == what the mail server LDAP-binds. Single source, DR-covered, rotatable.

🔴 1Password is ONLY reached via op-connect (never the ``op`` CLI). Items are fetched by
TITLE (list-filter → id → full item), and writes use PUT read-modify-write (GET → append to
``fields[]`` → PUT the whole item) — the universally-supported, safe write. Config from env
(injected at deploy — nothing baked):
  OP_CONNECT_URL      op-connect base, e.g. http://op-connect.example:8080
  OP_CONNECT_TOKEN    bearer to op-connect (the mounted fleet connect token)
  OP_CONNECT_VAULT    vault id holding the items
  DELTA_BOT_CREDS_ITEM        title of the per-bot creds item
  DELTA_AUTHENTIK_TOKEN_ITEM  title of the Authentik-sync-token item

Every HTTP call goes through an injectable ``httpx.Client`` → unit-tested with MockTransport.
"""
from __future__ import annotations

import os
import secrets as _secrets
import string
from typing import Optional

import httpx

_ALPHABET = string.ascii_letters + string.digits


def gen_password(length: int = 24, min_length: int = 9) -> str:
    """CSPRNG bot password, never shorter than the server minimum."""
    n = max(length, min_length)
    return "".join(_secrets.choice(_ALPHABET) for _ in range(n))


def _client(token: str, client: Optional[httpx.Client]) -> httpx.Client:
    return client or httpx.Client(timeout=15.0, headers={"Authorization": f"Bearer {token}"})


def resolve_item(client: httpx.Client, base_url: str, vault: str, title: str) -> dict:
    """Fetch a full op-connect item BY TITLE: list-filter → id → full item (with fields)."""
    base = base_url.rstrip("/")
    r = client.get(f"{base}/v1/vaults/{vault}/items",
                   params={"filter": f'title eq "{title}"'})
    r.raise_for_status()
    items = r.json() or []
    if not items:
        raise KeyError(f"op-connect item not found by title: {title}")
    item_id = items[0]["id"]
    r = client.get(f"{base}/v1/vaults/{vault}/items/{item_id}")
    r.raise_for_status()
    return r.json()


def item_fields(item: dict) -> dict:
    """{label: value} for every field on an op-connect item."""
    return {f.get("label"): f.get("value") for f in (item.get("fields") or [])}


def read_authentik_creds(base_url: str, token: str, vault: str, title: str,
                         *, client: Optional[httpx.Client] = None) -> tuple[str, str]:
    """Read (authentik_url, credential) from the Authentik-sync-token op-connect item —
    so the Authentik API token never touches env/compose in plaintext."""
    item = resolve_item(_client(token, client), base_url, vault, title)
    f = item_fields(item)
    return f["authentik_url"], f["credential"]


class OpConnectStore:
    """Per-bot password store on the op-connect item ``DELTA_BOT_CREDS_ITEM`` (by title).

    ``get_or_create(bot)`` returns the bot's stored password, minting + persisting a new
    CONCEALED field (via PUT read-modify-write) only if absent — so a password rotated in
    op-connect by the secrets owner is HONORED, never overwritten. Fields keyed by bot id.
    """

    def __init__(self, *, base_url: Optional[str] = None, token: Optional[str] = None,
                 vault: Optional[str] = None, item: Optional[str] = None,
                 client: Optional[httpx.Client] = None, password_min_length: int = 9):
        self.base_url = (base_url or os.environ.get("OP_CONNECT_URL", "")).rstrip("/")
        self.token = token or os.environ.get("OP_CONNECT_TOKEN", "")
        self.vault = vault or os.environ.get("OP_CONNECT_VAULT", "")
        self.item_title = item or os.environ.get("DELTA_BOT_CREDS_ITEM", "")
        self.password_min_length = password_min_length
        self._client = _client(self.token, client)

    def get(self, bot: str) -> Optional[str]:
        """Return the stored password for ``bot``, or None if the field is absent."""
        return item_fields(resolve_item(self._client, self.base_url, self.vault,
                                        self.item_title)).get(bot)

    def get_or_create(self, bot: str) -> str:
        """Return ``bot``'s password, minting + persisting (PUT read-modify-write) if absent."""
        item = resolve_item(self._client, self.base_url, self.vault, self.item_title)
        for f in (item.get("fields") or []):
            if f.get("label") == bot and f.get("value"):
                return f["value"]                      # existing (incl. rotated) → honored
        pw = gen_password(24, self.password_min_length)
        item.setdefault("fields", []).append(
            {"label": bot, "type": "CONCEALED", "value": pw})
        r = self._client.put(
            f"{self.base_url}/v1/vaults/{self.vault}/items/{item['id']}", json=item)
        r.raise_for_status()
        return pw
