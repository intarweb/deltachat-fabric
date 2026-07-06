"""op-connect password store — the SINGLE durable source for per-bot mail credentials.

Per the fleet secrets contract (auth/secrets owner): each bot's fixed IMAP/SMTP password lives
as one CONCEALED field (keyed by bot id) on the op-connect item ``deltachat-bot-creds``.
Three consumers share it: the reconciler mints-if-absent, the Authentik sync ``set_password``s
from it, and the relay reads it for ``add_or_update_transport`` — so the credential Authentik
provisions == what the fabric configures == what Stalwart LDAP-binds. Single source, no
minting divergence, DR-covered + auditable/rotatable by the secrets owner.

🔴 1Password is ONLY reached via op-connect (never the ``op`` CLI). Config from env
(injected at deploy — nothing baked):
  OP_CONNECT_URL      op-connect base, e.g. http://op-connect.example:8080
  OP_CONNECT_TOKEN    bearer (the fleet ~/.op-connect-token, mounted)
  OP_CONNECT_VAULT    vault id holding the item
  DELTA_BOT_CREDS_ITEM  item id (or title) of ``deltachat-bot-creds``

Every HTTP call goes through an injectable ``httpx.Client`` so this is unit-tested with
``httpx.MockTransport`` — no live op-connect.
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


class OpConnectStore:
    """Per-bot password store on the op-connect item ``deltachat-bot-creds``.

    ``get_or_create(bot)`` returns the bot's stored password, minting + persisting one (a new
    CONCEALED field) only if absent — so a password rotated in op-connect by the secrets owner
    is HONORED, never overwritten. Fields are keyed by bot id.
    """

    def __init__(self, *, base_url: Optional[str] = None, token: Optional[str] = None,
                 vault: Optional[str] = None, item: Optional[str] = None,
                 client: Optional[httpx.Client] = None, password_min_length: int = 9):
        self.base_url = (base_url or os.environ.get("OP_CONNECT_URL", "")).rstrip("/")
        self.token = token or os.environ.get("OP_CONNECT_TOKEN", "")
        self.vault = vault or os.environ.get("OP_CONNECT_VAULT", "")
        self.item = item or os.environ.get("DELTA_BOT_CREDS_ITEM", "")
        self.password_min_length = password_min_length
        self._client = client or httpx.Client(
            timeout=15.0, headers={"Authorization": f"Bearer {self.token}"}
        )

    def _item_url(self) -> str:
        return f"{self.base_url}/v1/vaults/{self.vault}/items/{self.item}"

    def _get_item(self) -> dict:
        r = self._client.get(self._item_url())
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _field_value(item: dict, bot: str) -> Optional[str]:
        for f in item.get("fields", []) or []:
            if f.get("label") == bot:
                return f.get("value")
        return None

    def get(self, bot: str) -> Optional[str]:
        """Return the stored password for ``bot``, or None if the field is absent."""
        return self._field_value(self._get_item(), bot)

    def get_or_create(self, bot: str) -> str:
        """Return ``bot``'s password, minting + persisting a CONCEALED field if absent.

        Read-then-mint-if-absent against the shared item, so a rotated value is preserved.
        """
        item = self._get_item()
        existing = self._field_value(item, bot)
        if existing:
            return existing
        pw = gen_password(24, self.password_min_length)
        fields = list(item.get("fields", []) or [])
        fields.append({"label": bot, "type": "CONCEALED", "value": pw})
        # PATCH-add the new field (op-connect merges by the fields array on the item).
        r = self._client.patch(self._item_url(), json=[
            {"op": "add", "path": "/fields", "value": {"label": bot, "type": "CONCEALED", "value": pw}}
        ])
        if r.status_code >= 400:
            # fall back to a full PUT of the fields array if PATCH isn't supported
            item["fields"] = fields
            r = self._client.put(self._item_url(), json=item)
            r.raise_for_status()
        return pw
