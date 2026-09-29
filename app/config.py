"""Generic runtime config — injected at DEPLOY, never baked into the image.

Per the generic-engine rule: NO roster, bot names, mesh addresses, or fleet config
live in this repo. Everything is read from env / a mounted roster file at runtime.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class BotSpec:
    """One bot's Delta identity + realm — parsed from the injected roster."""
    id: str                       # fleet bot id (e.g. "bot-a") — injected, not baked
    realm: str = "default"        # realm/channel grouping (e.g. "realm-a", "realm-b")
    localpart: str = ""           # delta account localpart; defaults to id if unset
    display_name: str = ""        # Delta display name shown to humans; defaults to Capitalized id

    def __post_init__(self):
        if not self.localpart:
            self.localpart = self.id


@dataclass
class Config:
    mail_domain: str                          # e.g. deltachat.example.net (injected)
    imap_host: str
    imap_port: int = 993
    submission_host: str = ""
    submission_port: int = 587
    a2a_directory_url: str = ""               # a2abridge /agents directory (injected)
    password_min_length: int = 9
    username_min_length: int = 1
    username_max_length: int = 64
    roster: list[BotSpec] = field(default_factory=list)
    realm_leads: dict[str, str] = field(default_factory=dict)  # realm -> "main" bot id
    # Per-bot wake transport for bots NOT reachable via the a2a directory, keyed by bot id —
    # the roster's top-level ``wakes:`` map (a separate key, so a roster carrying it still loads
    # in older images that only read ``bots``/``realm_leads``). Shape:
    #   wakes:
    #     <bot id>: {kind: hermes-runs, url: "http://host:port", key_env: SOME_ENV_VAR}
    # ``kind: hermes-runs`` = Hermes Agent's documented programmatic input, ``POST /v1/runs``
    # (Bearer = the API server key, read from env var ``key_env`` — never from the roster).
    wakes: dict[str, dict] = field(default_factory=dict)

    @classmethod
    def load(cls, roster_path: str | None = None) -> "Config":
        """Build config from env + a mounted roster YAML. Nothing fleet-specific baked."""
        roster_path = roster_path or os.environ.get("DELTA_ROSTER_PATH", "/config/roster.yaml")
        domain = os.environ.get("DELTA_MAIL_DOMAIN", "")
        imap_host = os.environ.get("DELTA_IMAP_HOST", domain)
        roster, leads, wakes = [], {}, {}
        p = Path(roster_path)
        if p.exists():
            data = yaml.safe_load(p.read_text()) or {}
            roster = [BotSpec(**b) if isinstance(b, dict) else BotSpec(id=b)
                      for b in data.get("bots", [])]
            leads = data.get("realm_leads", {}) or {}
            wakes = {str(k): dict(v) for k, v in (data.get("wakes") or {}).items()}
        return cls(
            mail_domain=domain,
            imap_host=imap_host,
            imap_port=int(os.environ.get("DELTA_IMAP_PORT", "993")),
            submission_host=os.environ.get("DELTA_SUBMISSION_HOST", imap_host),
            submission_port=int(os.environ.get("DELTA_SUBMISSION_PORT", "587")),
            a2a_directory_url=os.environ.get("A2A_DIRECTORY_URL", ""),
            password_min_length=int(os.environ.get("DELTA_PASSWORD_MIN_LENGTH", "9")),
            username_min_length=int(os.environ.get("DELTA_USERNAME_MIN_LENGTH", "1")),
            username_max_length=int(os.environ.get("DELTA_USERNAME_MAX_LENGTH", "64")),
            roster=roster,
            realm_leads=leads,
            wakes=wakes,
        )

    @staticmethod
    def _capitalize(s: str) -> str:
        """Capitalize the first character only (preserve the rest): 'robot' -> 'Robot',
        'mcBot' -> 'McBot'. Generic — no fleet names baked in."""
        return s[:1].upper() + s[1:] if s else s

    def display_name_for(self, localpart: str) -> str:
        """Delta display name a human should see for a bot account (e.g. 'Robot', not the
        raw email). Uses the roster's explicit ``display_name`` if set, else the Capitalized bot
        id. Generic: falls back to the Capitalized localpart for an unknown account."""
        for b in self.roster:
            if localpart in (b.localpart, b.id):
                return b.display_name or self._capitalize(b.id)
        return self._capitalize(localpart)
