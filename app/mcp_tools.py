"""MCP tools for the Delta Chat Fabric — thin clients over the relay's HTTP contract.

Generic: the relay base URL is injected (env ``RELAY_URL``, default the in-container
loopback). No fleet identity here. Every tool POSTs/GETs a relay contract (see
app.relay for the request/response models):

    delta_send          POST {relay}/send            {bot_id,target,text}
      -> {"status":"sent","msg_id":int,"account_id":int}
    delta_list_contacts GET  {relay}/contacts?bot_id=
      -> {"account_id":int,"contacts":[{id,address,display_name}, ...]}
    delta_list_channels GET  {relay}/channels?bot_id=
      -> {"account_id":int,"channels":[{id,name,members:[localpart,...]}, ...]}
    delta_send_channel  POST {relay}/send_channel     {bot_id,channel_id,text}
      -> {"status":"sent","msg_id":int,"account_id":int,"channel_id":int}
    delta_create_channel POST {relay}/channel         {bot_id,name,members[]}
      -> {"status":"created","channel_id":int,"account_id":int,"name","members"}
    delta_add_member    POST {relay}/channel/member   {bot_id,channel_id,contact}
      -> {"status":"added","channel_id":int,"account_id":int,"contact"}
    delta_react         POST {relay}/react            {bot_id,chat_id,msg_id,emoji}
      -> {"status":"reacted","account_id":int,"chat_id":int,"msg_id":int,"emoji"}

All 4xx/5xx are surfaced as RuntimeError with the relay detail (404 = unknown bot).
The HTTP client is injectable so this is unit-testable with httpx.MockTransport.
"""
from __future__ import annotations

import os
from typing import Optional

import httpx


class _RelayTool:
    """Shared base: injectable relay base-url + httpx client, plus request helpers."""

    def __init__(self, relay_url: Optional[str] = None, client: Optional[httpx.AsyncClient] = None):
        self.relay_url = (relay_url or os.environ.get("RELAY_URL", "http://localhost:8080")).rstrip("/")
        self.client = client or httpx.AsyncClient(timeout=10.0)

    def _check(self, resp: httpx.Response, tool: str) -> dict:
        if resp.status_code >= 400:
            raise RuntimeError(f"{tool} failed ({resp.status_code}): {_detail(resp)}")
        return resp.json()

    async def _post(self, path: str, body: dict, tool: str) -> dict:
        resp = await self.client.post(f"{self.relay_url}{path}", json=body)
        return self._check(resp, tool)

    async def _get(self, path: str, params: dict, tool: str) -> dict:
        resp = await self.client.get(f"{self.relay_url}{path}", params=params)
        return self._check(resp, tool)


class DeltaSendTool(_RelayTool):
    """``delta_send`` — send a message as a bot's Delta account via the relay."""

    name = "delta_send"

    async def send(self, bot_id: str, target: int, text: str) -> dict:
        """Call the relay /send. Returns the relay's JSON on success.

        Raises RuntimeError on a non-2xx (e.g. 404 unknown bot), surfacing the relay detail.
        """
        return await self._post(
            "/send", {"bot_id": bot_id, "target": int(target), "text": text}, self.name
        )


class DeltaListContactsTool(_RelayTool):
    """``delta_list_contacts`` — list a bot account's known contacts."""

    name = "delta_list_contacts"

    async def list_contacts(self, bot_id: str) -> dict:
        return await self._get("/contacts", {"bot_id": bot_id}, self.name)


class DeltaListChannelsTool(_RelayTool):
    """``delta_list_channels`` — list a bot account's group chats (channels)."""

    name = "delta_list_channels"

    async def list_channels(self, bot_id: str) -> dict:
        return await self._get("/channels", {"bot_id": bot_id}, self.name)


class DeltaSendChannelTool(_RelayTool):
    """``delta_send_channel`` — send a message into a group chat (channel)."""

    name = "delta_send_channel"

    async def send_channel(self, bot_id: str, channel_id: int, text: str) -> dict:
        return await self._post(
            "/send_channel",
            {"bot_id": bot_id, "channel_id": int(channel_id), "text": text},
            self.name,
        )


class DeltaCreateChannelTool(_RelayTool):
    """``delta_create_channel`` — create a group chat with caller-supplied members.

    ``members`` come from the caller (generic model): NOTHING about the fleet roster is
    baked in — the tool passes exactly what it's given.
    """

    name = "delta_create_channel"

    async def create_channel(self, bot_id: str, name: str,
                             members: Optional[list[str]] = None) -> dict:
        return await self._post(
            "/channel",
            {"bot_id": bot_id, "name": name, "members": list(members or [])},
            self.name,
        )


class DeltaAddMemberTool(_RelayTool):
    """``delta_add_member`` — add ONE caller-supplied contact to a channel (generic)."""

    name = "delta_add_member"

    async def add_member(self, bot_id: str, channel_id: int, contact: str) -> dict:
        return await self._post(
            "/channel/member",
            {"bot_id": bot_id, "channel_id": int(channel_id), "contact": contact},
            self.name,
        )


class DeltaReactTool(_RelayTool):
    """``delta_react`` — set an emoji reaction on a message."""

    name = "delta_react"

    async def react(self, bot_id: str, chat_id: int, msg_id: int, emoji: str) -> dict:
        return await self._post(
            "/react",
            {"bot_id": bot_id, "chat_id": int(chat_id), "msg_id": int(msg_id), "emoji": emoji},
            self.name,
        )


class DeltaSecureJoinTool(_RelayTool):
    """``delta_secure_join`` — accept a securejoin/verified invite (link or QR) as a bot.

    On success the inviter becomes a verified key-contact of the bot's account (the E2E
    key-exchange), so they can then be added to an encrypted channel. Returns the resulting
    chat id. This is the human-onboarding mechanism (a person shares their Delta invite link).
    """

    name = "delta_secure_join"

    async def secure_join(self, bot_id: str, invite: str) -> dict:
        return await self._post(
            "/secure_join", {"bot_id": bot_id, "invite": invite}, self.name
        )


class DeltaMessagesTool(_RelayTool):
    """``delta_messages`` — read a bot's recent messages in a chat (the read-back / receipt side).

    Returns ``{account_id, chat_id, messages:[{id,text,from_id}, ...]}`` (newest last). Lets a
    caller confirm a message was RECEIVED (e.g. prove a bot-to-bot send round-trips).
    """

    name = "delta_messages"

    async def messages(self, bot_id: str, chat_id: int, limit: int = 20) -> dict:
        return await self._get(
            "/messages", {"bot_id": bot_id, "chat_id": int(chat_id), "limit": int(limit)},
            self.name,
        )


def _detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        return str(body.get("detail", body)) if isinstance(body, dict) else str(body)
    except Exception:
        return resp.text
