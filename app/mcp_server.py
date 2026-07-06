"""MCP server for the Delta Chat Fabric.

Exposes the fabric's 7 delta operations as a **streamable-HTTP MCP server at ``/mcp``** so
an MCP gateway (or any MCP client) can connect and auto-discover the tools via ``tools/list``. Each tool is a thin
wrapper that delegates to the relay's HTTP contract by REUSING the client classes in
``app.mcp_tools`` (constructed with the injected ``relay_url``) — so the relay stays the
single source of truth and this module adds no new relay logic.

Generic-engine rule (hard): ZERO fleet identity. Nothing about a bot roster, domain, or
mesh is baked here — ``relay_url`` is injected and ``bot_id`` / targets are caller args
that flow straight through to the relay contract. The tool docstrings + type hints ARE the
schema an MCP client's ``tools/list`` discovers.

Transport / endpoint (what an MCP client connects to):
  - path      ``/mcp``   (FastMCP's default ``streamable_http_path``)
  - transport ``streamable-http`` (stateless_http=True — no per-session server state)
  - auth      NONE at this endpoint — access is Virtual-Key-gated upstream at the MCP gateway.

Usage:
  ``build_mcp(relay_url)`` -> a configured ``FastMCP`` with the 7 tools registered.
  ``build_mcp_app(relay_url)`` -> the Starlette ASGI app (``streamable_http_app()``), which
  ALSO wires ``session_manager.run()` into its own lifespan — so it can be served directly
  by uvicorn with no extra lifespan plumbing. Mount it into another ASGI app via
  ``Mount("/mcp", app=...)`` only if you additionally run ``mcp.session_manager.run()`` in
  the host app's lifespan (see FastMCP docs); serving it standalone is simpler and is what
  ``app.main`` does.

Standalone entrypoint (handy for a dedicated container / Dockerfile):
  ``python -m app.mcp_server`` -> ``mcp.run(transport="streamable-http")`` bound to
  ``DELTA_MCP_HOST``/``DELTA_MCP_PORT``.
"""
from __future__ import annotations

import os
from typing import Optional

from mcp.server.fastmcp import FastMCP

from .mcp_tools import (
    DeltaAddMemberTool,
    DeltaCreateChannelTool,
    DeltaListChannelsTool,
    DeltaListContactsTool,
    DeltaReactTool,
    DeltaSendChannelTool,
    DeltaSendTool,
)

# The 7 tool names exactly as they appear in an MCP client's tools/list.
TOOL_NAMES = (
    "delta_send",
    "delta_list_contacts",
    "delta_list_channels",
    "delta_send_channel",
    "delta_create_channel",
    "delta_add_member",
    "delta_react",
)


def build_mcp(relay_url: Optional[str] = None) -> FastMCP:
    """Build the ``FastMCP`` with the 7 delta tools registered.

    ``relay_url`` is injected into every underlying relay client (falls back to the
    ``RELAY_URL`` env / in-container loopback via ``_RelayTool``). The server is
    ``stateless_http`` so each request is self-contained (no sticky session), which is what
    an MCP gateway's connect-and-list-tools flow wants.
    """
    mcp = FastMCP("deltachat", stateless_http=True)

    # One relay client per operation, all pointed at the same injected relay_url.
    send_tool = DeltaSendTool(relay_url=relay_url)
    contacts_tool = DeltaListContactsTool(relay_url=relay_url)
    channels_tool = DeltaListChannelsTool(relay_url=relay_url)
    send_channel_tool = DeltaSendChannelTool(relay_url=relay_url)
    create_channel_tool = DeltaCreateChannelTool(relay_url=relay_url)
    add_member_tool = DeltaAddMemberTool(relay_url=relay_url)
    react_tool = DeltaReactTool(relay_url=relay_url)

    @mcp.tool(name="delta_send")
    async def delta_send(bot_id: str, target: int, text: str) -> dict:
        """Send a direct message as a bot's Delta Chat account.

        Args:
            bot_id: The bot/account localpart to send AS.
            target: The Delta chat id (a 1:1 or group chat) or contact id to send TO.
            text: The message body.

        Returns the relay result, e.g. ``{"status":"sent","msg_id":int,"account_id":int}``.
        """
        return await send_tool.send(bot_id=bot_id, target=target, text=text)

    @mcp.tool(name="delta_list_contacts")
    async def delta_list_contacts(bot_id: str) -> dict:
        """List the contacts known to a bot's Delta Chat account.

        Args:
            bot_id: The bot/account localpart whose contacts to list.

        Returns ``{"account_id":int,"contacts":[{id,address,display_name}, ...]}``.
        """
        return await contacts_tool.list_contacts(bot_id=bot_id)

    @mcp.tool(name="delta_list_channels")
    async def delta_list_channels(bot_id: str) -> dict:
        """List the group chats (channels) a bot's Delta Chat account belongs to.

        Args:
            bot_id: The bot/account localpart whose channels to list.

        Returns ``{"account_id":int,"channels":[{id,name,members:[localpart,...]}, ...]}``.
        """
        return await channels_tool.list_channels(bot_id=bot_id)

    @mcp.tool(name="delta_send_channel")
    async def delta_send_channel(bot_id: str, channel_id: int, text: str) -> dict:
        """Send a message into an existing group chat (channel).

        Args:
            bot_id: The bot/account localpart to send AS.
            channel_id: The Delta group-chat id to post into.
            text: The message body.

        Returns ``{"status":"sent","msg_id":int,"account_id":int,"channel_id":int}``.
        """
        return await send_channel_tool.send_channel(
            bot_id=bot_id, channel_id=channel_id, text=text
        )

    @mcp.tool(name="delta_create_channel")
    async def delta_create_channel(
        bot_id: str, name: str, members: Optional[list[str]] = None
    ) -> dict:
        """Create a new group chat (channel) with caller-supplied members.

        Args:
            bot_id: The bot/account localpart that creates and owns the channel.
            name: The display name for the new group chat.
            members: Email addresses to add as members (default: none). Supplied entirely
                by the caller — nothing is baked in.

        Returns ``{"status":"created","channel_id":int,"account_id":int,"name","members"}``.
        """
        return await create_channel_tool.create_channel(
            bot_id=bot_id, name=name, members=members
        )

    @mcp.tool(name="delta_add_member")
    async def delta_add_member(bot_id: str, channel_id: int, contact: str) -> dict:
        """Add one contact to an existing group chat (channel).

        Args:
            bot_id: The bot/account localpart performing the add.
            channel_id: The Delta group-chat id to add the member to.
            contact: The contact's email address to add (caller-supplied).

        Returns ``{"status":"added","channel_id":int,"account_id":int,"contact"}``.
        """
        return await add_member_tool.add_member(
            bot_id=bot_id, channel_id=channel_id, contact=contact
        )

    @mcp.tool(name="delta_react")
    async def delta_react(bot_id: str, chat_id: int, msg_id: int, emoji: str) -> dict:
        """Set an emoji reaction on a message.

        Args:
            bot_id: The bot/account localpart reacting.
            chat_id: The Delta chat id the message lives in.
            msg_id: The message id to react to.
            emoji: The reaction emoji (e.g. "\U0001f44d").

        Returns ``{"status":"reacted","account_id":int,"chat_id":int,"msg_id":int,"emoji"}``.
        """
        return await react_tool.react(
            bot_id=bot_id, chat_id=chat_id, msg_id=msg_id, emoji=emoji
        )

    return mcp


def build_mcp_app(relay_url: Optional[str] = None):
    """Return the streamable-HTTP Starlette ASGI app (serves ``/mcp``).

    The returned app already wires ``session_manager.run()`` into its own lifespan, so it is
    safe to hand straight to uvicorn (no extra lifespan plumbing needed).
    """
    return build_mcp(relay_url).streamable_http_app()


def main() -> None:  # pragma: no cover - process entry
    """Standalone launcher: serve the MCP server over streamable-HTTP.

    Bind from env (generic): ``DELTA_MCP_HOST`` (default 0.0.0.0),
    ``DELTA_MCP_PORT`` (default 8000), ``RELAY_URL`` (relay base, via mcp_tools default).
    """
    mcp = build_mcp(os.environ.get("RELAY_URL"))
    mcp.settings.host = os.environ.get("DELTA_MCP_HOST", "0.0.0.0")
    mcp.settings.port = int(os.environ.get("DELTA_MCP_PORT", "8000"))
    mcp.run(transport="streamable-http")


if __name__ == "__main__":  # pragma: no cover
    main()
