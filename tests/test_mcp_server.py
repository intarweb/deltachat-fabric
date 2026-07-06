"""Unit tests for the bifrost-facing MCP server (app.mcp_server).

NO live network: the relay HTTP layer is mocked via httpx.MockTransport, injected by
patching httpx.AsyncClient (which _RelayTool constructs when no client is passed). We
assert (1) build_mcp registers exactly the 7 tools with the right names + clean schema,
(2) each tool delegates to the relay contract (right method/path/body), and (3) the app
exposes /mcp.
"""
from __future__ import annotations

import json

import httpx
import pytest

import app.mcp_server as mcp_server
from app.mcp_server import TOOL_NAMES, build_mcp, build_mcp_app


class _RecordingTransport(httpx.MockTransport):
    """Records every request and replies with a canned JSON body per (method, path)."""

    def __init__(self, responses: dict):
        self.requests: list[httpx.Request] = []
        self.responses = responses

        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            key = (request.method, request.url.path)
            body = self.responses.get(key, {"status": "ok"})
            return httpx.Response(200, json=body)

        super().__init__(handler)


@pytest.fixture
def relay_recorder(monkeypatch):
    """Patch httpx.AsyncClient so every relay client in build_mcp uses one MockTransport."""
    recorder = _RecordingTransport(
        {
            ("POST", "/send"): {"status": "sent", "msg_id": 1, "account_id": 7},
            ("GET", "/contacts"): {"account_id": 7, "contacts": []},
            ("GET", "/channels"): {"account_id": 7, "channels": []},
            ("POST", "/send_channel"): {"status": "sent", "msg_id": 2, "account_id": 7, "channel_id": 5},
            ("POST", "/channel"): {"status": "created", "channel_id": 5, "account_id": 7, "name": "n", "members": []},
            ("POST", "/channel/member"): {"status": "added", "channel_id": 5, "account_id": 7, "contact": "c@x"},
            ("POST", "/react"): {"status": "reacted", "account_id": 7, "chat_id": 1, "msg_id": 1, "emoji": "x"},
        }
    )
    real_init = httpx.AsyncClient.__init__

    def patched_init(self, *args, **kwargs):
        kwargs["transport"] = recorder
        real_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", patched_init)
    return recorder


# --------------------------------------------------------------------------- registration


async def test_build_mcp_registers_exactly_nine_tools_with_right_names():
    mcp = build_mcp(relay_url="http://relay.test:8080")
    tools = await mcp.list_tools()
    names = sorted(t.name for t in tools)
    assert names == sorted(TOOL_NAMES)
    assert len(tools) == 9
    assert {"delta_secure_join", "delta_messages"} <= set(names)


async def test_tools_have_clean_schema_for_tools_list():
    """bifrost's tools/list needs typed args + a docstring description per tool."""
    mcp = build_mcp(relay_url="http://relay.test:8080")
    tools = {t.name: t for t in await mcp.list_tools()}

    expected_args = {
        "delta_send": {"bot_id", "target", "text"},
        "delta_list_contacts": {"bot_id"},
        "delta_list_channels": {"bot_id"},
        "delta_send_channel": {"bot_id", "channel_id", "text"},
        "delta_create_channel": {"bot_id", "name", "members"},
        "delta_add_member": {"bot_id", "channel_id", "contact"},
        "delta_react": {"bot_id", "chat_id", "msg_id", "emoji"},
    }
    for name, args in expected_args.items():
        t = tools[name]
        assert t.description, f"{name} missing description"
        props = set(t.inputSchema.get("properties", {}).keys())
        assert props == args, f"{name} schema props {props} != {args}"


# --------------------------------------------------------------------------- delegation


def _result_text(res):
    # FastMCP.call_tool returns (content_blocks, structured) or a content list depending
    # on version; normalize to the structured dict.
    if isinstance(res, tuple):
        return res[1]
    # content-block list
    return json.loads(res[0].text)


async def test_delta_send_delegates_to_relay(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    res = await mcp.call_tool("delta_send", {"bot_id": "bot-a", "target": 42, "text": "hi"})
    req = relay_recorder.requests[-1]
    assert req.method == "POST"
    assert req.url.path == "/send"
    assert json.loads(req.content.decode()) == {"bot_id": "bot-a", "target": 42, "text": "hi"}
    assert _result_text(res)["status"] == "sent"


async def test_delta_list_contacts_delegates_to_relay(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool("delta_list_contacts", {"bot_id": "bot-a"})
    req = relay_recorder.requests[-1]
    assert req.method == "GET"
    assert req.url.path == "/contacts"
    assert dict(req.url.params) == {"bot_id": "bot-a"}


async def test_delta_list_channels_delegates_to_relay(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool("delta_list_channels", {"bot_id": "bot-lead"})
    req = relay_recorder.requests[-1]
    assert req.method == "GET"
    assert req.url.path == "/channels"
    assert dict(req.url.params) == {"bot_id": "bot-lead"}


async def test_delta_send_channel_delegates_to_relay(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool(
        "delta_send_channel", {"bot_id": "bot-a", "channel_id": 5, "text": "hi all"}
    )
    req = relay_recorder.requests[-1]
    assert req.method == "POST"
    assert req.url.path == "/send_channel"
    assert json.loads(req.content.decode()) == {
        "bot_id": "bot-a", "channel_id": 5, "text": "hi all"}


async def test_delta_create_channel_delegates_and_passes_members(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool(
        "delta_create_channel",
        {"bot_id": "bot-lead", "name": "ops", "members": ["a@x", "b@x"]},
    )
    req = relay_recorder.requests[-1]
    assert req.url.path == "/channel"
    assert json.loads(req.content.decode()) == {
        "bot_id": "bot-lead", "name": "ops", "members": ["a@x", "b@x"]}


async def test_delta_create_channel_defaults_members_empty(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool("delta_create_channel", {"bot_id": "bot-lead", "name": "solo"})
    req = relay_recorder.requests[-1]
    assert json.loads(req.content.decode())["members"] == []


async def test_delta_add_member_delegates_to_relay(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool(
        "delta_add_member", {"bot_id": "bot-lead", "channel_id": 5, "contact": "c@x"}
    )
    req = relay_recorder.requests[-1]
    assert req.url.path == "/channel/member"
    assert json.loads(req.content.decode()) == {
        "bot_id": "bot-lead", "channel_id": 5, "contact": "c@x"}


async def test_delta_react_delegates_to_relay(relay_recorder):
    mcp = build_mcp(relay_url="http://relay.test:8080")
    await mcp.call_tool(
        "delta_react", {"bot_id": "bot-a", "chat_id": 1, "msg_id": 9, "emoji": "\U0001f44d"}
    )
    req = relay_recorder.requests[-1]
    assert req.url.path == "/react"
    assert json.loads(req.content.decode()) == {
        "bot_id": "bot-a", "chat_id": 1, "msg_id": 9, "emoji": "\U0001f44d"}


# --------------------------------------------------------------------------- app / endpoint


def test_build_mcp_app_exposes_mcp_endpoint():
    app = build_mcp_app(relay_url="http://relay.test:8080")
    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/mcp" in paths


def test_relay_url_is_injected_no_fleet_identity():
    """Generic audit: the relay_url flows into every client; nothing baked."""
    mcp = build_mcp(relay_url="http://injected.example:9999")
    # build succeeds with an arbitrary injected relay_url and registers the 7 tools.
    assert mcp.name == "deltachat"


# --------------------------------------------------------------------------- transport security (Host-header / DNS-rebinding)


def test_transport_security_defaults_protection_off(monkeypatch):
    """🔴 Regression: the mcp SDK's default rebinding protection allows only localhost Hosts,
    so an in-cluster client connecting by service name (Host: mcp-deltachat:8000) gets 421.
    Our default disables protection (internal-net, gateway-fronted) so the deployed Host is
    accepted."""
    monkeypatch.delenv("DELTA_MCP_ALLOWED_HOSTS", raising=False)
    monkeypatch.delenv("DELTA_MCP_ALLOWED_ORIGINS", raising=False)
    ts = mcp_server._transport_security()
    assert ts.enable_dns_rebinding_protection is False


def test_transport_security_env_reenables_and_scopes(monkeypatch):
    """A browser-exposed deploy can re-enable + scope protection via env (generic — injected)."""
    monkeypatch.setenv("DELTA_MCP_ALLOWED_HOSTS", "mcp-deltachat:8000, mcp-deltachat")
    monkeypatch.delenv("DELTA_MCP_ALLOWED_ORIGINS", raising=False)
    ts = mcp_server._transport_security()
    assert ts.enable_dns_rebinding_protection is True
    assert ts.allowed_hosts == ["mcp-deltachat:8000", "mcp-deltachat"]
    assert ts.allowed_origins == ["mcp-deltachat:8000", "mcp-deltachat"]  # defaults to hosts
