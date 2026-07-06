"""Unit tests for the delta MCP tools — mocked relay, no live network."""
from __future__ import annotations

import json

import httpx
import pytest

from app.mcp_tools import (
    DeltaAddMemberTool,
    DeltaCreateChannelTool,
    DeltaCreateInviteTool,
    DeltaDeleteChatTool,
    DeltaListChannelsTool,
    DeltaListContactsTool,
    DeltaMessagesTool,
    DeltaReactTool,
    DeltaSecureJoinTool,
    DeltaSendChannelTool,
    DeltaSendTool,
)


def make_tool(handler, cls=DeltaSendTool):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return cls(relay_url="http://relay.test:8080", client=client)


async def test_delta_send_posts_send_contract_and_returns_result():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"status": "sent", "msg_id": 1001, "account_id": 7})

    tool = make_tool(handler)
    result = await tool.send(bot_id="bot-a", target=42, text="hi")

    assert captured["url"] == "http://relay.test:8080/send"
    assert captured["body"] == {"bot_id": "bot-a", "target": 42, "text": "hi"}
    assert result == {"status": "sent", "msg_id": 1001, "account_id": 7}


async def test_delta_send_raises_on_unknown_bot():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "no delta account for bot 'bot-c'"})

    tool = make_tool(handler)
    with pytest.raises(RuntimeError) as ei:
        await tool.send(bot_id="bot-c", target=1, text="x")
    assert "404" in str(ei.value)
    assert "bot-c" in str(ei.value)


# --------------------------------------------------------------------------- list_contacts


async def test_delta_list_contacts_gets_contract_and_returns_result():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"account_id": 7, "contacts": [
            {"id": 1, "address": "bot-b@x.net", "display_name": "bot-b"}]})

    tool = make_tool(handler, DeltaListContactsTool)
    result = await tool.list_contacts(bot_id="bot-a")

    assert captured["method"] == "GET"
    assert captured["url"] == "http://relay.test:8080/contacts?bot_id=bot-a"
    assert result["contacts"][0]["address"] == "bot-b@x.net"


async def test_delta_list_contacts_raises_on_unknown_bot():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "no delta account for bot 'bot-c'"})

    tool = make_tool(handler, DeltaListContactsTool)
    with pytest.raises(RuntimeError) as ei:
        await tool.list_contacts(bot_id="bot-c")
    assert "404" in str(ei.value)


# --------------------------------------------------------------------------- list_channels


async def test_delta_list_channels_returns_result():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert str(request.url) == "http://relay.test:8080/channels?bot_id=bot-lead"
        return httpx.Response(200, json={"account_id": 3, "channels": [
            {"id": 555, "name": "realm-a", "members": ["bot-lead", "bot-a"]}]})

    tool = make_tool(handler, DeltaListChannelsTool)
    result = await tool.list_channels(bot_id="bot-lead")
    assert result["channels"][0]["members"] == ["bot-lead", "bot-a"]


async def test_delta_list_channels_raises_on_server_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, json={"detail": "backend down"})

    tool = make_tool(handler, DeltaListChannelsTool)
    with pytest.raises(RuntimeError) as ei:
        await tool.list_channels(bot_id="bot-a")
    assert "502" in str(ei.value)


# --------------------------------------------------------------------------- send_channel


async def test_delta_send_channel_posts_contract():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={
            "status": "sent", "msg_id": 1002, "account_id": 7, "channel_id": 555})

    tool = make_tool(handler, DeltaSendChannelTool)
    result = await tool.send_channel(bot_id="bot-a", channel_id=555, text="hi all")

    assert captured["url"] == "http://relay.test:8080/send_channel"
    assert captured["body"] == {"bot_id": "bot-a", "channel_id": 555, "text": "hi all"}
    assert result["channel_id"] == 555


async def test_delta_send_channel_raises_on_unknown_bot():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "no delta account for bot 'bot-c'"})

    tool = make_tool(handler, DeltaSendChannelTool)
    with pytest.raises(RuntimeError):
        await tool.send_channel(bot_id="bot-c", channel_id=1, text="x")


# --------------------------------------------------------------------------- create_channel


async def test_delta_create_channel_passes_members_from_args():
    # 🔴 generic: members are exactly the caller's argument, nothing baked
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={
            "status": "created", "channel_id": 501, "account_id": 3,
            "name": "ops", "members": ["bot-a@x.net", "bot-b@x.net"]})

    tool = make_tool(handler, DeltaCreateChannelTool)
    result = await tool.create_channel(
        bot_id="bot-lead", name="ops", members=["bot-a@x.net", "bot-b@x.net"])

    assert captured["body"] == {
        "bot_id": "bot-lead", "name": "ops", "members": ["bot-a@x.net", "bot-b@x.net"]}
    assert result["channel_id"] == 501


async def test_delta_create_channel_defaults_members_to_empty():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={
            "status": "created", "channel_id": 502, "account_id": 3,
            "name": "solo", "members": []})

    tool = make_tool(handler, DeltaCreateChannelTool)
    await tool.create_channel(bot_id="bot-lead", name="solo")
    assert captured["body"]["members"] == []


# --------------------------------------------------------------------------- add_member


async def test_delta_add_member_posts_contract():
    # 🔴 generic: the contact is a caller argument, never derived from a baked roster
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={
            "status": "added", "channel_id": 555, "account_id": 3, "contact": "bot-i@x.net"})

    tool = make_tool(handler, DeltaAddMemberTool)
    result = await tool.add_member(bot_id="bot-lead", channel_id=555, contact="bot-i@x.net")

    assert captured["body"] == {
        "bot_id": "bot-lead", "channel_id": 555, "contact": "bot-i@x.net"}
    assert result["contact"] == "bot-i@x.net"


async def test_delta_add_member_raises_on_unknown_bot():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "no delta account for bot 'bot-c'"})

    tool = make_tool(handler, DeltaAddMemberTool)
    with pytest.raises(RuntimeError):
        await tool.add_member(bot_id="bot-c", channel_id=1, contact="x@x.net")


# --------------------------------------------------------------------------- react


async def test_delta_react_posts_contract():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={
            "status": "reacted", "account_id": 7, "chat_id": 42, "msg_id": 99, "emoji": "👍"})

    tool = make_tool(handler, DeltaReactTool)
    result = await tool.react(bot_id="bot-a", chat_id=42, msg_id=99, emoji="👍")

    assert captured["url"] == "http://relay.test:8080/react"
    assert captured["body"] == {"bot_id": "bot-a", "chat_id": 42, "msg_id": 99, "emoji": "👍"}
    assert result["status"] == "reacted"


async def test_delta_react_raises_on_server_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, json={"detail": "send_reaction failed"})

    tool = make_tool(handler, DeltaReactTool)
    with pytest.raises(RuntimeError) as ei:
        await tool.react(bot_id="bot-a", chat_id=1, msg_id=1, emoji="x")
    assert "502" in str(ei.value)


# --------------------------------------------------------------------------- secure_join


async def test_delta_secure_join_posts_contract_and_returns_result():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"status": "securejoin-initiated", "account_id": 3, "chat_id": 4321})

    tool = make_tool(handler, DeltaSecureJoinTool)
    result = await tool.secure_join(bot_id="bot-a", invite="https://i.delta.chat/#FAKE")

    assert captured["url"] == "http://relay.test:8080/secure_join"
    assert captured["body"] == {"bot_id": "bot-a", "invite": "https://i.delta.chat/#FAKE"}
    assert result == {"status": "securejoin-initiated", "account_id": 3, "chat_id": 4321}


async def test_delta_messages_gets_contract_and_returns_result():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"account_id": 7, "chat_id": 55,
                                         "messages": [{"id": 9, "text": "hi", "from_id": 2}]})

    tool = make_tool(handler, DeltaMessagesTool)
    result = await tool.messages(bot_id="bot-a", chat_id=55, limit=20)
    assert "/messages" in captured["url"] and "bot_id=bot-a" in captured["url"] and "chat_id=55" in captured["url"]
    assert result["messages"] == [{"id": 9, "text": "hi", "from_id": 2}]


async def test_delta_create_invite_gets_contract_and_returns_result():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"account_id": 1, "invite": "https://i.delta.chat/#X"})

    tool = make_tool(handler, DeltaCreateInviteTool)
    result = await tool.create_invite(bot_id="bot-a")
    assert "/invite" in captured["url"] and "bot_id=bot-a" in captured["url"]
    assert result["invite"] == "https://i.delta.chat/#X"


async def test_delta_delete_chat_posts_contract_and_returns_result():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"status": "deleted", "account_id": 3, "chat_id": 12})

    tool = make_tool(handler, DeltaDeleteChatTool)
    result = await tool.delete_chat(bot_id="bot-a", chat_id=12)

    assert captured["url"] == "http://relay.test:8080/delete_chat"
    assert captured["body"] == {"bot_id": "bot-a", "chat_id": 12}
    assert result == {"status": "deleted", "account_id": 3, "chat_id": 12}
