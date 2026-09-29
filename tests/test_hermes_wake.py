"""Bots outside the a2a directory (Hermes Agent bots) are woken through a roster-configured
transport: ``wakes: {<bot>: {kind: hermes-runs, url, key_env}}`` → ``POST <url>/v1/runs``
(Hermes' documented programmatic input) with the API server key from env."""
import json

import httpx

from app.config import BotSpec, Config
from app.relay import (AgentDirectory, DeltaChatBackend, HoldQueue, InboundMessage, Relay)
from tests.test_relay import FakeBackend, _FakeRpc

DOMAIN = "deltachat.example.net"


def _config():
    return Config(
        mail_domain=DOMAIN, imap_host="mail.example.net",
        a2a_directory_url="http://directory.test/agents",
        roster=[BotSpec(id="lead", realm="r"), BotSpec(id="herm", realm="r", display_name="Herm")],
        realm_leads={"r": "lead"},
        wakes={"herm": {"kind": "hermes-runs", "url": "http://herm.test:8642",
                        "key_env": "HERM_KEY"}},
    )


def _relay(tmp_path, status=202):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"agents": []})
        return httpx.Response(status, json={"run_id": "run_1", "status": "started"})

    cfg = _config()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    relay = Relay(cfg, FakeBackend(accounts={"herm": 5, "lead": 3}),
                  AgentDirectory(cfg, client), HoldQueue(str(tmp_path)))
    return relay, calls


DM = InboundMessage(account_id=5, chat_id=12, msg_id=99, text="hello herm", is_group=False,
                    members=[], mentioned=[], from_localpart="lead", rfc724_mid="abc@x")


async def test_dm_to_hermes_bot_starts_a_run(tmp_path, monkeypatch):
    monkeypatch.setenv("HERM_KEY", "k-123")
    relay, calls = _relay(tmp_path)
    assert await relay.handle_inbound(DM) == ["herm"]
    posts = [c for c in calls if c.method == "POST"]
    assert len(posts) == 1 and not [c for c in calls if c.method == "GET"]  # no directory hit
    req = posts[0]
    assert str(req.url) == "http://herm.test:8642/v1/runs"
    assert req.headers["authorization"] == "Bearer k-123"
    assert req.headers["idempotency-key"]                      # stable → safe retries
    body = json.loads(req.content)
    assert "hello herm" in body["input"] and "lead" in body["input"]
    assert body["session_id"] == "deltachat:herm:12"
    assert req.headers["x-hermes-session-key"] == "deltachat:herm:12"
    assert len(relay.hold) == 0


async def test_retry_reuses_the_idempotency_key(tmp_path, monkeypatch):
    monkeypatch.setenv("HERM_KEY", "k")
    relay, calls = _relay(tmp_path)
    d = relay.directory
    await d.wake("http://herm.test:8642", "herm", {"text": "t", "rfc724_mid": "m1", "chat_id": 1})
    await d.wake("http://herm.test:8642", "herm", {"text": "t", "rfc724_mid": "m1", "chat_id": 1})
    keys = {c.headers["idempotency-key"] for c in calls if c.method == "POST"}
    assert len(keys) == 1


async def test_missing_key_env_holds_the_wake(tmp_path, monkeypatch):
    monkeypatch.delenv("HERM_KEY", raising=False)
    relay, calls = _relay(tmp_path)
    assert await relay.handle_inbound(DM) == []
    assert not [c for c in calls if c.method == "POST"]   # never sends an unauthenticated run
    assert len(relay.hold) == 1


async def test_rejected_run_holds_the_wake(tmp_path, monkeypatch):
    monkeypatch.setenv("HERM_KEY", "k")
    relay, _ = _relay(tmp_path, status=401)
    assert await relay.handle_inbound(DM) == []
    assert len(relay.hold) == 1


def test_roster_wakes_map_loads(tmp_path, monkeypatch):
    roster = tmp_path / "roster.yaml"
    roster.write_text(
        "bots:\n  - {id: a, realm: r}\n  - {id: h, realm: r, display_name: H}\n"
        "realm_leads: {r: a}\n"
        "wakes:\n  h: {kind: hermes-runs, url: 'http://h:8642', key_env: H_KEY}\n")
    monkeypatch.setenv("DELTA_MAIL_DOMAIN", DOMAIN)
    cfg = Config.load(str(roster))
    assert [b.id for b in cfg.roster] == ["a", "h"]
    assert cfg.wakes == {"h": {"kind": "hermes-runs", "url": "http://h:8642", "key_env": "H_KEY"}}


def test_io_starts_only_for_roster_accounts(tmp_path):
    """Bots that left the roster keep their accounts but stay DORMANT (no IO)."""
    rpc = _FakeRpc()
    for lp in ("lead", "herm", "retired"):
        accid = rpc.add_account()
        rpc.accounts[accid] = f"{lp}@{DOMAIN}"
    rpc.start_io = lambda accid: rpc.started.append(accid)
    be = DeltaChatBackend(_config(), str(tmp_path), _rpc=rpc)
    started = be._start_io_for_roster()
    assert sorted(started) == sorted([be.account_id_for("herm"), be.account_id_for("lead")])
    assert be.account_id_for("retired") not in rpc.started
    assert be.account_id_for("retired") is not None      # account kept (recoverable)
