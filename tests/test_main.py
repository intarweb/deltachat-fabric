import json

import pytest

from app import main
from app.config import BotSpec, Config


def _cfg(*localparts, leads=None):
    return Config(
        mail_domain="d.example", imap_host="mail.d.example",
        roster=[BotSpec(id=lp) for lp in localparts],
        realm_leads=leads or {},
    )


# -- SecretsStore -----------------------------------------------------------


def test_secrets_store_mints_and_persists(tmp_path):
    path = tmp_path / "secrets.json"
    s = main.SecretsStore(str(path), password_min_length=9)
    pw = s.get_or_create("bot-a")
    assert len(pw) >= 9
    # stable across calls
    assert s.get_or_create("bot-a") == pw
    # persisted to disk, reloaded by a fresh store
    s2 = main.SecretsStore(str(path))
    assert s2.get_or_create("bot-a") == pw


def test_secrets_store_file_is_mode_600(tmp_path):
    path = tmp_path / "secrets.json"
    s = main.SecretsStore(str(path))
    s.get_or_create("bot-b")
    assert oct(path.stat().st_mode & 0o777) == "0o600"


def test_secrets_store_distinct_per_bot(tmp_path):
    s = main.SecretsStore(str(tmp_path / "s.json"))
    assert s.get_or_create("a") != s.get_or_create("b")


# -- desired_localparts -----------------------------------------------------


def test_desired_localparts_dedup_order():
    cfg = Config(mail_domain="d", imap_host="m",
                 roster=[BotSpec(id="bot-a"), BotSpec(id="bot-b"),
                         BotSpec(id="bot-a", localpart="bot-a")])
    assert main.desired_localparts(cfg) == ["bot-a", "bot-b"]


# -- should_reconcile scheduling --------------------------------------------


def test_should_reconcile_first_run_honors_run_on_start():
    assert main.should_reconcile(100.0, None, 3600, run_on_start=True) is True
    assert main.should_reconcile(100.0, None, 3600, run_on_start=False) is False


def test_should_reconcile_interval_elapsed():
    assert main.should_reconcile(100.0 + 3600, 100.0, 3600) is True
    assert main.should_reconcile(100.0 + 3600, 100.0, 3600, run_on_start=False) is True


def test_should_reconcile_interval_not_yet_elapsed():
    assert main.should_reconcile(100.0 + 10, 100.0, 3600) is False


# -- reconcile_once (test-safe: injected onboard seam) ----------------------


@pytest.mark.asyncio
async def test_reconcile_once_provisions_desired_and_computes_prune(tmp_path):
    cfg = _cfg("bot-a", "bot-b")
    secrets = main.SecretsStore(str(tmp_path / "s.json"), cfg.password_min_length)
    onboarded = []

    async def fake_onboard(localpart, password):
        onboarded.append((localpart, password))
        return True

    res = await main.reconcile_once(cfg, secrets, existing=["bot-b", "bot-c"],
                                    onboard=fake_onboard)

    # every desired account (re)asserted via onboarding into the core
    assert res["provisioned"] == ["bot-a", "bot-b"]
    assert res["failed"] == []
    # diff vs existing: bot-a is new; bot-c is no longer desired → prune report
    assert res["to_provision"] == ["bot-a"]
    assert res["prune"] == ["bot-c"]
    # onboarded exactly the desired bots
    assert {lp for lp, _ in onboarded} == {"bot-a", "bot-b"}
    # passwords came from the secrets store (persisted)
    assert secrets.get_or_create("bot-a") in {pw for _, pw in onboarded}


@pytest.mark.asyncio
async def test_reconcile_once_reports_onboard_failures(tmp_path):
    cfg = _cfg("bot-a")
    secrets = main.SecretsStore(str(tmp_path / "s.json"))

    async def failing_onboard(localpart, password):
        return False

    res = await main.reconcile_once(cfg, secrets, existing=[], onboard=failing_onboard)
    assert res["failed"] == ["bot-a"]
    assert res["provisioned"] == []


@pytest.mark.asyncio
async def test_reconcile_once_onboard_exception_marks_failed(tmp_path):
    """A raising onboard (core/transport error) is caught → that bot is 'failed', and the
    reconcile pass survives (a single bad account never crashes the loop)."""
    cfg = _cfg("bot-a", "bot-b")
    secrets = main.SecretsStore(str(tmp_path / "s.json"))

    async def boom(localpart, password):
        if localpart == "bot-a":
            raise RuntimeError("core down")
        return True

    res = await main.reconcile_once(cfg, secrets, existing=[], onboard=boom)
    assert res["failed"] == ["bot-a"]
    assert res["provisioned"] == ["bot-b"]


@pytest.mark.asyncio
async def test_reconcile_once_no_real_io_when_onboard_injected(tmp_path):
    """Guard: with the onboard injected, reconcile_once never touches a live core/IMAP."""
    cfg = _cfg("a", "b")
    secrets = main.SecretsStore(str(tmp_path / "s.json"))
    seen = []

    async def fake_onboard(*a):
        seen.append(a)
        return True

    await main.reconcile_once(cfg, secrets, existing=[], onboard=fake_onboard)
    assert len(seen) == 2  # exactly the two desired bots, no network


# -- Service wiring is importable/constructible without I/O -----------------


class _FakeBackend:
    """Minimal DeltaBackend fake for the wiring test (no rpc-server)."""
    def __init__(self):
        self._localpart_to_accid = {"bot-a": 1}

    def account_id_for(self, lp):
        return self._localpart_to_accid.get(lp)


def test_service_wiring_builds_app_without_io(tmp_path):
    import httpx

    from app.relay import AgentDirectory, HoldQueue, Relay

    cfg = _cfg("bot-a", leads={"default": "bot-a"})
    backend = _FakeBackend()
    directory = AgentDirectory(cfg, httpx.AsyncClient())
    hold = HoldQueue(str(tmp_path))
    relay = Relay(cfg, backend, directory, hold)
    secrets = main.SecretsStore(str(tmp_path / "s.json"))

    svc = main.Service(cfg, relay, secrets)
    # the FastAPI app is wired and carries the /send route
    routes = {r.path for r in svc.app.routes}
    assert "/send" in routes and "/healthz" in routes
    assert svc.relay is relay


def test_service_app_send_route_works_end_to_end(tmp_path):
    """The wired app actually serves /send through the injected fake backend."""
    import httpx
    from fastapi.testclient import TestClient

    from app.relay import AgentDirectory, HoldQueue, Relay

    class SendBackend(_FakeBackend):
        def send(self, accid, chat_id, text):
            return 4242

    cfg = _cfg("bot-a")
    relay = Relay(cfg, SendBackend(), AgentDirectory(cfg, httpx.AsyncClient()),
                  HoldQueue(str(tmp_path)))
    svc = main.Service(cfg, relay, main.SecretsStore(str(tmp_path / "s.json")))
    client = TestClient(svc.app)
    r = client.post("/send", json={"bot_id": "bot-a", "target": 7, "text": "hi"})
    assert r.status_code == 200
    assert r.json() == {"status": "sent", "msg_id": 4242, "account_id": 1}


# -- event pump: the startup-freeze regression ------------------------------


def test_event_pump_dispatches_incoming_and_never_blocks_the_loop():
    """Regression for the startup-freeze bug (both uvicorns stuck at 'Waiting for application
    startup', never bind): the BLOCKING deltachat event read must run in a thread and must
    NOT freeze the asyncio loop; incoming messages bridge onto the loop via the pump."""
    import asyncio
    import threading

    async def _run():
        started = threading.Event()
        release = threading.Event()
        got: list = []

        class BlockingBackend:
            def __init__(self):
                self.calls = 0

            def next_inbound(self):
                self.calls += 1
                if self.calls == 1:
                    return "MSG-1"                # one incoming to dispatch
                started.set()
                release.wait(2.0)                 # then block, like get_next_event long-poll
                return None

        class FakeRelay:
            async def handle_inbound(self, msg):
                got.append(msg)
                return []

        loop = asyncio.get_running_loop()
        stop = {"v": False}
        th = threading.Thread(
            target=main._event_pump,
            args=(BlockingBackend(), FakeRelay(), loop),
            kwargs={"_should_stop": lambda: stop["v"]},
            daemon=True,
        )
        th.start()
        # the incoming msg is dispatched onto our loop while we stay responsive...
        for _ in range(100):
            if got:
                break
            await asyncio.sleep(0.01)
        # ...and the loop is still alive even though the pump thread is now blocked in read.
        assert started.wait(1.0)
        assert await asyncio.wait_for(asyncio.sleep(0, result="alive"), timeout=1.0) == "alive"
        assert got == ["MSG-1"]
        stop["v"] = True
        release.set()
        th.join(2.0)

    asyncio.run(_run())
