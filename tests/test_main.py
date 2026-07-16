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


def test_event_pump_does_NOT_auto_react_on_inbound():
    """🔴 Spec (authoritative interaction protocol): the relay is wake+deliver+surface ONLY — it must
    NEVER emit a reaction on a bot's behalf. The 👀 proof-of-life is AGENT-side (the woken bot
    calls delta_react as its first step), NOT relay code. Regression on the false-liveness bug:
    the pump used to auto-fire backend.react_seen (👀) on every inbound → Justin saw a 👀 even
    when the bot never actually woke (a lie). This asserts the inbound pump auto-reacts on
    NOTHING while still dispatching the wake. FAILS on the old auto-👀 pump code (proves
    fail-on-broken)."""
    import asyncio

    from app.relay import InboundMessage

    async def _run():
        reacted: list = []
        submitted: list = []

        class SpyBackend:
            def __init__(self):
                self.calls = 0

            def next_inbound(self):
                self.calls += 1
                if self.calls == 1:
                    return InboundMessage(account_id=7, chat_id=1, msg_id=5, text="hi",
                                          is_group=False, members=[], mentioned=[])
                raise StopIteration  # break the pump loop after one message

            # 🔴 the relay must call NONE of these on an inbound message (no auto-react):
            def react_seen(self, accid, msg_id):
                reacted.append(("react_seen", accid, msg_id))

            def react(self, accid, msg_id, emoji):
                reacted.append(("react", accid, msg_id, emoji))

        class FakeRelay:
            async def handle_inbound(self, msg):
                return []

            async def handle_reaction(self, r):
                return []

        loop = asyncio.get_running_loop()

        def _submit(coro):
            submitted.append(coro)
            coro.close()  # we assert on auto-reactions + dispatch, not delivery result

        main._event_pump(SpyBackend(), FakeRelay(), loop,
                         _should_stop=lambda: False, _submit=_submit)

        assert reacted == []          # relay did NOT auto-react on the bot's behalf
        assert len(submitted) == 1    # the inbound was still dispatched to the wake path

    asyncio.run(_run())


def test_event_pump_dispatches_verified_to_provision_not_wake():
    """A securejoin-verified event (InboundVerified) is routed to on_verified (event-driven
    provisioning), NOT to the wake path — proving verification provisioning is event-driven with
    no reconcile-time wait/poll."""
    import asyncio
    from app.relay import InboundVerified

    async def _run():
        provisioned: list = []
        waked: list = []

        class SpyBackend:
            def __init__(self):
                self.calls = 0

            def next_inbound(self):
                self.calls += 1
                if self.calls == 1:
                    return InboundVerified(account_id=10, contact_id=3, addr="m1@d.example")
                raise StopIteration

        class FakeRelay:
            async def handle_inbound(self, msg):
                waked.append(msg)

            async def handle_reaction(self, r):
                waked.append(r)

        async def on_verified(ev):
            provisioned.append(ev)

        loop = asyncio.get_running_loop()

        def _submit(coro):
            # run the coroutine to completion so the append fires, in-thread
            try:
                coro.send(None)
            except StopIteration:
                pass

        main._event_pump(SpyBackend(), FakeRelay(), loop, on_verified=on_verified,
                         _should_stop=lambda: False, _submit=_submit)

        assert len(provisioned) == 1 and provisioned[0].addr == "m1@d.example"
        assert waked == []  # a verified event must NOT hit the wake/deliver path

    asyncio.run(_run())


def test_serve_boots_and_both_uvicorns_bind_with_blocking_backend(tmp_path, monkeypatch):
    """🔴 The integration test that would have CAUGHT the freeze: boot the REAL `_serve`
    gather and assert BOTH uvicorns bind + respond within a timeout — EVEN with a backend
    whose event read BLOCKS like the real deltachat get_next_event(). If the blocking read
    regresses back onto the event loop, uvicorn never binds → these HTTP calls time out →
    this test FAILS. (Units passing while the app won't boot is the exact gap this closes.)"""
    import asyncio
    import contextlib
    import socket
    import threading

    import httpx

    def _free_port() -> int:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        p = s.getsockname()[1]
        s.close()
        return p

    relay_port, mcp_port = _free_port(), _free_port()
    monkeypatch.setenv("RELAY_HOST", "127.0.0.1")
    monkeypatch.setenv("RELAY_PORT", str(relay_port))
    monkeypatch.setenv("DELTA_MCP_HOST", "127.0.0.1")
    monkeypatch.setenv("DELTA_MCP_PORT", str(mcp_port))
    monkeypatch.setenv("DELTA_RECONCILE_ON_START", "0")   # this test is about binding, not onboarding
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DELTA_BACKUP_DIR", str(tmp_path / "backup"))
    monkeypatch.setenv("DELTA_BACKUP_INTERVAL", "99999")   # backup loop sleeps; never fires in-test

    release = threading.Event()

    class BlockingBackend:
        """Its event read BLOCKS (like get_next_event long-poll). If run on the loop, freeze."""
        _localpart_to_accid: dict = {}

        def next_inbound(self):
            release.wait(15)     # block the pump thread; must NOT freeze the asyncio loop
            return None

        def ensure_account(self, *a, **k):
            return True

    from app.relay import AgentDirectory, HoldQueue, Relay

    cfg = _cfg("bot-a", leads={"default": "bot-a"})
    relay = Relay(cfg, BlockingBackend(), AgentDirectory(cfg, httpx.AsyncClient()),
                  HoldQueue(str(tmp_path)))
    svc = main.Service(cfg, relay, main.SecretsStore(str(tmp_path / "s.json")), backup_backend=None)

    async def _await_ok(client, url, timeout=12.0):
        import time
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            try:
                r = await client.get(url, timeout=2.0)
                if r.status_code == 200:
                    return r
                last = r.status_code
            except Exception as e:  # not bound yet
                last = repr(e)
            await asyncio.sleep(0.1)
        raise AssertionError(f"{url} never became ready (last={last}) — loop likely frozen")

    async def _run():
        task = asyncio.create_task(main._serve(svc))
        try:
            async with httpx.AsyncClient() as c:
                # relay uvicorn must bind + /healthz respond — impossible if the loop is frozen
                r = await _await_ok(c, f"http://127.0.0.1:{relay_port}/healthz")
                assert r.json()["status"] == "ok"
                # 🔴 MCP uvicorn must ACCEPT a by-name Host (Host: mcp-deltachat:8000), not just
                # localhost — the DNS-rebinding-protection default returns 421 to an in-cluster
                # client connecting by service name (the bug that blocked bifrost). POST a real
                # initialize with a non-localhost Host and assert it is NOT rejected.
                init = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                                   "clientInfo": {"name": "t", "version": "1"}}}
                mr = await c.post(
                    f"http://127.0.0.1:{mcp_port}/mcp",
                    headers={"Host": "mcp-deltachat:8000", "Content-Type": "application/json",
                             "Accept": "application/json, text/event-stream"},
                    json=init, timeout=5.0,
                )
                assert mr.status_code != 421, f"by-name Host rejected (421): {mr.text}"
                assert "Invalid Host header" not in mr.text
                assert mr.status_code < 500
        finally:
            release.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    asyncio.run(_run())


# -- per-realm channel provisioning (Vikunja proj-23 step 5b) ----------------


def test_desired_channels_one_per_realm_with_a_lead():
    cfg = Config(
        mail_domain="d.example", imap_host="m",
        roster=[BotSpec(id="lead", realm="r1"), BotSpec(id="a", realm="r1"),
                BotSpec(id="b", realm="r2"), BotSpec(id="c", realm="r3")],
        realm_leads={"r1": "lead", "r2": "b"},  # r3 has NO lead → skipped
    )
    chans = {c["realm"]: c for c in main.desired_channels(cfg)}
    assert set(chans) == {"r1", "r2"}          # r3 skipped (no main → can't route)
    assert chans["r1"]["lead"] == "lead"
    assert chans["r1"]["name"] == "r1"
    assert chans["r1"]["members"] == ["a", "lead"]   # sorted, de-duped
    assert chans["r2"]["members"] == ["b"]


class _ChanBackend:
    """Fake DeltaBackend surface for provision_channels (no live core)."""

    def __init__(self, accounts, channels=None):
        self._acc = accounts                       # localpart -> accid
        self._channels = channels or {}            # accid -> [{id,name,members}]
        self.created: list = []
        self.added: list = []
        self._next = 900

    def account_id_for(self, lp):
        return self._acc.get(lp)

    def list_channels(self, accid):
        return self._channels.get(accid, [])

    def create_channel(self, accid, name, members):
        self._next += 1
        self.created.append((accid, name, list(members)))
        return self._next

    def add_member(self, accid, chat_id, contact):
        self.added.append((accid, chat_id, contact))


def test_provision_channels_creates_missing_channel_with_members_minus_lead():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1"), BotSpec(id="a", realm="r1")],
                 realm_leads={"r1": "lead"})
    be = _ChanBackend(accounts={"lead": 3, "a": 4})
    res = main.provision_channels(cfg, be)
    # created by the lead's account, members = realm minus the lead, as addresses
    assert be.created == [(3, "r1", ["a@d.example"])]
    assert res[0]["created"]


def test_provision_channels_idempotent_adds_only_missing_members():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1"), BotSpec(id="a", realm="r1"),
                         BotSpec(id="x", realm="r1")],
                 realm_leads={"r1": "lead"})
    # channel already exists with lead+a present → only x is added, nothing recreated
    be = _ChanBackend(accounts={"lead": 3, "a": 4, "x": 5},
                      channels={3: [{"id": 77, "name": "r1", "members": ["lead", "a"]}]})
    res = main.provision_channels(cfg, be)
    assert be.created == []
    assert be.added == [(3, 77, "x@d.example")]
    assert res[0]["added"] == 1


def test_provision_channels_skips_lead_not_onboarded_yet():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1")], realm_leads={"r1": "lead"})
    be = _ChanBackend(accounts={})               # lead has no account yet
    res = main.provision_channels(cfg, be)
    assert res[0]["skipped"] == "lead-not-onboarded"
    assert be.created == []


def test_desired_channels_lead_id_normalized_to_localpart():
    # 🔴 Break (b): realm_leads maps realm -> bot ID, but members[] + account lookup key by
    # LOCALPART. desired_channels must normalize the lead to its localpart, else account_id_for
    # (localpart-keyed) returns None and the realm is silently skipped whenever id != localpart.
    cfg = Config(
        mail_domain="d.example", imap_host="m",
        roster=[BotSpec(id="lead", realm="r1", localpart="pantest01"),
                BotSpec(id="m1", realm="r1", localpart="pantest02")],
        realm_leads={"r1": "lead"},               # keyed by bot ID, not localpart
    )
    ch = main.desired_channels(cfg)[0]
    assert ch["lead"] == "pantest01"              # bot-id -> localpart
    assert ch["members"] == ["pantest01", "pantest02"]


def test_provision_channels_resolves_lead_when_id_ne_localpart():
    # End-to-end of break (b): with id != localpart the lead account now resolves and the lead
    # is correctly excluded from the member address list.
    cfg = Config(
        mail_domain="d.example", imap_host="m",
        roster=[BotSpec(id="lead", realm="r1", localpart="pantest01"),
                BotSpec(id="m1", realm="r1", localpart="pantest02")],
        realm_leads={"r1": "lead"},
    )
    be = _ChanBackend(accounts={"pantest01": 10, "pantest02": 11})
    res = main.provision_channels(cfg, be)
    assert be.created == [(10, "r1", ["pantest02@d.example"])]   # lead excluded, lead acct = 10
    assert res[0]["created"]


# -- securejoin STAR (break (a): make members verified key-contacts of the realm lead) --------


class _StarBackend:
    """Fake DeltaBackend surface for securejoin_star + provision_verified_member (no live core)."""

    def __init__(self, accounts, *, verified=None, channels=None):
        self._acc = accounts                       # localpart -> accid
        self._verified = set(verified or [])        # (lead_accid, addr) verified key-contacts
        self.invites = 0
        self.joins: list = []                       # (member_accid, invite)
        self._channels = channels or {}             # lead_accid -> [ {id,name,members:[lp]} ]
        self.created: list = []                     # (lead_accid, name, [addr])
        self.added: list = []                       # (lead_accid, chat_id, addr)

    def account_id_for(self, lp):
        return self._acc.get(lp)

    def localpart_for(self, accid):
        for lp, a in self._acc.items():
            if a == accid:
                return lp
        return None

    def is_verified_key_contact(self, accid, addr):
        return (accid, addr.strip().lower()) in self._verified

    def create_invite(self, accid):
        self.invites += 1
        return f"invite-for-{accid}"

    def secure_join(self, member_accid, invite):
        self.joins.append((member_accid, invite))
        return 1

    # provisioning surface
    def list_channels(self, accid):
        return list(self._channels.get(accid, []))

    def create_channel(self, accid, name, member_addrs):
        chat_id = 100 + len(self.created)
        self.created.append((accid, name, list(member_addrs)))
        self._channels.setdefault(accid, []).append(
            {"id": chat_id, "name": name,
             "members": [a.split("@", 1)[0] for a in member_addrs]})
        return chat_id

    def add_member(self, accid, chat_id, addr):
        self.added.append((accid, chat_id, addr))
        for c in self._channels.get(accid, []):
            if c["id"] == chat_id:
                c["members"].append(addr.split("@", 1)[0])


def _star_be(accounts, **kw):
    return _StarBackend(accounts, **kw)


def test_securejoin_star_fires_and_forgets_each_member():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1", localpart="lead"),
                         BotSpec(id="m1", realm="r1", localpart="m1"),
                         BotSpec(id="m2", realm="r1", localpart="m2")],
                 realm_leads={"r1": "lead"})
    be = _star_be({"lead": 10, "m1": 11, "m2": 12})
    res = main.securejoin_star(cfg, be)
    # fire-and-forget: each non-lead member's securejoin is INITIATED, never waited on
    assert sorted(res[0]["initiated"]) == ["m1", "m2"]
    assert res[0]["pending"] == [] and res[0]["skipped_verified"] == []
    assert be.invites == 2 and len(be.joins) == 2


def test_securejoin_star_idempotent_skips_already_verified():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1", localpart="lead"),
                         BotSpec(id="m1", realm="r1", localpart="m1")],
                 realm_leads={"r1": "lead"})
    # m1 is ALREADY a verified key-contact of the lead → no invite, no securejoin
    be = _star_be({"lead": 10, "m1": 11}, verified={(10, "m1@d.example")})
    res = main.securejoin_star(cfg, be)
    assert res[0]["skipped_verified"] == ["m1"]
    assert res[0]["initiated"] == [] and be.invites == 0 and be.joins == []


def test_securejoin_star_skips_lead_not_onboarded():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1", localpart="lead")],
                 realm_leads={"r1": "lead"})
    be = _star_be({})                              # lead has no account yet
    res = main.securejoin_star(cfg, be)
    assert res[0]["skipped"] == "lead-not-onboarded"
    assert be.invites == 0


def test_securejoin_star_pending_when_member_not_onboarded():
    cfg = Config(mail_domain="d.example", imap_host="m",
                 roster=[BotSpec(id="lead", realm="r1", localpart="lead"),
                         BotSpec(id="m1", realm="r1", localpart="m1")],
                 realm_leads={"r1": "lead"})
    be = _star_be({"lead": 10})                    # m1 not onboarded yet → retried next pass
    res = main.securejoin_star(cfg, be)
    assert res[0]["pending"] == ["m1"]
    assert res[0]["initiated"] == [] and be.invites == 0


# -- event-driven provisioning (a verified-key-contact EVENT adds the member to the channel) --


def _prov_cfg():
    return Config(mail_domain="d.example", imap_host="m",
                  roster=[BotSpec(id="lead", realm="r1", localpart="lead"),
                          BotSpec(id="m1", realm="r1", localpart="m1")],
                  realm_leads={"r1": "lead"})


def test_provision_verified_member_creates_channel_when_missing():
    # verified EVENT for m1 → lead has no channel yet → create it WITH the member
    be = _star_be({"lead": 10, "m1": 11})
    res = main.provision_verified_member(_prov_cfg(), be, 10, "m1@d.example")
    assert res["created"] == 100 and res["added"] == "m1"
    assert be.created == [(10, "r1", ["m1@d.example"])]


def test_provision_verified_member_adds_to_existing_channel():
    be = _star_be({"lead": 10, "m1": 11},
                  channels={10: [{"id": 55, "name": "r1", "members": ["lead"]}]})
    res = main.provision_verified_member(_prov_cfg(), be, 10, "m1@d.example")
    assert res["channel_id"] == 55 and res["added"] == "m1"
    assert be.added == [(10, 55, "m1@d.example")]


def test_provision_verified_member_idempotent_when_already_in_channel():
    be = _star_be({"lead": 10, "m1": 11},
                  channels={10: [{"id": 55, "name": "r1", "members": ["lead", "m1"]}]})
    res = main.provision_verified_member(_prov_cfg(), be, 10, "m1@d.example")
    assert res["already"] == "m1" and be.added == [] and be.created == []


def test_provision_verified_member_ignores_non_member_or_non_led_realm():
    be = _star_be({"lead": 10, "m1": 11})
    # a verified contact that isn't a member of any realm this bot leads → no-op (None)
    assert main.provision_verified_member(_prov_cfg(), be, 10, "stranger@d.example") is None
    assert be.created == [] and be.added == []



# -- env-selectable secret backend (the atomic chatmaild→Stalwart cutover) -------------------


def test_select_secret_backend_env_selectable():
    assert main.select_secret_backend({}) == "local"                          # default (chatmaild)
    assert main.select_secret_backend({"DELTA_SECRET_BACKEND": "opconnect"}) == "opconnect"
    # explicit wins over auto-detect
    assert main.select_secret_backend(
        {"DELTA_SECRET_BACKEND": "local", "OP_CONNECT_URL": "x", "DELTA_BOT_CREDS_ITEM": "y"}) == "local"
    # auto → opconnect only when BOTH op-connect signals present
    assert main.select_secret_backend(
        {"OP_CONNECT_URL": "http://op", "DELTA_BOT_CREDS_ITEM": "creds"}) == "opconnect"
    assert main.select_secret_backend({"OP_CONNECT_URL": "http://op"}) == "local"


def test_both_secret_stores_share_get_or_create_interface():
    # the reconciler/onboard path only calls secrets.get_or_create(bot); both backends satisfy
    # it, so the Stalwart cutover is a store swap with NO reconciler change.
    from app.main import SecretsStore
    from app.opconnect import OpConnectStore
    assert callable(getattr(SecretsStore, "get_or_create", None))
    assert callable(getattr(OpConnectStore, "get_or_create", None))


# -- onboarded-registry + state-loss guard ----------------------------------


class _AcctBackend:
    """Fake backend exposing only account_id_for (what detect_state_loss needs)."""

    def __init__(self, accounts):
        self._acc = accounts   # localpart -> accid (present = configured)

    def account_id_for(self, lp):
        return self._acc.get(lp)


def test_onboard_registry_marks_and_persists(tmp_path):
    path = str(tmp_path / ".dcf-onboarded.json")
    reg = main.OnboardRegistry(path)
    assert reg.known() == set()
    reg.mark("bot-a")
    reg.mark(["bot-b", "bot-a"])          # idempotent + list form
    assert reg.known() == {"bot-a", "bot-b"}
    # durable: a fresh instance on the same path reloads the record
    assert main.OnboardRegistry(path).known() == {"bot-a", "bot-b"}


def test_onboard_registry_default_path_is_roster_sibling(monkeypatch):
    monkeypatch.delenv("DELTA_ONBOARD_REGISTRY", raising=False)
    monkeypatch.setenv("DELTA_ROSTER_PATH", "/config/roster.yaml")
    assert main.OnboardRegistry.default_path(_cfg()) == "/config/.dcf-onboarded.json"
    # env override wins
    monkeypatch.setenv("DELTA_ONBOARD_REGISTRY", "/config/custom.json")
    assert main.OnboardRegistry.default_path(_cfg()) == "/config/custom.json"


def test_detect_state_loss_flags_known_bot_with_missing_account(tmp_path):
    cfg = _cfg("bot-a", "bot-b")
    reg = main.OnboardRegistry(str(tmp_path / "r.json"))
    reg.mark(["bot-a", "bot-b"])          # both onboarded before (durable)
    # bot-a's account is GONE (accounts dir wiped), bot-b survived → only bot-a is state-loss
    lost = main.detect_state_loss(cfg, _AcctBackend({"bot-b": 11}), reg)
    assert lost == ["bot-a"]


def test_detect_state_loss_silent_on_first_boot_and_normal_restart(tmp_path):
    cfg = _cfg("bot-a", "bot-b")
    empty = main.OnboardRegistry(str(tmp_path / "empty.json"))
    # first-ever boot: registry empty → nothing was onboarded before → no alarm
    assert main.detect_state_loss(cfg, _AcctBackend({}), empty) == []
    # normal restart: known bots + their accounts present (durable data) → no alarm
    reg = main.OnboardRegistry(str(tmp_path / "r.json"))
    reg.mark(["bot-a", "bot-b"])
    assert main.detect_state_loss(cfg, _AcctBackend({"bot-a": 10, "bot-b": 11}), reg) == []


@pytest.mark.asyncio
async def test_reconcile_once_records_provisioned_in_registry(tmp_path):
    cfg = _cfg("bot-a", "bot-b")
    secrets = main.SecretsStore(str(tmp_path / "s.json"), cfg.password_min_length)
    reg = main.OnboardRegistry(str(tmp_path / "r.json"))

    async def ok_onboard(localpart, password):
        return localpart == "bot-a"   # only bot-a succeeds

    await main.reconcile_once(cfg, secrets, existing=[], onboard=ok_onboard, registry=reg)
    # only successfully-provisioned bots are recorded (bot-b failed → not marked)
    assert reg.known() == {"bot-a"}
