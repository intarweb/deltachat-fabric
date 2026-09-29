"""Accounts whose bot left the roster stay in the core but DORMANT (no IO)."""
from app.config import BotSpec, Config
from app.relay import DeltaChatBackend
from tests.test_relay import _FakeRpc

DOMAIN = "deltachat.example.net"


def test_io_starts_only_for_roster_accounts(tmp_path):
    cfg = Config(mail_domain=DOMAIN, imap_host="m",
                 roster=[BotSpec(id="lead", realm="r"), BotSpec(id="b", realm="r")])
    rpc = _FakeRpc()
    for lp in ("lead", "b", "retired"):
        accid = rpc.add_account()
        rpc.accounts[accid] = f"{lp}@{DOMAIN}"
    rpc.start_io = lambda accid: rpc.started.append(accid)
    be = DeltaChatBackend(cfg, str(tmp_path), _rpc=rpc)
    started = be._start_io_for_roster()
    assert sorted(started) == sorted([be.account_id_for("lead"), be.account_id_for("b")])
    assert be.account_id_for("retired") not in rpc.started
    assert be.account_id_for("retired") is not None      # account kept (recoverable)


def test_empty_roster_starts_every_account(tmp_path):
    rpc = _FakeRpc()
    for lp in ("x", "y"):
        rpc.accounts[rpc.add_account()] = f"{lp}@{DOMAIN}"
    rpc.start_io = lambda accid: rpc.started.append(accid)
    be = DeltaChatBackend(Config(mail_domain=DOMAIN, imap_host="m"), str(tmp_path), _rpc=rpc)
    assert sorted(be._start_io_for_roster()) == [1, 2]
