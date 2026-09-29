"""External members: fleet bots with their OWN Delta account (e.g. a Hermes bot's
deltachat-platform plugin). The relay never onboards or wakes them; the realm lead securejoins
their roster invite link and, once verified, adds them to the realm group."""
import logging

from app.config import BotSpec, Config, ExternalMember
from app.main import desired_localparts, provision_channels, provision_verified_member, securejoin_star
from app.relay import InboundMessage, PeerMesh
from tests.test_relay import FakeBackend, make_relay

DOMAIN = "deltachat.example.net"
GHOST = f"ghost-agent@{DOMAIN}"


def _cfg(invite="https://i.delta.chat/#GHOSTINVITE"):
    return Config(
        mail_domain=DOMAIN, imap_host="m",
        roster=[BotSpec(id="lead", realm="r"), BotSpec(id="bot-a", realm="r")],
        realm_leads={"r": "lead"},
        external=[ExternalMember(id="hermes-ghost", realm="r", address=GHOST, invite=invite)])


class _Backend(FakeBackend):
    def __init__(self, verified=None, channels=None):
        super().__init__(accounts={"lead": 1, "bot-a": 2}, verified=verified,
                         channels=channels)
        self.joined = []
        self.added = []

    def secure_join(self, accid, invite):
        self.joined.append((accid, invite))
        return 99

    def add_member(self, accid, chat_id, addr):
        self.added.append((accid, chat_id, addr))


def test_external_is_never_onboarded():
    assert desired_localparts(_cfg()) == ["lead", "bot-a"]


def test_star_lead_joins_the_external_invite_until_verified():
    be = _Backend()
    res = securejoin_star(_cfg(), be)
    assert (1, "https://i.delta.chat/#GHOSTINVITE") in be.joined     # lead = joiner
    assert "hermes-ghost" in res[0]["initiated"]
    be2 = _Backend(verified={(1, GHOST), (1, f"bot-a@{DOMAIN}")})
    res2 = securejoin_star(_cfg(), be2)
    assert be2.joined == [] and "hermes-ghost" in res2[0]["skipped_verified"]


def test_star_without_invite_is_pending_loudly(caplog):
    with caplog.at_level(logging.WARNING):
        res = securejoin_star(_cfg(invite=""), _Backend())
    assert "hermes-ghost" in res[0]["pending"]
    assert any("no invite link" in r.getMessage() for r in caplog.records)


def test_verified_external_is_added_to_the_realm_group():
    chans = {1: [{"id": 50, "name": "r", "members": ["bot-a"]}]}
    be = _Backend(verified={(1, GHOST)}, channels=chans)
    res = provision_verified_member(_cfg(), be, 1, GHOST)
    assert res == {"realm": "r", "channel_id": 50, "added": "hermes-ghost"}
    assert be.added == [(1, 50, GHOST)]


def test_catch_up_adds_only_verified_externals():
    chans = {1: [{"id": 50, "name": "r", "members": ["bot-a"]}]}
    unverified = _Backend(channels=chans)
    provision_channels(_cfg(), unverified)
    assert unverified.added == []                      # core would refuse a non-key-contact
    verified = _Backend(verified={(1, GHOST)}, channels=chans)
    provision_channels(_cfg(), verified)
    assert verified.added == [(1, 50, GHOST)]


async def test_external_is_never_woken_or_held(tmp_path):
    wakes = []
    relay = make_relay(FakeBackend(accounts={"lead": 1}), [], wakes, tmp_path, config=_cfg())
    msg = InboundMessage(account_id=1, chat_id=50, msg_id=7, text="@ghost-agent hi",
                         is_group=True, members=["lead", "ghost-agent"],
                         mentioned=["ghost-agent"], from_localpart="bot-a")
    assert await relay.handle_inbound(msg) == []
    assert wakes == [] and len(relay.hold) == 0


def test_messages_from_an_external_count_as_bot():
    relay = make_relay(FakeBackend(accounts={}), [], [], "/tmp", config=_cfg())
    assert relay._sender_kind("ghost-agent") == "bot"


def test_send_to_peer_external_sends_only_when_known():
    be = _Backend(verified={(2, GHOST)})
    mesh = PeerMesh(_cfg(), be)
    assert mesh.send_to_peer("bot-a", "hermes-ghost", "hi")["status"] == "sent"
    assert be.sent_to == [(2, GHOST, "hi")]
    res = PeerMesh(_cfg(), _Backend()).send_to_peer("bot-a", GHOST, "hi")
    assert res["status"] == "rejected" and res["reason"] == "external-not-verified"


def test_roster_external_list_loads(tmp_path, monkeypatch):
    p = tmp_path / "roster.yaml"
    p.write_text("bots:\n  - {id: lead, realm: r}\nrealm_leads: {r: lead}\nexternal:\n"
                 f"  - {{id: hermes-ghost, realm: r, address: Ghost-Agent@{DOMAIN}, invite: x}}\n")
    monkeypatch.setenv("DELTA_MAIL_DOMAIN", DOMAIN)
    cfg = Config.load(str(p))
    assert cfg.external == [ExternalMember("hermes-ghost", "r", GHOST, "x")]
    assert [b.id for b in cfg.roster] == ["lead"]
