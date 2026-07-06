import textwrap

from app.config import Config


def test_load_parses_roster_and_defaults(tmp_path, monkeypatch):
    roster = tmp_path / "roster.yaml"
    roster.write_text(textwrap.dedent("""
        bots:
          - id: bot-lead
            realm: realm-a
          - id: bot-a
            realm: realm-a
            localpart: bot-a-bot
          - bot-e
        realm_leads:
          realm-a: bot-lead
    """))
    monkeypatch.setenv("DELTA_MAIL_DOMAIN", "deltachat.example.net")
    monkeypatch.setenv("DELTA_USERNAME_MIN_LENGTH", "1")

    cfg = Config.load(roster_path=str(roster))
    assert cfg.mail_domain == "deltachat.example.net"
    ids = {b.id: b for b in cfg.roster}
    assert set(ids) == {"bot-lead", "bot-a", "bot-e"}
    assert ids["bot-lead"].localpart == "bot-lead"       # defaults to id
    assert ids["bot-a"].localpart == "bot-a-bot"          # explicit override honored
    assert ids["bot-e"].realm == "default"              # bare-string entry → default realm
    assert cfg.realm_leads["realm-a"] == "bot-lead"


def test_load_no_roster_file_is_empty_not_error(tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_MAIL_DOMAIN", "d.example")
    cfg = Config.load(roster_path=str(tmp_path / "nope.yaml"))
    assert cfg.roster == []
    assert cfg.mail_domain == "d.example"    # generic engine: domain injected, nothing baked
