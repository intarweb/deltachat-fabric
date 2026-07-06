from datetime import datetime, timezone

from app import backup
from app.config import BotSpec, Config


class FakeBackupBackend:
    """In-memory backup backend: known accounts + records export_backup calls."""

    def __init__(self, accounts: dict[str, int], produce=True):
        self._accounts = accounts
        self.produce = produce
        self.calls: list[tuple[int, str]] = []

    def account_id_for(self, localpart):
        return self._accounts.get(localpart)

    def export_backup(self, account_id, folder):
        self.calls.append((account_id, folder))
        if self.produce:
            # emulate deltachat imex writing one tar into the folder
            from pathlib import Path
            (Path(folder) / "delta-backup.tar").write_text("BACKUP")


def _cfg(*localparts):
    return Config(
        mail_domain="d.example", imap_host="mail.d.example",
        roster=[BotSpec(id=lp) for lp in localparts],
    )


def test_accounts_to_backup_only_live_accounts_dedup_order():
    cfg = Config(
        mail_domain="d.example", imap_host="m",
        roster=[BotSpec(id="bot-a"), BotSpec(id="bot-b"), BotSpec(id="bot-c"),
                BotSpec(id="bot-a")],  # dup
    )
    backend = FakeBackupBackend({"bot-a": 1, "bot-b": 2})  # bot-c has no account
    assert backup.accounts_to_backup(cfg, backend) == ["bot-a", "bot-b"]


def test_prune_keeps_n_newest_per_account():
    files = [
        "/b/bot-a-20260101T000000Z.tar",
        "/b/bot-a-20260102T000000Z.tar",
        "/b/bot-a-20260103T000000Z.tar",
        "/b/bot-b-20260101T000000Z.tar",
        "/b/unrelated-file.txt",           # not ours — never touched
    ]
    to_delete = backup.prune_old_backups(files, retain=2)
    # only the oldest bot-a goes; bot-b (1 file) stays; unrelated untouched
    assert to_delete == ["/b/bot-a-20260101T000000Z.tar"]


def test_prune_retain_zero_deletes_all_recognized():
    files = ["/b/bot-a-20260101T000000Z.tar", "/b/skip.md"]
    assert backup.prune_old_backups(files, retain=0) == ["/b/bot-a-20260101T000000Z.tar"]


def test_prune_ignores_unrecognized_names():
    assert backup.prune_old_backups(["/b/random.tar", "/b/no-stamp-here.tar"], retain=1) == []


def test_next_backup_delay_scheduling():
    assert backup.next_backup_delay(3600, None, 1000) == 0.0        # never run → now
    assert backup.next_backup_delay(3600, 1000, 1000 + 3600) == 0.0  # elapsed → now
    assert backup.next_backup_delay(3600, 1000, 1000 + 100) == 3500  # remaining
    assert backup.next_backup_delay(3600, 1000, 1000 + 9999) == 0.0  # never negative


def test_run_backup_exports_and_names_then_prunes(tmp_path):
    cfg = _cfg("bot-a", "bot-b")
    backend = FakeBackupBackend({"bot-a": 1, "bot-b": 2})
    now = datetime(2026, 1, 5, 12, 0, 0, tzinfo=timezone.utc)
    res = backup.run_backup(cfg, backend, str(tmp_path), retain=7, now=now)

    assert sorted(backend.calls) == [(1, str(tmp_path / ".staging-bot-a-20260105T120000Z")),
                                     (2, str(tmp_path / ".staging-bot-b-20260105T120000Z"))]
    assert sorted(res["exported"]) == [
        str(tmp_path / "bot-a-20260105T120000Z.tar"),
        str(tmp_path / "bot-b-20260105T120000Z.tar"),
    ]
    assert res["errors"] == {}
    # produced files renamed to our scheme; staging dirs cleaned up
    got = {p.name for p in tmp_path.iterdir()}
    assert got == {"bot-a-20260105T120000Z.tar", "bot-b-20260105T120000Z.tar"}


def test_run_backup_prunes_old_across_runs(tmp_path):
    cfg = _cfg("bot-a")
    backend = FakeBackupBackend({"bot-a": 1})
    # run 3 times at increasing timestamps, retain=2 → oldest pruned
    for day in (1, 2, 3):
        now = datetime(2026, 1, day, 0, 0, 0, tzinfo=timezone.utc)
        backup.run_backup(cfg, backend, str(tmp_path), retain=2, now=now)
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["bot-a-20260102T000000Z.tar", "bot-a-20260103T000000Z.tar"]


def test_run_backup_records_error_when_no_file_produced(tmp_path):
    cfg = _cfg("bot-a")
    backend = FakeBackupBackend({"bot-a": 1}, produce=False)
    res = backup.run_backup(cfg, backend, str(tmp_path), retain=7)
    assert res["exported"] == []
    assert "bot-a" in res["errors"]


def test_list_backup_files_only_ours(tmp_path):
    (tmp_path / "bot-a-20260101T000000Z.tar").write_text("x")
    (tmp_path / "notes.md").write_text("x")
    files = backup.list_backup_files(str(tmp_path))
    assert [f.rsplit("/", 1)[-1] for f in files] == ["bot-a-20260101T000000Z.tar"]
