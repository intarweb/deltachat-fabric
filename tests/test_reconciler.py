from app import reconciler


def test_gen_password_respects_min_and_length():
    assert len(reconciler.gen_password(24)) == 24
    assert len(reconciler.gen_password(4, min_length=9)) == 9   # never below server min
    assert reconciler.gen_password() != reconciler.gen_password()  # random


def test_address_for():
    assert reconciler.address_for("bot-a", "deltachat.example.net") == "bot-a@deltachat.example.net"


def test_valid_username_bounds():
    assert reconciler.valid_username("bot-b", 1, 64) is True
    assert reconciler.valid_username("bot-b", 9, 64) is False       # too short for a min=9 server
    assert reconciler.valid_username("x" * 65, 1, 64) is False


def test_reconcile_diff_provision_and_prune_sorted():
    to_provision, to_prune = reconciler.reconcile(
        desired=["bot-a", "bot-b", "bot-lead"],
        existing=["bot-lead", "bot-c"],
    )
    assert to_provision == ["bot-a", "bot-b"]     # desired − existing, sorted
    assert to_prune == ["bot-c"]                # existing − desired


def test_reconcile_noop_when_aligned():
    assert reconciler.reconcile(["a", "b"], ["b", "a"]) == ([], [])


async def test_ensure_account_uses_injected_login():
    calls = []

    async def fake_login(host, port, user, password):
        calls.append((host, port, user, password))
        return True

    ok = await reconciler.ensure_account("mail.x", 993, "bot-a", "deltachat.example.net",
                                         "pw123456789", _login=fake_login)
    assert ok is True
    assert calls == [("mail.x", 993, "bot-a@deltachat.example.net", "pw123456789")]
