from app.routing import wake_targets


def test_mention_wakes_only_mentioned_members():
    assert wake_targets(["bot-a"], ["bot-a", "bot-b", "bot-lead"], "bot-lead") == ["bot-a"]


def test_mention_filters_non_members_and_dedupes():
    assert wake_targets(["bot-a", "bot-a", "bot-c"], ["bot-a", "bot-b"], "bot-b") == ["bot-a"]


def test_no_mention_wakes_only_channel_main():
    assert wake_targets([], ["bot-a", "bot-b", "bot-lead"], "bot-lead") == ["bot-lead"]


def test_no_mention_never_wakes_all():
    # the anti-thundering-herd guarantee: unaddressed msg must NOT fan out to everyone
    out = wake_targets([], ["a", "b", "c", "d"], "a")
    assert out == ["a"]
    assert len(out) < 4


def test_no_mention_no_valid_main_wakes_nobody():
    assert wake_targets([], ["a", "b"], "bot-c") == []
    assert wake_targets([], ["a", "b"], None) == []


def test_wake_targets_case_insensitive_returns_canonical():
    # mention-case-sensitivity-residual: differently-cased mention/member lists must
    # still resolve, returning the canonical member spelling. Before the fix the raw
    # `m in members` set-membership dropped a capitalized mention = silent deafness.
    assert wake_targets(["Bot-A"], ["bot-a", "bot-b"], "bot-b") == ["bot-a"]
    assert wake_targets(["BOT-A", "bot-b"], ["bot-a", "bot-b"], None) == ["bot-a", "bot-b"]
    # channel_main matched case-insensitively too
    assert wake_targets([], ["bot-a", "bot-lead"], "Bot-Lead") == ["bot-lead"]
