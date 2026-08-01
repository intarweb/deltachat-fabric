"""Wake-routing — the 🔴 anti-thundering-herd hard rule.

Inbound group message → wake ONLY the right bot(s):
  * @mentions present  → wake only the mentioned bots (that are channel members)
  * no mention         → wake NOBODY (untagged channel traffic never wakes;
                         only @mentions + 1:1 DMs wake)
  * NEVER wake all members on an unaddressed message.
"""
from __future__ import annotations


def wake_targets(mentioned: list[str], channel_members: list[str],
                 channel_main: str | None) -> list[str]:
    """Return the bot ids to wake for one inbound message. Never returns all members
    on a no-mention message, and no longer wakes the channel main on an unaddressed
    message: untagged channel traffic wakes NOBODY (only @mentions + 1:1 DMs wake).

    CASE-INSENSITIVE membership: mentions are matched to members by lowercased
    localpart and the CANONICAL member spelling is returned, so a caller that passes
    differently-cased mention/member lists still resolves (PR#37 fixed extract_mentions;
    this closes the same trap at the routing boundary). ``channel_main`` is retained in
    the signature for call-site compatibility but is no longer used to wake.
    """
    by_lower = {m.lower(): m for m in channel_members}
    if mentioned:
        # only mentioned bots that are actually in this channel, de-duped + ordered,
        # returning the canonical member spelling
        seen, out = set(), []
        for m in mentioned:
            canon = by_lower.get(m.lower())
            if canon is not None and canon not in seen:
                seen.add(canon)
                out.append(canon)
        return out
    # unaddressed → wake NOBODY (untagged channel traffic never wakes)
    return []
