"""Wake-routing — the 🔴 anti-thundering-herd hard rule.

Inbound group message → wake ONLY the right bot(s):
  * @mentions present  → wake only the mentioned bots (that are channel members)
  * no mention         → wake ONLY the channel's "main" (realm lead)
  * NEVER wake all members on an unaddressed message.
"""
from __future__ import annotations


def wake_targets(mentioned: list[str], channel_members: list[str],
                 channel_main: str | None) -> list[str]:
    """Return the bot ids to wake for one inbound message. Never returns all members
    on a no-mention message (that's the thundering-herd meltdown we forbid).

    CASE-INSENSITIVE membership: mentions are matched to members by lowercased
    localpart and the CANONICAL member spelling is returned, so a caller that passes
    differently-cased mention/member lists still resolves (PR#37 fixed extract_mentions;
    this closes the same trap at the routing boundary). channel_main is likewise matched
    case-insensitively.
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
    # unaddressed → just the channel main (if it's a member); never everyone
    if channel_main is not None:
        canon = by_lower.get(channel_main.lower())
        if canon is not None:
            return [canon]
    return []
