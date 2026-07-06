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
    on a no-mention message (that's the thundering-herd meltdown we forbid)."""
    members = set(channel_members)
    if mentioned:
        # only mentioned bots that are actually in this channel, de-duped + ordered
        seen, out = set(), []
        for m in mentioned:
            if m in members and m not in seen:
                seen.add(m)
                out.append(m)
        return out
    # unaddressed → just the channel main (if it's a member); never everyone
    if channel_main and channel_main in members:
        return [channel_main]
    return []
