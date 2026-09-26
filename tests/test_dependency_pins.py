"""Guard the version-locked deltachat trio.

`deltachat2` is a third-party binding that deserializes core's JSON-RPC replies into
typed dataclasses (dacite). It supports ONE core release at a time. Core ships breaking
API changes freely, so if the three packages resolve independently an old binder meets a
new core and every reply touching the changed type fails to deserialize — at RUNTIME,
against a live service, with nothing failing at build.

A split resolves to `MissingValueError: missing value for field "is_verified"` → 502 on
/contacts for every bot (core d8912a98 removed the field; the 2.58.0 Contact dataclass
still requires it).

These tests are build-time tripwires for that class of drift, so a bump cannot silently
re-open it.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REQUIREMENTS = REPO_ROOT / "requirements.txt"

TRIO = ("deltachat2", "deltachat-rpc-server", "deltachat-rpc-client")


def _requirements_lines() -> list[str]:
    return [
        line.strip()
        for line in REQUIREMENTS.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _spec(line: str) -> tuple[str, str]:
    """Split a requirement line into (name, version-spec)."""
    m = re.match(r"^([A-Za-z0-9_.\-]+)\s*(.*)$", line)
    assert m, f"unparseable requirement line: {line!r}"
    return m.group(1), m.group(2).strip()


def test_deltachat_trio_all_pinned_with_equality():
    """All three must be `==`-pinned. A range is what let the binder/core pair drift."""
    specs = dict(_spec(line) for line in _requirements_lines())
    for pkg in TRIO:
        assert pkg in specs, f"{pkg} missing from requirements.txt"
        assert specs[pkg].startswith("=="), (
            f"{pkg} must be pinned with '==', got {specs[pkg]!r} — an unbounded range "
            "lets the binding and the core resolve to incompatible versions"
        )


def test_deltachat_trio_versions_are_identical():
    """The trio is version-locked: a split version is the bug this file guards."""
    specs = dict(_spec(line) for line in _requirements_lines())
    versions = {pkg: specs[pkg].removeprefix("==") for pkg in TRIO}
    assert len(set(versions.values())) == 1, (
        f"the deltachat trio must share ONE version, got {versions}"
    )


def test_deltachat_trio_matches_the_installed_binding():
    """If the binding is importable, the core must be the version it supports.

    deltachat2 declares the core it was built against in its own ``full`` extra; this
    asserts the core we pin is that same version. Skipped when the trio is not installed
    (e.g. a docs-only checkout) rather than failing for an unrelated reason.
    """
    try:
        from importlib.metadata import version
    except ImportError:  # pragma: no cover
        return

    installed: dict[str, str] = {}
    for pkg in TRIO:
        try:
            installed[pkg] = version(pkg)
        except Exception:
            return  # trio not installed here — nothing to cross-check

    binder = installed["deltachat2"]
    core = installed["deltachat-rpc-server"]
    assert binder == core, (
        f"deltachat2 {binder} is installed against deltachat-rpc-server {core}. "
        "The binding supports one core release at a time — a mismatch deserializes "
        "into MissingValueError at runtime (e.g. 'is_verified')."
    )
