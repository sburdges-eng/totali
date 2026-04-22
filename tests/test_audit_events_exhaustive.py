"""Regression net: audit.log_events in config covers every literal event name
emitted by the codebase.

Scans all .py files under totali/ for `audit.log("event_name"` patterns,
compares to the config allowlist. A new event added without a config entry
fails this test loudly (rather than silently slipping into the chain-of-custody
record or — worse — being rejected by the A-5 allowlist guard at runtime).
"""

import re
from pathlib import Path

import yaml


SRC_ROOT = Path(__file__).resolve().parents[1] / "totali"
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"

# Matches: audit.log("foo"  or  self.audit.log("foo"  or  _audit.log("foo"
# Captures the event-name literal. Ignores f-strings (patterns where the event
# name is computed) — those are caller-specific and not allowlistable here.
EVENT_RE = re.compile(r"""audit\.log\(\s*(?:f)?["']([a-z_]+)["']""")


def _emitted_events() -> set[str]:
    events: set[str] = set()
    for py in SRC_ROOT.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        for match in EVENT_RE.finditer(py.read_text("utf-8", errors="ignore")):
            events.add(match.group(1))
    return events


def _allowed_events() -> set[str]:
    cfg = yaml.safe_load(CONFIG_PATH.read_text("utf-8"))
    return set(cfg.get("audit", {}).get("log_events", []))


class TestAllowlistExhaustive:
    def test_every_emitted_event_is_allowlisted(self):
        emitted = _emitted_events()
        allowed = _allowed_events()
        missing = sorted(emitted - allowed)
        assert not missing, (
            f"Events emitted by code but absent from config.audit.log_events: "
            f"{missing}. Add them to config/pipeline.yaml or remove the emit."
        )

    def test_allowlist_is_nonempty(self):
        assert _allowed_events(), "config.audit.log_events must not be empty"

    def test_allowlist_has_core_events(self):
        allowed = _allowed_events()
        # Spot-check a handful of phase-critical events.
        for core in ("ingest", "classify", "extract", "heal", "run_end"):
            assert core in allowed, f"{core!r} missing from allowlist"
