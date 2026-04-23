"""Heuristic prompt-injection guard for retrieved agent context.

The stateless prompt-builder in the agentic orchestrator retrieves files,
code, and artifacts and packs them into ``<RETRIEVED_CONTEXT>`` blocks. If
retrieved content contains instruction-like text (for example
``"ignore previous instructions"``, ``"you must now run X"``, or a fake
``"system:"`` role), a hostile or careless artifact could try to redirect the
model.

This module provides a deterministic, side-effect-free scan/redact pass over
such blocks. It is **heuristic only**; it is not a security boundary. The
caller (the prompt-builder) decides whether to abort, log, or proceed with
the redacted text.

Rules of engagement
-------------------
- Pure-Python, no I/O, no globals mutated, no logging.
- Same input → same output (``warnings`` ordering is stable).
- No catastrophic regex patterns: every quantifier is bounded or simple.
- Matches are case-insensitive.
- Each match is replaced with the literal token
  ``[REDACTED:injection-pattern]``; surrounding text is preserved.

The warning strings are shaped as ``"<pattern_name> at line <n>"`` so the
orchestrator can emit a stable ``context_injection_detected`` audit event
and, if any warning names a destructive-action pattern, HALT rather than
proceed.
"""

from __future__ import annotations

import re
from typing import Final

# Literal redaction token. Kept short and unambiguous so downstream renderers
# never confuse it with retrieved content.
REDACTION_TOKEN: Final[str] = "[REDACTED:injection-pattern]"

# Pattern names flagged as destructive-action triggers. The orchestrator HALTS
# on any warning whose name appears here (documented in AGENTIC.md; not
# enforced in this module).
DESTRUCTIVE_PATTERN_NAMES: Final[frozenset[str]] = frozenset(
    {"imperative_destructive", "execute_script"}
)


# Each entry is (name, compiled_pattern). Ordering is deterministic and drives
# the iteration order (and therefore the warning-list order for a given
# input). All quantifiers are bounded or applied to simple character classes;
# no nested unbounded ``*``/``+`` to avoid catastrophic backtracking.
_PATTERNS: Final[tuple[tuple[str, "re.Pattern[str]"], ...]] = (
    # "ignore (all )?(previous|prior|above) (instructions|rules|prompts)"
    (
        "ignore_previous",
        re.compile(
            r"ignore(?:\s+all)?\s+(?:previous|prior|above)"
            r"\s+(?:instructions|rules|prompts)",
            re.IGNORECASE,
        ),
    ),
    # "you (must|should|are required to) (now |immediately )?
    #  (run|execute|delete|push|merge|deploy)"
    (
        "imperative_destructive",
        re.compile(
            r"you\s+(?:must|should|are\s+required\s+to)"
            r"(?:\s+now|\s+immediately)?"
            r"\s+(?:run|execute|delete|push|merge|deploy)",
            re.IGNORECASE,
        ),
    ),
    # Fake system role markers embedded in retrieved text.
    (
        "fake_system_role",
        re.compile(r"system(?:\s+prompt)?\s*:", re.IGNORECASE),
    ),
    # XML-ish role tags.
    (
        "system_tag",
        re.compile(r"</?system>", re.IGNORECASE),
    ),
    (
        "assistant_tag",
        re.compile(r"</?assistant>", re.IGNORECASE),
    ),
    # "execute (this|the following) (script|code|command)"
    (
        "execute_script",
        re.compile(
            r"execute\s+(?:this|the\s+following)\s+(?:script|code|command)",
            re.IGNORECASE,
        ),
    ),
    # "as (your|the) (new |updated )?(system|admin|operator)"
    (
        "role_override",
        re.compile(
            r"as\s+(?:your|the)(?:\s+new|\s+updated)?"
            r"\s+(?:system|admin|operator)",
            re.IGNORECASE,
        ),
    ),
    # "forget (everything|all instructions)"
    (
        "forget_everything",
        re.compile(
            r"forget\s+(?:everything|all\s+instructions)",
            re.IGNORECASE,
        ),
    ),
)


def _line_of(text: str, index: int) -> int:
    """Return the 1-based line number in ``text`` for byte-offset ``index``.

    Pure helper; no I/O.
    """
    # Count newlines strictly before the match start.
    return text.count("\n", 0, index) + 1


def sanitize_retrieved(text: str) -> tuple[str, list[str]]:
    """Detect and neutralize common prompt-injection patterns in retrieved context.

    Parameters
    ----------
    text:
        The raw retrieved-context block (may be multi-line).

    Returns
    -------
    tuple[str, list[str]]
        ``(sanitized_text, warnings)``. ``sanitized_text`` has every matched
        pattern replaced with the literal token :data:`REDACTION_TOKEN`;
        surrounding text is preserved. ``warnings`` is a list of strings of
        the form ``"<pattern_name> at line <n>"``, one per match, in a
        deterministic order (pattern order as declared, and for each pattern
        by match position in the input).

    Notes
    -----
    Heuristic only; not a security boundary. The caller decides whether to
    abort, log, or proceed with the redacted text. The function is
    deterministic and has no side effects (no globals mutated, no I/O).
    """
    if not isinstance(text, str):  # pragma: no cover - defensive
        raise TypeError("sanitize_retrieved expects a str")

    warnings: list[str] = []
    # Collect all (start, end, pattern_name) spans, then apply redaction in a
    # single pass to keep offsets meaningful and behavior deterministic.
    spans: list[tuple[int, int, str]] = []
    for name, pattern in _PATTERNS:
        for match in pattern.finditer(text):
            spans.append((match.start(), match.end(), name))

    if not spans:
        return text, warnings

    # Sort by start offset, then by end offset (longer match wins on a tie).
    # Pattern-name order is preserved via a stable secondary sort below.
    spans.sort(key=lambda s: (s[0], -s[1]))

    # Merge/suppress overlaps: prefer the earliest-starting, longest span.
    # If two patterns match the same region, keep the first by declaration
    # order (which we recover by looking up the pattern index).
    pattern_order = {name: i for i, (name, _) in enumerate(_PATTERNS)}

    merged: list[tuple[int, int, str]] = []
    for start, end, name in spans:
        if merged and start < merged[-1][1]:
            # Overlap with previous kept span. Keep the one whose pattern
            # comes first in declaration order; on tie, the longer span.
            p_start, p_end, p_name = merged[-1]
            if (pattern_order[name], -(end - start)) < (
                pattern_order[p_name],
                -(p_end - p_start),
            ):
                merged[-1] = (start, max(end, p_end), name)
            # else: drop this span.
            continue
        merged.append((start, end, name))

    # Emit warnings in deterministic order: by line number, then by start
    # offset, then by declared pattern order.
    for start, _end, name in sorted(
        merged, key=lambda s: (_line_of(text, s[0]), s[0], pattern_order[s[2]])
    ):
        warnings.append(f"{name} at line {_line_of(text, start)}")

    # Apply redactions right-to-left so earlier offsets stay valid.
    out = text
    for start, end, _name in sorted(merged, key=lambda s: s[0], reverse=True):
        out = out[:start] + REDACTION_TOKEN + out[end:]

    return out, warnings


__all__ = [
    "REDACTION_TOKEN",
    "DESTRUCTIVE_PATTERN_NAMES",
    "sanitize_retrieved",
]
