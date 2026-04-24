"""Stateless prompt-builder integration point for retrieved agent context.

The agentic orchestrator's retrieval stage gathers files, code, and artifacts
from the working tree and packs them into ``<CONTEXT: label>`` blocks that
feed a stateless worker prompt. This module is the wiring that every
retrieved block must flow through on its way to prompt assembly:

1. Run :func:`totali.agents.context_sanitizer.sanitize_retrieved` over the
   raw text (heuristic prompt-injection scrub).
2. Emit a ``context_injection_detected`` audit event for any block that
   triggered one or more warnings.
3. Halt the build if any warning names a destructive-action pattern (as
   defined by :data:`DESTRUCTIVE_PATTERN_NAMES`) — a retrieved artifact
   tried to smuggle a delete/push/deploy/merge directive or a role
   override into the prompt, and proceeding is not safe.

The module is deterministic and has no I/O beyond optional audit emission.
It does not import :class:`AuditLogger` at runtime; the audit object is
accepted by duck-type (anything with ``.log(event_type, data)``) and its
concrete class is only named under ``TYPE_CHECKING``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from totali.agents.context_sanitizer import sanitize_retrieved

if TYPE_CHECKING:  # pragma: no cover - typing only
    from totali.audit.logger import AuditLogger


class InjectionHaltError(RuntimeError):
    """Raised when retrieved context contains destructive-action patterns.

    The stateless-worker orchestrator HALTS rather than proceeds when this
    fires — a retrieved artifact tried to smuggle delete/push/deploy/merge
    instructions into the prompt.
    """


# Pattern names (emitted by ``context_sanitizer.sanitize_retrieved``) that
# indicate destructive intent. Matching any of these in a block's warnings
# triggers HALT unless ``halt_on_destructive=False`` is passed.
#
# Note: this set is the prompt-builder's policy on what counts as
# destructive for the purpose of halting assembly. It is a superset-capable
# allowlist that may differ from the sanitizer's own
# ``DESTRUCTIVE_PATTERN_NAMES`` constant (which is advisory to callers).
# The drift test in ``tests/test_prompt_builder.py`` pins this set to names
# the sanitizer actually emits.
DESTRUCTIVE_PATTERN_NAMES: frozenset[str] = frozenset(
    {
        "imperative_destructive",  # "you must run/execute/delete/push/merge/deploy"
        "execute_script",  # "execute this script/code/command"
        "role_override",  # "as your new system/admin/operator"
        "forget_everything",  # "forget everything / all instructions"
    }
)


def _pattern_names(warnings: list[str]) -> list[str]:
    """Extract pattern names from sanitizer warning strings.

    The sanitizer emits ``"<pattern_name> at line <n>"``. This helper
    splits on the first space and keeps the head.
    """
    names: list[str] = []
    for w in warnings:
        head, _, _ = w.partition(" ")
        if head:
            names.append(head)
    return names


def _destructive_hits(pattern_names: list[str]) -> list[str]:
    """Return the ordered subset of pattern names that are destructive.

    Preserves first occurrence in input order so callers get stable
    diagnostics when multiple destructive patterns fire in one block.
    """
    seen: set[str] = set()
    hits: list[str] = []
    for name in pattern_names:
        if name in DESTRUCTIVE_PATTERN_NAMES and name not in seen:
            seen.add(name)
            hits.append(name)
    return hits


def build_context_block(
    label: str,
    text: str,
    *,
    audit: "AuditLogger | None" = None,
    halt_on_destructive: bool = True,
) -> tuple[str, list[str]]:
    """Sanitize ``text`` and package it for the prompt capsule.

    Calls :func:`sanitize_retrieved`. If the sanitizer returns any warnings
    and an audit logger is provided, emits a ``context_injection_detected``
    event carrying the ``label``, the list of ``pattern_names``, the
    ``warning_count``, and a ``halt`` flag. If any warning names a
    destructive pattern (see :data:`DESTRUCTIVE_PATTERN_NAMES`) and
    ``halt_on_destructive`` is true, raises :class:`InjectionHaltError`
    naming the matching patterns.

    Parameters
    ----------
    label:
        Short identifier for the block's origin (filename, artifact id).
        Prefixed onto the sanitized text as ``[CONTEXT: <label>]`` so
        downstream assembly can identify provenance.
    text:
        Raw retrieved content. May be multi-line.
    audit:
        Optional audit logger. Must support ``log(event_type, data)``.
    halt_on_destructive:
        When true (default), a destructive-pattern hit raises
        :class:`InjectionHaltError` and no block is returned.

    Returns
    -------
    tuple[str, list[str]]
        ``(block_text, warnings)`` — the header-prefixed sanitized text
        and the sanitizer's warning list.

    Raises
    ------
    InjectionHaltError
        When ``halt_on_destructive`` is true and a destructive pattern
        was detected.

    Notes
    -----
    Deterministic; the only side effect is ``audit.log`` when an audit
    logger is passed and warnings were raised.
    """
    sanitized, warnings = sanitize_retrieved(text)
    names = _pattern_names(warnings)
    destructive = _destructive_hits(names)
    will_halt = bool(destructive) and halt_on_destructive

    if warnings and audit is not None:
        audit.log(
            "context_injection_detected",
            {
                "block_label": label,
                "pattern_names": names,
                "warning_count": len(warnings),
                "halt": will_halt,
            },
        )

    if will_halt:
        raise InjectionHaltError(
            f"destructive injection pattern(s) in retrieved block "
            f"{label!r}: {destructive}"
        )

    block_text = f"[CONTEXT: {label}]\n{sanitized}"
    return block_text, warnings


def assemble_prompt_capsule(
    blocks: list[tuple[str, str]],
    *,
    audit: "AuditLogger | None" = None,
    halt_on_destructive: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Sanitize each block and join into a single prompt-capsule string.

    Each input is a ``(label, raw_text)`` pair. Every block flows through
    :func:`build_context_block`, so halting on destructive patterns and
    audit emission follow the same rules as the single-block path.

    Parameters
    ----------
    blocks:
        Ordered list of ``(label, raw_text)`` pairs. Order is preserved in
        the output capsule.
    audit:
        Optional audit logger forwarded to each per-block call.
    halt_on_destructive:
        When true (default), the first destructive-pattern block aborts
        assembly with :class:`InjectionHaltError`. Preceding audit events
        have already been emitted; no partial capsule is returned.

    Returns
    -------
    tuple[str, dict[str, Any]]
        ``(capsule_text, diagnostics)`` where ``diagnostics`` contains:

        - ``block_count``: number of input blocks.
        - ``warnings_by_block``: dict mapping label to its warning list
          (only entries where warnings were non-empty).
        - ``total_warnings``: sum of warnings across all blocks.

    Raises
    ------
    InjectionHaltError
        Propagated from :func:`build_context_block` when a destructive
        pattern fires and ``halt_on_destructive`` is true.
    """
    rendered: list[str] = []
    warnings_by_block: dict[str, list[str]] = {}
    total_warnings = 0

    for label, raw in blocks:
        block_text, warnings = build_context_block(
            label,
            raw,
            audit=audit,
            halt_on_destructive=halt_on_destructive,
        )
        rendered.append(block_text)
        if warnings:
            warnings_by_block.setdefault(label, []).extend(warnings)
            total_warnings += len(warnings)

    capsule_text = "\n\n".join(rendered)
    diagnostics: dict[str, Any] = {
        "block_count": len(blocks),
        "warnings_by_block": warnings_by_block,
        "total_warnings": total_warnings,
    }
    return capsule_text, diagnostics


__all__ = [
    "DESTRUCTIVE_PATTERN_NAMES",
    "InjectionHaltError",
    "assemble_prompt_capsule",
    "build_context_block",
]
