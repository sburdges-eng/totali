"""Detect pipeline input kind from file extension."""

from __future__ import annotations

from pathlib import Path

_CODED_SURVEY_SUFFIXES = frozenset({".asc", ".crd", ".txt"})
_LAS_SUFFIXES = frozenset({".las", ".laz"})


def is_coded_survey_input(path: str | Path) -> bool:
    return Path(path).suffix.lower() in _CODED_SURVEY_SUFFIXES


def is_las_input(path: str | Path) -> bool:
    return Path(path).suffix.lower() in _LAS_SUFFIXES


def input_kind(path: str | Path) -> str:
    """Return ``las`` or ``coded_survey``; raises ValueError for unknown types."""
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix in _LAS_SUFFIXES:
        return "las"
    if suffix in _CODED_SURVEY_SUFFIXES:
        return "coded_survey"
    raise ValueError(
        f"unsupported input extension {suffix!r} on {p.name!r}; "
        f"expected one of {sorted(_LAS_SUFFIXES | _CODED_SURVEY_SUFFIXES)}"
    )
