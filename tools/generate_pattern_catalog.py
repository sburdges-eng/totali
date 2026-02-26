#!/usr/bin/env python3
"""
Pattern Catalog generator for extracted snippet archive.
Produces a rebuild/redesign-friendly catalog of reusable patterns.
Read-only. No code execution. Deterministic.
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

# --- CONFIG ---
DEFAULT_SNIPPET_ROOT = "extracted_snippets_v2"
OUTPUT_DIR: str = ""  # Set to SNIPPET_ROOT/reports if empty
MAX_PREVIEW_LINES: int = 12
LANGS_TO_PARSE: tuple[str, ...] = (
    "py", "python", "js", "jsx", "ts", "tsx", "rs", "rust",
    "cpp", "c", "hpp", "h", "json", "yml", "yaml", "toml", "sh", "bash", "sql",
)

# Pattern definitions: (tag, definition, keywords list)
PATTERNS = [
    ("State_Machine", "Finite state machines, job/task lifecycle, scheduling states.", [
        "State", "NotScheduled", "Scheduled", "Running", "Completed",
        "Failed", "Skipped", "transition", "RRULE",
    ]),
    ("Result_Value_Wrappers", "Result/Either/Option types, value-present/absent wrappers.", [
        "ValueState", "ValuePresent", "ValueMissing", "ValueInvalid",
        "Result", "Either", "Option", "MissingReason", "ErrorDetail",
    ]),
    ("Adapters_Translators", "Data format translation, IR mapping, serialize/deserialize.", [
        "adapter", "to_ir", "from_ir", "serialize", "deserialize",
        "map_", "convert",
    ]),
    ("Contracts_Schemas", "Data structures, type contracts, schema validation.", [
        "dataclass", "pydantic", "schema", "struct", "interface",
        "typing", "Protocol", "jsonschema",
    ]),
    ("Invariants_Guards", "Assertions, runtime checks, guard clauses, integrity constraints.", [
        "assert", "invariant", "guard", "must", "raise",
        "RuntimeError", "Violation", "stop-gradient", "no_grad", "integrity",
    ]),
    ("Orchestration_Routing", "Workflow coordination, dispatch, routing, pipeline phases.", [
        "orchestrator", "dispatch", "route", "gate", "phase",
        "pipeline", "fallback",
    ]),
    ("Realtime_Safety", "Audio-thread safety, lock-free, no allocation in hot path.", [
        "RT-safe", "audio thread", "no locks", "atomics",
        "processBlock", "no allocation",
    ]),
    ("Training_ML_Safety", "Checkpointing, manifest, EMA, diversity/variance in training.", [
        "checkpoint", "manifest", "ema", "collapse", "diversity",
        "variance", "loss", "dataset",
    ]),
    ("IO_Bridges", "FFI, language bindings, OSC, gRPC, socket interfaces.", [
        "FFI", "bridge", "bindings", "pybind", "OSC",
        "grpc", "socket",
    ]),
    ("UI_Controls", "Knobs, sliders, look-and-feel, panels, meters, UI state.", [
        "Knob", "Slider", "LookAndFeel", "UI", "panel",
        "meter", "Zustand",
    ]),
]


def _parse_table_line(line: str) -> list[str]:
    """Parse markdown table row. Handle escaped pipes in cells."""
    if not line.strip().startswith("|"):
        return []
    placeholder = "\x00PIPE\x00"
    line = line.replace("\\|", placeholder)
    parts = [p.strip().replace(placeholder, "|") for p in line.split("|")]
    if len(parts) >= 2:
        return parts[1:-1]
    return []


def _parse_snippet_index(index_path: Path) -> list[dict]:
    """Parse SNIPPET_INDEX.md into list of row dicts."""
    text = index_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    rows = []
    headers = None
    for line in lines:
        cells = _parse_table_line(line)
        if not cells:
            continue
        if headers is None and cells[0] == "snippet_title":
            headers = cells
            continue
        if headers and len(cells) >= len(headers):
            row = {}
            for i, h in enumerate(headers):
                row[h] = cells[i] if i < len(cells) else ""
            rows.append(row)
    return rows


def _matches_pattern(content: str, keywords: list[str]) -> int:
    """Return count of keyword matches (case-insensitive)."""
    lower = content.lower()
    count = 0
    for kw in keywords:
        if kw.lower() in lower:
            count += 1
    return count


def _classify_snippet(content: str, filename: str) -> list[str]:
    """Return list of pattern tags that match."""
    combined = content + " " + filename
    tags = []
    for item in PATTERNS:
        tag = item[0]
        keywords = item[2]
        if _matches_pattern(combined, keywords) > 0:
            tags.append(tag)
    return tags


def _confidence(status: str, tag_count: int) -> str:
    """HIGH if RECONSTRUCTED and multiple tag matches, MED if RECONSTRUCTED with one, LOW if LOOSE."""
    if status == "RECONSTRUCTED":
        return "HIGH" if tag_count >= 2 else "MED"
    if status == "CONFLICT":
        return "MED"
    return "LOW"


def _sha256_12(sha: str) -> str:
    return sha[:12] if sha and len(sha) >= 12 else sha or ""


def _line_range(source_md: str, start: str, end: str) -> str:
    if source_md and start and end:
        return f"{source_md} L{start}-{end}"
    if source_md and start:
        return f"{source_md} L{start}"
    return source_md or ""


def _excerpt(lines: list[str], max_lines: int) -> str:
    """Return first max_lines as string with line numbers."""
    out = []
    for i, line in enumerate(lines[:max_lines], 1):
        out.append(f"  {i:3d}| {line.rstrip()}")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Pattern Catalog from extracted snippets")
    parser.add_argument(
        "--snippet-root",
        default=DEFAULT_SNIPPET_ROOT,
        help=f"Path to extracted_snippets_v2 (default: {DEFAULT_SNIPPET_ROOT})",
    )
    args = parser.parse_args()
    root = Path(args.snippet_root).resolve()
    out_dir = Path(OUTPUT_DIR).resolve() if OUTPUT_DIR else root / "reports"
    index_path = root / "reports" / "SNIPPET_INDEX.md"

    if not root.exists():
        print(f"Error: SNIPPET_ROOT does not exist: {root}", file=sys.stderr)
        return 1
    if not index_path.exists():
        print(f"Error: SNIPPET_INDEX.md not found: {index_path}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _parse_snippet_index(index_path)
    processed = [r for r in rows if r.get("status") in ("RECONSTRUCTED", "CONFLICT", "LOOSE")]
    processed = [r for r in processed if r.get("output_file")]

    tag_to_entries: dict[str, list[dict]] = defaultdict(list)
    sha_to_locations: dict[str, list[dict]] = defaultdict(list)
    conflicts_by_path: dict[str, list[dict]] = defaultdict(list)
    skipped = 0
    skipped_missing: list[str] = []

    for row in processed:
        output_file = row.get("output_file", "").strip()
        if not output_file:
            continue
        file_path = root / output_file
        status = row.get("status", "")
        title = row.get("snippet_title", "")
        lang = row.get("language", "")
        path_val = row.get("path", "")
        sha = row.get("sha256", "")
        source_md = row.get("source_md", "")
        start_line = row.get("start_line", "")
        end_line = row.get("end_line", "")

        if status == "CONFLICT" and path_val:
            conflicts_by_path[path_val].append({
                "sha256": sha,
                "source_md": source_md,
                "start_line": start_line,
                "output_file": output_file,
            })

        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            skipped += 1
            skipped_missing.append(output_file)
            continue

        norm_lang = (lang or "").strip().lower()
        if not norm_lang and output_file:
            m = re.search(r"by_lang/([^/]+)/", output_file)
            if m:
                norm_lang = m.group(1).lower()
        if norm_lang and norm_lang not in LANGS_TO_PARSE:
            continue

        tags = _classify_snippet(content, str(file_path.name))
        if not tags:
            continue

        sha_12 = _sha256_12(sha)
        line_range = _line_range(source_md, str(start_line), str(end_line))
        conf = _confidence(status, len(tags))

        entry = {
            "title": title,
            "status": status,
            "language": lang,
            "reconstructed_path": path_val if status in ("RECONSTRUCTED", "CONFLICT") else "",
            "file_link": output_file,
            "sha256_12": sha_12,
            "sha256": sha,
            "source_md": source_md,
            "start_line": start_line,
            "end_line": end_line,
            "line_range": line_range,
            "confidence": conf,
            "tags": tags,
            "content": content,
        }
        for tag in tags:
            tag_to_entries[tag].append(entry)
        sha_to_locations[sha].append(entry)

    # Sort entries: by tag, then confidence (HIGH>MED>LOW), then title, then sha256
    conf_order = {"HIGH": 0, "MED": 1, "LOW": 2}

    for item in PATTERNS:
        tag = item[0]
        if tag in tag_to_entries:
            tag_to_entries[tag].sort(
                key=lambda e: (conf_order.get(e["confidence"], 3), e["title"], e["sha256"])
            )

    # --- Write PATTERN_CATALOG.md ---
    catalog_path = out_dir / "PATTERN_CATALOG.md"
    with open(catalog_path, "w", encoding="utf-8") as f:
        f.write("# Pattern Catalog\n\n")
        f.write("## Overview\n\n")
        f.write(f"- total snippets processed: {len(processed)}\n")
        recon_count = sum(1 for r in processed if r.get("status") == "RECONSTRUCTED")
        loose_count = sum(1 for r in processed if r.get("status") == "LOOSE")
        conflict_count = sum(1 for r in processed if r.get("status") == "CONFLICT")
        f.write(f"- reconstructed: {recon_count}\n")
        f.write(f"- loose: {loose_count}\n")
        f.write(f"- conflicts: {conflict_count}\n")
        f.write(f"- skipped (missing file): {skipped}\n")
        if skipped_missing:
            f.write(f"- SKIPPED_MISSING_FILE: {len(skipped_missing)} files\n")
            for p in skipped_missing[:30]:
                f.write(f"  - {p}\n")
            if len(skipped_missing) > 30:
                f.write(f"  - ... and {len(skipped_missing) - 30} more\n")
        f.write("\n### Tag distribution\n\n")
        for item in PATTERNS:
            tag = item[0]
            count = len(tag_to_entries.get(tag, []))
            f.write(f"- {tag}: {count}\n")
        f.write("\n---\n\n")

        for item in PATTERNS:
            tag, definition, keywords = item[0], item[1], item[2]
            entries = tag_to_entries.get(tag, [])
            if not entries:
                continue
            f.write(f"## {tag}\n\n")
            f.write(f"*{definition}*\n\n")
            kw_str = ", ".join(keywords[:5]) + ("..." if len(keywords) > 5 else "")
            f.write(f"*Keywords: {kw_str}*\n\n")
            f.write("| title | status | language | reconstructed_path | file_link | sha256_12 | source | confidence |\n")
            f.write("|-------|--------|----------|-------------------|-----------|-----------|--------|------------|\n")
            for e in entries:
                title_esc = (e["title"] or "").replace("|", "\\|")[:60]
                path_esc = (e["reconstructed_path"] or "").replace("|", "\\|")[:40]
                link_esc = (e["file_link"] or "").replace("|", "\\|")[:50]
                f.write(f"| {title_esc} | {e['status']} | {e['language']} | {path_esc} | {link_esc} | {e['sha256_12']} | {e['line_range'][:40]} | {e['confidence']} |\n")

            high_med = [e for e in entries if e["confidence"] in ("HIGH", "MED")][:10]
            for e in high_med:
                f.write(f"\n### Excerpt: {e['title'][:50]} ({e['sha256_12']})\n\n")
                lines = e["content"].splitlines()
                f.write("```\n")
                f.write(_excerpt(lines, MAX_PREVIEW_LINES))
                f.write("\n```\n\n")
            f.write("\n")

    # --- Write PATTERN_TAG_INDEX.json ---
    index_data = {}
    for item in PATTERNS:
        tag = item[0]
        entries = tag_to_entries.get(tag, [])
        index_data[tag] = [
            {
                "file": e["file_link"],
                "status": e["status"],
                "sha256": e["sha256"],
                "reconstructed_path": e["reconstructed_path"],
                "title": e["title"],
            }
            for e in entries
        ]
    with open(out_dir / "PATTERN_TAG_INDEX.json", "w", encoding="utf-8") as f:
        json.dump(index_data, f, indent=2)

    # --- Write DUPLICATE_PATTERNS.md ---
    dup_path = out_dir / "DUPLICATE_PATTERNS.md"
    with open(dup_path, "w", encoding="utf-8") as f:
        f.write("# Duplicate Patterns (identical sha256, multiple tags/locations)\n\n")
        for sha, locs in sorted(sha_to_locations.items()):
            if len(locs) <= 1:
                continue
            files = {loc["file_link"] for loc in locs}
            if len(files) < 2:
                continue
            f.write(f"## sha256: {sha[:16]}...\n\n")
            for loc in locs:
                f.write(f"- {loc['file_link']} | {loc['title'][:40]} | tags: {', '.join(loc['tags'])}\n")
            f.write("\n")

    # --- Write CONFLICT_SUMMARY.md ---
    conf_path = out_dir / "CONFLICT_SUMMARY.md"
    with open(conf_path, "w", encoding="utf-8") as f:
        f.write("# Conflict Summary (same virtual path, different content)\n\n")
        for vpath, variants in sorted(conflicts_by_path.items()):
            if len(variants) < 2:
                continue
            f.write(f"## {vpath}\n\n")
            f.write(f"- number of variants: {len(variants)}\n")
            f.write("- sha256 list:\n")
            for v in variants:
                f.write(f"  - {v['sha256']}\n")
            f.write("- sources:\n")
            for v in variants:
                f.write(f"  - {v['source_md']} L{v['start_line']} -> {v['output_file']}\n")
            f.write("\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
