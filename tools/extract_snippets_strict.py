#!/usr/bin/env python3
"""
Ultra-strict Markdown archive code extractor.
Reconstructs a filesystem-like snippet pack from fenced code blocks in .md files.
Read-only. No modifications to source. Deterministic output.
"""

import hashlib
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

# --- USER CONFIG (editable) ---
ARCHIVE_ROOT: str = "/Users/seanburdges/Dev"
OUTPUT_ROOT: str = "./extracted_snippets"
ALLOWED_LANGS: tuple[str, ...] = (
    "python", "py", "cpp", "c", "h", "hpp", "rust", "rs", "ts", "tsx",
    "js", "jsx", "json", "yaml", "yml", "toml", "bash", "sh", "zsh", "sql",
)
MAX_BLOCK_SIZE_KB: int = 512
METADATA_WINDOW_LINES: int = 40

# --- REJECTION REASONS ---
REJECT_MISSING_TITLE = "MISSING_TITLE"
REJECT_MISSING_VIRTUAL_PATH = "MISSING_VIRTUAL_PATH"
REJECT_INVALID_VIRTUAL_PATH = "INVALID_VIRTUAL_PATH"
REJECT_SKIPPED_LANG = "SKIPPED_LANG"
REJECT_EMPTY_BODY = "EMPTY_BODY"
REJECT_OVERSIZED = "OVERSIZED"
REJECT_CONFLICT_SAME_PATH_DIFFERENT_HASH = "CONFLICT_SAME_PATH_DIFFERENT_HASH"

# Path/Path: lines matching exactly
PATH_PATTERN = re.compile(r"^Path:\s+(.+)$")
VIRTUAL_PATH_PATTERN = re.compile(r"^VirtualPath:\s+(.+)$")
HASH_PATTERN = re.compile(r"^Hash:\s+([a-fA-F0-9]{64})$")
STATUS_PATTERN = re.compile(r"^Status:\s+(CANONICAL|LEGACY|EXPERIMENTAL|SHADOW|QUARANTINE)$")
HEADER_PATTERN = re.compile(r"^(#{1,6})\s+(.+)$")
FENCE_PATTERN = re.compile(r"^```(\w*)\s*$")


def _normalize_lang(lang: str) -> str:
    """Map lang tag to canonical form for ALLOWED_LANGS check."""
    return lang.strip().lower() if lang else ""


def _is_valid_virtual_path(path: str) -> bool:
    """Path must be relative, contain slash, have extension."""
    if not path or not isinstance(path, str):
        return False
    p = path.strip()
    if not p:
        return False
    if p.startswith("/") or (len(p) >= 2 and p[1] == ":"):
        return False
    if "/" not in p:
        return False
    parts = p.rsplit(".", 1)
    if len(parts) != 2 or not parts[1]:
        return False
    return True


def _find_metadata_in_window(lines: list[str], fence_line_idx: int) -> dict:
    """Scan up to METADATA_WINDOW_LINES before fence. Return title, path, hash, status."""
    start = max(0, fence_line_idx - METADATA_WINDOW_LINES)
    window = lines[start:fence_line_idx]

    title: Optional[str] = None
    virtual_path: Optional[str] = None
    declared_hash: Optional[str] = None
    declared_status: Optional[str] = None

    for i in range(len(window) - 1, -1, -1):
        line = window[i]
        stripped = line.strip()

        if HEADER_PATTERN.match(line):
            if title is None:
                m = HEADER_PATTERN.match(line)
                if m:
                    title = m.group(2).strip()
            continue

        if PATH_PATTERN.match(line):
            m = PATH_PATTERN.match(line)
            if m and virtual_path is None:
                virtual_path = m.group(1).strip()
            continue

        if VIRTUAL_PATH_PATTERN.match(line):
            m = VIRTUAL_PATH_PATTERN.match(line)
            if m and virtual_path is None:
                virtual_path = m.group(1).strip()
            continue

        if HASH_PATTERN.match(line):
            m = HASH_PATTERN.match(line)
            if m and declared_hash is None:
                declared_hash = m.group(1).strip().lower()
            continue

        if STATUS_PATTERN.match(line):
            m = STATUS_PATTERN.match(line)
            if m and declared_status is None:
                declared_status = m.group(1)
            continue

    return {
        "title": title,
        "virtual_path": virtual_path,
        "declared_hash": declared_hash,
        "declared_status": declared_status,
    }


def _collect_md_files(root: Path) -> list[Path]:
    """Return sorted list of .md files under root."""
    out: list[Path] = []
    for p in root.rglob("*.md"):
        if p.is_file():
            out.append(p)
    return sorted(out)


def _find_fenced_blocks(lines: list[str]) -> list[tuple[int, int, str, str]]:
    """
    Return list of (start_line_1based, end_line_1based, lang, body).
    Fence is ```lang at start, ``` at end.
    """
    blocks: list[tuple[int, int, str, str]] = []
    i = 0
    while i < len(lines):
        m = FENCE_PATTERN.match(lines[i])
        if m:
            lang = m.group(1) or ""
            start_line = i + 1
            i += 1
            body_lines: list[str] = []
            while i < len(lines):
                if lines[i].strip() == "```":
                    end_line = i + 1
                    body = "".join(body_lines)
                    blocks.append((start_line, end_line, lang, body))
                    i += 1
                    break
                body_lines.append(lines[i])
                i += 1
            continue
        i += 1
    return blocks


def _sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _sha256_12(hex_hash: str) -> str:
    return hex_hash[:12] if len(hex_hash) >= 12 else hex_hash


def _ext_from_path(virtual_path: str) -> str:
    """Get extension from virtual path (e.g. .py, .cpp)."""
    parts = virtual_path.rsplit(".", 1)
    if len(parts) == 2 and parts[1]:
        return "." + parts[1]
    return ""


def main() -> int:
    archive = Path(ARCHIVE_ROOT)
    if not archive.exists() or not archive.is_dir():
        print(f"Error: ARCHIVE_ROOT does not exist: {ARCHIVE_ROOT}", file=sys.stderr)
        return 1

    output = Path(OUTPUT_ROOT)
    reconstructed = output / "reconstructed"
    conflicts_dir = output / "conflicts"
    reports_dir = output / "reports"

    reconstructed.mkdir(parents=True, exist_ok=True)
    conflicts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    max_bytes = MAX_BLOCK_SIZE_KB * 1024

    md_files = _collect_md_files(archive)
    archive_resolved = archive.resolve()

    index_rows: list[dict] = []
    sha_to_locations: dict[str, list[dict]] = defaultdict(list)
    path_to_first: dict[str, dict] = {}
    conflicts_list: list[dict] = []
    reject_counts: dict[str, int] = defaultdict(int)
    extracted_count = 0
    rejected_count = 0
    conflicts_count = 0
    oversized_count = 0
    hash_mismatch_count = 0
    total_blocks_found = 0

    for md_path in md_files:
        rel_md = md_path.relative_to(archive_resolved)
        try:
            text = md_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            reject_counts["READ_ERROR"] = reject_counts.get("READ_ERROR", 0) + 1
            index_rows.append({
                "status": "REJECTED",
                "reject_reason": f"READ_ERROR: {e}",
                "source_md": str(rel_md),
                "start_line": 0,
                "end_line": 0,
            })
            continue

        lines = text.splitlines(keepends=True)
        blocks = _find_fenced_blocks(lines)
        total_blocks_found += len(blocks)
        source_stem = md_path.stem

        for start_line, end_line, lang, body in blocks:
            total_blocks_found += 0  # already counted
            norm_lang = _normalize_lang(lang)

            window_start = max(0, start_line - 1 - METADATA_WINDOW_LINES)
            meta = _find_metadata_in_window(lines, start_line - 1)

            title = meta["title"]
            virtual_path = meta["virtual_path"]
            declared_hash = meta["declared_hash"]
            declared_status = meta["declared_status"]

            if not title:
                reject_counts[REJECT_MISSING_TITLE] += 1
                rejected_count += 1
                index_rows.append({
                    "snippet_title": "",
                    "virtual_path": virtual_path or "",
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "output_file": "",
                    "sha256": "",
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": False,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": len(body.encode("utf-8")),
                    "reject_reason": REJECT_MISSING_TITLE,
                })
                continue

            if not virtual_path:
                reject_counts[REJECT_MISSING_VIRTUAL_PATH] += 1
                rejected_count += 1
                index_rows.append({
                    "snippet_title": title,
                    "virtual_path": "",
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "output_file": "",
                    "sha256": "",
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": False,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": len(body.encode("utf-8")),
                    "reject_reason": REJECT_MISSING_VIRTUAL_PATH,
                })
                continue

            if not _is_valid_virtual_path(virtual_path):
                reject_counts[REJECT_INVALID_VIRTUAL_PATH] += 1
                rejected_count += 1
                index_rows.append({
                    "snippet_title": title,
                    "virtual_path": virtual_path,
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "output_file": "",
                    "sha256": "",
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": False,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": len(body.encode("utf-8")),
                    "reject_reason": REJECT_INVALID_VIRTUAL_PATH,
                })
                continue

            if norm_lang not in ALLOWED_LANGS:
                reject_counts[REJECT_SKIPPED_LANG] += 1
                rejected_count += 1
                index_rows.append({
                    "snippet_title": title,
                    "virtual_path": virtual_path,
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "output_file": "",
                    "sha256": "",
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": False,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": len(body.encode("utf-8")),
                    "reject_reason": REJECT_SKIPPED_LANG,
                })
                continue

            if not body.strip():
                reject_counts[REJECT_EMPTY_BODY] += 1
                rejected_count += 1
                index_rows.append({
                    "snippet_title": title,
                    "virtual_path": virtual_path,
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "output_file": "",
                    "sha256": "",
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": False,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": 0,
                    "reject_reason": REJECT_EMPTY_BODY,
                })
                continue

            byte_size = len(body.encode("utf-8"))
            if byte_size > max_bytes:
                reject_counts[REJECT_OVERSIZED] += 1
                oversized_count += 1
                rejected_count += 1
                index_rows.append({
                    "snippet_title": title,
                    "virtual_path": virtual_path,
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "output_file": "",
                    "sha256": "",
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": False,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": byte_size,
                    "reject_reason": REJECT_OVERSIZED,
                })
                continue

            computed_hash = _sha256_hex(body)
            hash_mismatch = bool(declared_hash and declared_hash.lower() != computed_hash)
            if hash_mismatch:
                hash_mismatch_count += 1

            loc = {
                "virtual_path": virtual_path,
                "source_md": str(rel_md),
                "start_line": start_line,
                "end_line": end_line,
                "sha256": computed_hash,
                "title": title,
            }
            sha_to_locations[computed_hash].append(loc)

            if virtual_path in path_to_first:
                first = path_to_first[virtual_path]
                if first["sha256"] == computed_hash:
                    index_rows.append({
                        "snippet_title": title,
                        "virtual_path": virtual_path,
                        "language": norm_lang or lang,
                        "status": "EXTRACTED",
                        "output_file": str(Path("reconstructed") / virtual_path),
                        "sha256": computed_hash,
                        "declared_hash": declared_hash or "",
                        "hash_mismatch": hash_mismatch,
                        "declared_status": declared_status or "",
                        "source_md": str(rel_md),
                        "start_line": start_line,
                        "end_line": end_line,
                        "byte_size": byte_size,
                        "reject_reason": "",
                    })
                else:
                    ext = _ext_from_path(virtual_path) or ".txt"
                    conflict_name = f"{virtual_path}**{_sha256_12(computed_hash)}**{source_stem}__L{start_line}{ext}"
                    conflict_path = conflicts_dir / conflict_name
                    conflict_path.parent.mkdir(parents=True, exist_ok=True)
                    conflict_path.write_text(body, encoding="utf-8")

                    conflicts_count += 1
                    conflicts_list.append({
                        "virtual_path": virtual_path,
                        "sha256_variants": [first["sha256"], computed_hash],
                        "source_locations": [
                            (first["source_md"], first["start_line"]),
                            (str(rel_md), start_line),
                        ],
                        "output_files": [
                            str(Path("reconstructed") / virtual_path),
                            str(Path("conflicts") / conflict_name),
                        ],
                    })
                    index_rows.append({
                        "snippet_title": title,
                        "virtual_path": virtual_path,
                        "language": norm_lang or lang,
                        "status": "CONFLICT",
                        "output_file": str(Path("conflicts") / conflict_name),
                        "sha256": computed_hash,
                        "declared_hash": declared_hash or "",
                        "hash_mismatch": hash_mismatch,
                        "declared_status": declared_status or "",
                        "source_md": str(rel_md),
                        "start_line": start_line,
                        "end_line": end_line,
                        "byte_size": byte_size,
                        "reject_reason": REJECT_CONFLICT_SAME_PATH_DIFFERENT_HASH,
                    })
                    extracted_count += 1
            else:
                path_to_first[virtual_path] = {
                    "sha256": computed_hash,
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "title": title,
                }
                out_file = reconstructed / virtual_path
                out_file.parent.mkdir(parents=True, exist_ok=True)
                out_file.write_text(body, encoding="utf-8")
                extracted_count += 1
                index_rows.append({
                    "snippet_title": title,
                    "virtual_path": virtual_path,
                    "language": norm_lang or lang,
                    "status": "EXTRACTED",
                    "output_file": str(Path("reconstructed") / virtual_path),
                    "sha256": computed_hash,
                    "declared_hash": declared_hash or "",
                    "hash_mismatch": hash_mismatch,
                    "declared_status": declared_status or "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": byte_size,
                    "reject_reason": "",
                })

    # --- Write SNIPPET_INDEX.md ---
    index_path = reports_dir / "SNIPPET_INDEX.md"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("| snippet_title | virtual_path | language | status | output_file | sha256 | declared_hash | hash_mismatch | declared_status | source_md | start_line | end_line | byte_size | reject_reason |\n")
        f.write("|---------------|--------------|----------|--------|-------------|--------|---------------|---------------|-----------------|-----------|------------|----------|-----------|---------------|\n")
        for r in index_rows:
            title = (r.get("snippet_title") or "").replace("|", "\\|")
            vp = (r.get("virtual_path") or "").replace("|", "\\|")
            lang = r.get("language", "")
            status = r.get("status", "")
            out_f = (r.get("output_file") or "").replace("|", "\\|")
            sha = r.get("sha256", "")
            decl_hash = r.get("declared_hash", "")
            hm = str(r.get("hash_mismatch", False)).lower()
            decl_status = r.get("declared_status", "")
            src = (r.get("source_md") or "").replace("|", "\\|")
            sl = r.get("start_line", "")
            el = r.get("end_line", "")
            bs = r.get("byte_size", "")
            rej = (r.get("reject_reason") or "").replace("|", "\\|")
            f.write(f"| {title} | {vp} | {lang} | {status} | {out_f} | {sha} | {decl_hash} | {hm} | {decl_status} | {src} | {sl} | {el} | {bs} | {rej} |\n")

    # --- Write DUPLICATE_GROUPS.md ---
    dup_path = reports_dir / "DUPLICATE_GROUPS.md"
    with open(dup_path, "w", encoding="utf-8") as f:
        f.write("# Duplicate Groups (sha256 count > 1)\n\n")
        for sha, locs in sorted(sha_to_locations.items()):
            if len(locs) <= 1:
                continue
            f.write(f"## sha256: {sha}\n\n")
            for loc in locs:
                f.write(f"- {loc['virtual_path']} @ {loc['source_md']}:L{loc['start_line']}\n")
            f.write("\n")

    # --- Write CONFLICTS.md ---
    conf_path = reports_dir / "CONFLICTS.md"
    with open(conf_path, "w", encoding="utf-8") as f:
        f.write("# VirtualPath Collisions (different sha256)\n\n")
        for c in conflicts_list:
            f.write(f"## {c['virtual_path']}\n\n")
            f.write(f"- sha256 variants: {c['sha256_variants']}\n")
            f.write("- source locations:\n")
            for src, ln in c["source_locations"]:
                f.write(f"  - {src}:L{ln}\n")
            f.write("- output files:\n")
            for of in c["output_files"]:
                f.write(f"  - {of}\n")
            f.write("\n")

    # --- Write RUN_LOG.md ---
    log_path = reports_dir / "RUN_LOG.md"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("# Run Log\n\n")
        f.write(f"- md files scanned: {len(md_files)}\n")
        f.write(f"- total fenced blocks found: {total_blocks_found}\n")
        f.write(f"- extracted count: {extracted_count}\n")
        f.write(f"- rejected count: {rejected_count}\n")
        f.write(f"- conflicts count: {conflicts_count}\n")
        f.write(f"- oversized count: {oversized_count}\n")
        f.write(f"- hash_mismatch count: {hash_mismatch_count}\n")
        f.write("\n## Rejected by reason\n\n")
        for reason, count in sorted(reject_counts.items()):
            f.write(f"- {reason}: {count}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
