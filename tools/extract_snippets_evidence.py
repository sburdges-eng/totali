#!/usr/bin/env python3
"""
Evidence-backed Markdown code extractor.
Extracts fenced code blocks using multiple path signals. Read-only. Deterministic.
"""

import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

# --- USER CONFIG (editable) ---
ARCHIVE_ROOT: str = "/Users/seanburdges/Dev"
OUTPUT_ROOT: str = "./extracted_snippets_v2"
ALLOWED_LANGS: tuple[str, ...] = (
    "python", "py", "cpp", "c", "h", "hpp", "rust", "rs", "ts", "tsx",
    "js", "jsx", "json", "yaml", "yml", "toml", "bash", "sh", "zsh", "sql",
)
MAX_BLOCK_SIZE_KB: int = 512
WINDOW_LINES: int = 80
TITLE_REQUIRED: bool = True
PATH_REQUIRED_FOR_RECONSTRUCTED: bool = True

# Path signal types
SIGNAL_PATH_KEY = "PathKey"
SIGNAL_BACKTICKS = "Backticks"
SIGNAL_TABLE = "Table"
SIGNAL_PREFIX = "Prefix"
SIGNAL_FENCE_ATTR = "FenceAttr"
SIGNAL_NONE = "NONE"

VALID_EXTENSIONS = frozenset(
    ("py", "cpp", "c", "h", "hpp", "rs", "ts", "tsx", "js", "jsx",
     "json", "yml", "yaml", "toml", "sh", "sql", "md")
)

# Patterns
PATH_PATTERN = re.compile(r"^Path:\s+(.+)$")
VIRTUAL_PATH_PATTERN = re.compile(r"^VirtualPath:\s+(.+)$")
FILE_PATTERN = re.compile(r"^File:\s+(.+)$")
LOCATION_PATTERN = re.compile(r"^Location:\s+(.+)$")
SOURCE_PATTERN = re.compile(r"^Source:\s+(.+)$")
HEADER_PATTERN = re.compile(r"^(#{1,6})\s+(.+)$")
BACKTICK_PATH_PATTERN = re.compile(r"^`([^`]+)`\s*$")
# Table: find path-like token (slash + extension)
TABLE_PATH_PATTERN = re.compile(
    r"[^\s|]+\/[^\s|]*\.(?:py|cpp|c|h|hpp|rs|ts|tsx|js|jsx|json|yml|yaml|toml|sh|sql|md)\b"
)
# Fence attr: title="path" or file=path
FENCE_ATTR_PATTERN = re.compile(
    r"(?:title|file)\s*=\s*[\"']?([^\"'\s]+(?:/[^\"'\s]+)*\.[a-zA-Z0-9]+)[\"']?",
    re.IGNORECASE
)
FENCE_START_PATTERN = re.compile(r"^```(\w*)(.*)$")


def _normalize_lang(lang: str) -> str:
    return lang.strip().lower() if lang else ""


def _normalize_path(path: str) -> str:
    """Strip leading ./ for consistent output paths."""
    p = path.strip()
    if p.startswith("./"):
        p = p[2:]
    return p


def _is_valid_path(path: str) -> bool:
    """Path must be relative, contain slash, end with valid extension."""
    if not path or not isinstance(path, str):
        return False
    p = path.strip()
    if not p:
        return False
    if p.startswith("/") or (len(p) >= 2 and p[1] == ":") or "://" in p:
        return False
    if "/" not in p:
        return False
    if any(c in p for c in "[]()"):
        return False
    parts = p.rsplit(".", 1)
    if len(parts) != 2 or not parts[1]:
        return False
    return parts[1].lower() in VALID_EXTENSIONS


def _sanitize_filename(s: str) -> str:
    """Replace chars unsafe for filenames."""
    return re.sub(r'[<>:"/\\|?*]', "_", s)[:100].strip() or "untitled"


def _find_path_candidate(lines: list[str], fence_line_idx: int, fence_line_content: str) -> tuple[Optional[str], str, str]:
    """
    Find closest valid path using priority. Return (path, signal_type, evidence_excerpt).
    Scan from fence upward within WINDOW_LINES.
    """
    start = max(0, fence_line_idx - WINDOW_LINES)
    window = list(enumerate(lines[start:fence_line_idx], start=start))

    # Priority 5: Fence attr on the opening line
    if fence_line_content:
        m = FENCE_ATTR_PATTERN.search(fence_line_content)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_FENCE_ATTR, fence_line_content[:200])

    # Scan backward (closest first)
    for i in range(len(window) - 1, -1, -1):
        line_idx, line = window[i]
        stripped = line.strip()

        # Priority 1: Path: or VirtualPath:
        m = PATH_PATTERN.match(line)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_PATH_KEY, line.rstrip()[:200])
        m = VIRTUAL_PATH_PATTERN.match(line)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_PATH_KEY, line.rstrip()[:200])

        # Priority 2: Backticked path on its own line
        m = BACKTICK_PATH_PATTERN.match(stripped)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_BACKTICKS, line.rstrip()[:200])

        # Priority 3: Table row with path-like token
        if "|" in line and "/" in line:
            m = TABLE_PATH_PATTERN.search(line)
            if m:
                full_match = m.group(0).strip("`")
                if _is_valid_path(full_match):
                    return (full_match, SIGNAL_TABLE, line.rstrip()[:200])

        # Priority 4: File:, Location:, Source:
        m = FILE_PATTERN.match(line)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_PREFIX, line.rstrip()[:200])
        m = LOCATION_PATTERN.match(line)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_PREFIX, line.rstrip()[:200])
        m = SOURCE_PATTERN.match(line)
        if m:
            cand = m.group(1).strip().strip("`")
            if _is_valid_path(cand):
                return (cand, SIGNAL_PREFIX, line.rstrip()[:200])

    return (None, SIGNAL_NONE, "")


def _find_title(lines: list[str], fence_line_idx: int) -> Optional[str]:
    """Closest preceding header within WINDOW_LINES."""
    start = max(0, fence_line_idx - WINDOW_LINES)
    window = lines[start:fence_line_idx]
    for i in range(len(window) - 1, -1, -1):
        m = HEADER_PATTERN.match(window[i])
        if m:
            return m.group(2).strip()
    return None


def _collect_md_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in root.rglob("*.md"):
        if p.is_file():
            out.append(p)
    return sorted(out)


def _find_fenced_blocks(lines: list[str]) -> list[tuple[int, int, str, str, str]]:
    """
    Return (start_line_1based, end_line_1based, lang, body, fence_line_content).
    """
    blocks: list[tuple[int, int, str, str, str]] = []
    i = 0
    while i < len(lines):
        m = FENCE_START_PATTERN.match(lines[i])
        if m:
            lang = m.group(1) or ""
            fence_rest = m.group(2) or ""
            start_line = i + 1
            i += 1
            body_lines: list[str] = []
            while i < len(lines):
                if lines[i].strip() == "```":
                    end_line = i + 1
                    body = "".join(body_lines)
                    blocks.append((start_line, end_line, lang, body, fence_rest))
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
    parts = virtual_path.rsplit(".", 1)
    if len(parts) == 2 and parts[1]:
        return "." + parts[1]
    return ".txt"


def main() -> int:
    archive = Path(ARCHIVE_ROOT)
    if not archive.exists() or not archive.is_dir():
        print(f"Error: ARCHIVE_ROOT does not exist: {ARCHIVE_ROOT}", file=sys.stderr)
        return 1

    output = Path(OUTPUT_ROOT)
    reconstructed = output / "reconstructed"
    conflicts_dir = output / "conflicts"
    loose_dir = output / "loose"
    reports_dir = output / "reports"

    reconstructed.mkdir(parents=True, exist_ok=True)
    conflicts_dir.mkdir(parents=True, exist_ok=True)
    loose_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    max_bytes = MAX_BLOCK_SIZE_KB * 1024
    md_files = _collect_md_files(archive)
    archive_resolved = archive.resolve()

    index_rows: list[dict] = []
    path_to_first: dict[str, dict] = {}
    conflicts_list: list[dict] = []
    reject_counts: dict[str, int] = defaultdict(int)
    path_signal_counts: dict[str, int] = defaultdict(int)
    path_candidate_samples: dict[str, list[str]] = defaultdict(list)
    SAMPLE_LIMIT = 20

    reconstructed_count = 0
    loose_count = 0
    conflicts_count = 0
    duplicates_count = 0
    total_blocks_found = 0

    for md_path in md_files:
        rel_md = md_path.relative_to(archive_resolved)
        try:
            text = md_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            reject_counts["READ_ERROR"] = reject_counts.get("READ_ERROR", 0) + 1
            index_rows.append({
                "snippet_title": "",
                "language": "",
                "status": "REJECTED",
                "path": "",
                "path_signal_type": SIGNAL_NONE,
                "evidence_excerpt": "",
                "output_file": "",
                "sha256": "",
                "source_md": str(rel_md),
                "start_line": 0,
                "end_line": 0,
                "byte_size": 0,
                "reject_reason": f"READ_ERROR: {e}",
            })
            continue

        lines = text.splitlines(keepends=True)
        blocks = _find_fenced_blocks(lines)
        source_stem = md_path.stem

        for start_line, end_line, lang, body, fence_rest in blocks:
            total_blocks_found += 1
            norm_lang = _normalize_lang(lang)
            fence_line_idx = start_line - 1

            title = _find_title(lines, fence_line_idx)
            path_cand, path_signal, evidence = _find_path_candidate(
                lines, fence_line_idx, fence_rest
            )

            if path_signal != SIGNAL_NONE and len(path_candidate_samples[path_signal]) < SAMPLE_LIMIT:
                path_candidate_samples[path_signal].append(evidence)

            path_signal_counts[path_signal] += 1

            # Reject: lang not allowed
            if norm_lang not in ALLOWED_LANGS:
                reject_counts["SKIPPED_LANG"] += 1
                index_rows.append({
                    "snippet_title": title or "",
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "path": "",
                    "path_signal_type": path_signal,
                    "evidence_excerpt": evidence[:200],
                    "output_file": "",
                    "sha256": "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": len(body.encode("utf-8")),
                    "reject_reason": "SKIPPED_LANG",
                })
                continue

            # Reject: empty body
            if not body.strip():
                reject_counts["EMPTY_BODY"] += 1
                index_rows.append({
                    "snippet_title": title or "",
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "path": "",
                    "path_signal_type": path_signal,
                    "evidence_excerpt": evidence[:200],
                    "output_file": "",
                    "sha256": "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": 0,
                    "reject_reason": "EMPTY_BODY",
                })
                continue

            byte_size = len(body.encode("utf-8"))
            if byte_size > max_bytes:
                reject_counts["OVERSIZED"] += 1
                index_rows.append({
                    "snippet_title": title or "",
                    "language": norm_lang or lang,
                    "status": "REJECTED",
                    "path": "",
                    "path_signal_type": path_signal,
                    "evidence_excerpt": evidence[:200],
                    "output_file": "",
                    "sha256": "",
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": byte_size,
                    "reject_reason": "OVERSIZED",
                })
                continue

            computed_hash = _sha256_hex(body)

            # RECONSTRUCTED: title + valid path (PATH_REQUIRED_FOR_RECONSTRUCTED)
            if path_cand and _is_valid_path(path_cand):
                path_cand = _normalize_path(path_cand)
                if TITLE_REQUIRED and not title:
                    # Mark MISSING_TITLE but still allow extraction to loose/
                    title_safe = _sanitize_filename(title or "untitled")
                    ext = _ext_from_lang(norm_lang)
                    loose_name = f"{title_safe}**{_sha256_12(computed_hash)}**{source_stem}__L{start_line}{ext}"
                    loose_path = loose_dir / "by_lang" / (norm_lang or "unknown") / loose_name
                    loose_path.parent.mkdir(parents=True, exist_ok=True)
                    loose_path.write_text(body, encoding="utf-8")
                    loose_count += 1
                    index_rows.append({
                        "snippet_title": "",
                        "language": norm_lang or lang,
                        "status": "LOOSE",
                        "path": "",
                        "path_signal_type": path_signal,
                        "evidence_excerpt": evidence[:200],
                        "output_file": str(loose_path.relative_to(output)),
                        "sha256": computed_hash,
                        "source_md": str(rel_md),
                        "start_line": start_line,
                        "end_line": end_line,
                        "byte_size": byte_size,
                        "reject_reason": "MISSING_TITLE",
                    })
                    continue

                if path_cand in path_to_first:
                    first = path_to_first[path_cand]
                    if first["sha256"] == computed_hash:
                        duplicates_count += 1
                        index_rows.append({
                            "snippet_title": title or "",
                            "language": norm_lang or lang,
                            "status": "RECONSTRUCTED",
                            "path": path_cand,
                            "path_signal_type": path_signal,
                            "evidence_excerpt": evidence[:200],
                            "output_file": str(Path("reconstructed") / path_cand),
                            "sha256": computed_hash,
                            "source_md": str(rel_md),
                            "start_line": start_line,
                            "end_line": end_line,
                            "byte_size": byte_size,
                            "reject_reason": "",
                        })
                    else:
                        ext = _ext_from_path(path_cand)
                        conflict_name = f"{path_cand}**{_sha256_12(computed_hash)}**{source_stem}__L{start_line}{ext}"
                        conflict_path = conflicts_dir / conflict_name
                        conflict_path.parent.mkdir(parents=True, exist_ok=True)
                        conflict_path.write_text(body, encoding="utf-8")
                        conflicts_count += 1
                        conflicts_list.append({
                            "path": path_cand,
                            "sha256_variants": [first["sha256"], computed_hash],
                            "source_locations": [
                                (first["source_md"], first["start_line"]),
                                (str(rel_md), start_line),
                            ],
                            "output_files": [
                                str(Path("reconstructed") / path_cand),
                                str(Path("conflicts") / conflict_name),
                            ],
                        })
                        index_rows.append({
                            "snippet_title": title or "",
                            "language": norm_lang or lang,
                            "status": "CONFLICT",
                            "path": path_cand,
                            "path_signal_type": path_signal,
                            "evidence_excerpt": evidence[:200],
                            "output_file": str(Path("conflicts") / conflict_name),
                            "sha256": computed_hash,
                            "source_md": str(rel_md),
                            "start_line": start_line,
                            "end_line": end_line,
                            "byte_size": byte_size,
                            "reject_reason": "",
                        })
                else:
                    path_to_first[path_cand] = {
                        "sha256": computed_hash,
                        "source_md": str(rel_md),
                        "start_line": start_line,
                        "title": title or "",
                    }
                    out_file = reconstructed / path_cand
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                    out_file.write_text(body, encoding="utf-8")
                    reconstructed_count += 1
                    index_rows.append({
                        "snippet_title": title or "",
                        "language": norm_lang or lang,
                        "status": "RECONSTRUCTED",
                        "path": path_cand,
                        "path_signal_type": path_signal,
                        "evidence_excerpt": evidence[:200],
                        "output_file": str(Path("reconstructed") / path_cand),
                        "sha256": computed_hash,
                        "source_md": str(rel_md),
                        "start_line": start_line,
                        "end_line": end_line,
                        "byte_size": byte_size,
                        "reject_reason": "",
                    })
                continue

            # LOOSE: no path but valid block
            if not path_cand or not _is_valid_path(path_cand or ""):
                title_safe = _sanitize_filename(title or "untitled")
                ext = _ext_from_lang(norm_lang)
                loose_name = f"{title_safe}**{_sha256_12(computed_hash)}**{source_stem}__L{start_line}{ext}"
                loose_path = loose_dir / "by_lang" / (norm_lang or "unknown") / loose_name
                loose_path.parent.mkdir(parents=True, exist_ok=True)
                loose_path.write_text(body, encoding="utf-8")
                loose_count += 1
                index_rows.append({
                    "snippet_title": title or "",
                    "language": norm_lang or lang,
                    "status": "LOOSE",
                    "path": "",
                    "path_signal_type": path_signal,
                    "evidence_excerpt": evidence[:200],
                    "output_file": str(loose_path.relative_to(output)),
                    "sha256": computed_hash,
                    "source_md": str(rel_md),
                    "start_line": start_line,
                    "end_line": end_line,
                    "byte_size": byte_size,
                    "reject_reason": "",
                })

    # --- Write SNIPPET_INDEX.md ---
    index_path = reports_dir / "SNIPPET_INDEX.md"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("| snippet_title | language | status | path | path_signal_type | evidence_excerpt | output_file | sha256 | source_md | start_line | end_line | byte_size | reject_reason |\n")
        f.write("|---------------|----------|--------|------|------------------|------------------|-------------|--------|------------|------------|----------|-----------|---------------|\n")
        for r in index_rows:
            def esc(s):
                return (s or "").replace("|", "\\|").replace("\n", " ")[:200]
            f.write(f"| {esc(r.get('snippet_title'))} | {r.get('language', '')} | {r.get('status', '')} | {esc(r.get('path'))} | {r.get('path_signal_type', '')} | {esc(r.get('evidence_excerpt'))} | {esc(r.get('output_file'))} | {r.get('sha256', '')} | {esc(r.get('source_md'))} | {r.get('start_line', '')} | {r.get('end_line', '')} | {r.get('byte_size', '')} | {esc(r.get('reject_reason'))} |\n")

    # --- Write RUN_LOG.md ---
    log_path = reports_dir / "RUN_LOG.md"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("# Run Log\n\n")
        f.write(f"- md files scanned: {len(md_files)}\n")
        f.write(f"- total fenced blocks found: {total_blocks_found}\n")
        f.write(f"- reconstructed count: {reconstructed_count}\n")
        f.write(f"- loose count: {loose_count}\n")
        f.write(f"- conflicts count: {conflicts_count}\n")
        f.write(f"- duplicates count (same path, same hash, kept first): {duplicates_count}\n")
        f.write("\n## Rejected by reason\n\n")
        for reason, count in sorted(reject_counts.items()):
            f.write(f"- {reason}: {count}\n")
        f.write("\n## Path signal counts by type\n\n")
        for sig, count in sorted(path_signal_counts.items()):
            f.write(f"- {sig}: {count}\n")

    # --- Write PATH_CANDIDATE_SAMPLES.md ---
    samples_path = reports_dir / "PATH_CANDIDATE_SAMPLES.md"
    with open(samples_path, "w", encoding="utf-8") as f:
        f.write("# Path Candidate Samples (20 per signal type)\n\n")
        for sig in [SIGNAL_PATH_KEY, SIGNAL_BACKTICKS, SIGNAL_TABLE, SIGNAL_PREFIX, SIGNAL_FENCE_ATTR]:
            f.write(f"## {sig}\n\n")
            for ex in path_candidate_samples.get(sig, [])[:SAMPLE_LIMIT]:
                f.write(f"- `{ex.replace(chr(10), ' ')[:200]}`\n")
            f.write("\n")

    return 0


def _ext_from_lang(lang: str) -> str:
    lang_to_ext = {
        "python": ".py", "py": ".py", "cpp": ".cpp", "c": ".c",
        "h": ".h", "hpp": ".hpp", "rust": ".rs", "rs": ".rs",
        "ts": ".ts", "tsx": ".tsx", "js": ".js", "jsx": ".jsx",
        "json": ".json", "yaml": ".yml", "yml": ".yml", "toml": ".toml",
        "bash": ".sh", "sh": ".sh", "zsh": ".sh", "sql": ".sql",
    }
    return lang_to_ext.get(lang, ".txt")


if __name__ == "__main__":
    sys.exit(main())
