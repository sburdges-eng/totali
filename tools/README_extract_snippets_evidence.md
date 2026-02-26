# extract_snippets_evidence — Evidence-Backed Markdown Code Extractor

Upgrades the strict markdown extractor to an **evidence-backed extractor**. Extracts fenced code blocks from `.md` files using multiple path signals. Read-only. Deterministic output.

## Absolute Rules

- **Only read** `.md` files under `ARCHIVE_ROOT`
- **Do NOT** read any non-markdown files
- **Do NOT** invent paths — a path must be explicitly present in the markdown text near the block
- **Do NOT** execute code
- **Deterministic** output (sorted md paths, process blocks top-to-bottom)

## Run Command

```bash
python3 tools/extract_snippets_evidence.py
```

## Config (top of script)

| Config | Default | Description |
|--------|---------|-------------|
| ARCHIVE_ROOT | /Users/seanburdges/Dev | Markdown archive directory (must exist) |
| OUTPUT_ROOT | ./extracted_snippets_v2 | Output directory |
| ALLOWED_LANGS | python, py, cpp, c, h, ... | Language tags for extraction |
| MAX_BLOCK_SIZE_KB | 512 | Max block size in KB |
| WINDOW_LINES | 80 | Lines to scan above fence for metadata |
| TITLE_REQUIRED | true | Require header for RECONSTRUCTED |
| PATH_REQUIRED_FOR_RECONSTRUCTED | true | Require path for RECONSTRUCTED |

## Path Signals (priority order)

Within `WINDOW_LINES` above the fence, find the closest path candidate:

1. **PathKey** — Lines starting with `Path:` or `VirtualPath:`
2. **Backticks** — Backticked path on its own line, e.g. `` `KmiDi_CANON/brain/x.py` ``
3. **Table** — Table row containing a path-like token (contains slash + file extension)
4. **Prefix** — Lines starting with `File:`, `Location:`, or `Source:`
5. **FenceAttr** — Fence info string attributes:
   - `` ```python title="relative/path.py" ``
   - `` ```python file=relative/path.py ``

If multiple candidates exist, choose the closest one above the fence that passes validation.

## Path Validation

- Must be **relative** (not starting with `/` or drive letter)
- Must contain **at least one slash**
- Must end with a **plausible extension**: py, cpp, c, h, hpp, rs, ts, tsx, js, jsx, json, yml, yaml, toml, sh, sql, md
- No `[]()` (excludes markdown link syntax)

## Title Rule

- Must have a preceding markdown header within `WINDOW_LINES`
- If missing and `TITLE_REQUIRED=true`: mark `MISSING_TITLE`, but still allow extraction to `loose/`

## Extraction Output Classes

| Class | Condition | Output |
|-------|-----------|--------|
| **RECONSTRUCTED** | Title + valid path | `reconstructed/<path>` |
| **CONFLICT** | Same path, different hash | `conflicts/<path>**{sha256_12}**{source_stem}__L{line}.ext` |
| **LOOSE** | Valid block, no path (or path but no title) | `loose/by_lang/<lang>/{title_or_untitled}**{sha256_12}**{source_stem}__L{line}.ext` |
| **REJECTED** | SKIPPED_LANG, EMPTY_BODY, OVERSIZED | — |

### Conflict Handling

- **Same path, same hash**: Keep first, record as duplicate in RUN_LOG
- **Same path, different hash**: Write each variant to `conflicts/`

## Reports (under OUTPUT_ROOT/reports)

| Report | Contents |
|--------|----------|
| **SNIPPET_INDEX.md** | snippet_title, language, status, path, path_signal_type, evidence_excerpt, output_file, sha256, source_md, start_line, end_line, byte_size, reject_reason |
| **RUN_LOG.md** | md files scanned, total blocks, reconstructed/loose/conflicts/duplicates counts, rejected by reason, path_signal counts by type |
| **PATH_CANDIDATE_SAMPLES.md** | 20 examples per signal type of extracted evidence lines (to validate parsing) |

## Do NOT

- Guess paths from imports, symbols, or code content
- Merge content
- Modify markdown sources
