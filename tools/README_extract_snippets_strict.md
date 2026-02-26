# extract_snippets_strict — Ultra-Strict Markdown Code Extractor

Extracts fenced code blocks from `.md` files into a filesystem-like snippet pack. Read-only. Deterministic.

## Run Command

```bash
python3 tools/extract_snippets_strict.py
```

Or from the project root:

```bash
cd /path/to/Dev
python3 tools/extract_snippets_strict.py
```

**Config:** Edit the top of `extract_snippets_strict.py`:

- `ARCHIVE_ROOT` — path to the markdown archive directory (must exist; script exits 1 if missing)
- `OUTPUT_ROOT` — output directory (default: `./extracted_snippets`)
- `ALLOWED_LANGS` — language tags allowed for extraction
- `MAX_BLOCK_SIZE_KB` — max block size in KB (default: 512)

## Expected Metadata Format

A fenced code block is extractable **only if** both are present within 40 lines above the fence:

### Required

1. **TITLE** — closest preceding markdown header (`#`, `##`, `###`, etc.)
2. **VIRTUAL PATH** — exactly one of:
   - `Path: <relative/path/to/file.ext>`
   - `VirtualPath: <relative/path/to/file.ext>`

The path must:
- be relative (no leading `/` or drive letter)
- contain at least one slash
- have a file extension (e.g. `.py`, `.cpp`, `.json`)

### Optional

- `Hash: <sha256>` — 64 hex chars (recorded, not validated)
- `Status: CANONICAL|LEGACY|EXPERIMENTAL|SHADOW|QUARANTINE` — recorded only

## Example (authors must follow this)

````markdown
# ValueState and Execution States

Path: KmiDi_CANON/brain/kmidi_core/explicit_types.py
Hash: fa0b0e44eb8388a0ba88d37e8a0bbac597d478026c0599e43f88a73d766702b7
Status: CANONICAL

```python
# code here
```
````

## Output Layout

```
OUTPUT_ROOT/
├── reconstructed/     # virtual filesystem (VirtualPath)
├── conflicts/        # same VirtualPath, different sha256
└── reports/
    ├── SNIPPET_INDEX.md
    ├── DUPLICATE_GROUPS.md
    ├── CONFLICTS.md
    └── RUN_LOG.md
```

## Rejection Reasons

| Reason | Meaning |
|--------|---------|
| MISSING_TITLE | No header within 40 lines above fence |
| MISSING_VIRTUAL_PATH | No `Path:` or `VirtualPath:` line |
| INVALID_VIRTUAL_PATH | Path absolute, no slash, or no extension |
| SKIPPED_LANG | Language not in ALLOWED_LANGS |
| EMPTY_BODY | Code block is empty |
| OVERSIZED | Block exceeds MAX_BLOCK_SIZE_KB |
| CONFLICT_SAME_PATH_DIFFERENT_HASH | Same VirtualPath, different content → written to conflicts/ |
