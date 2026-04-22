# DWG Tool Parser — Agentic Completion Plan

Scope: `dwg-tool-parser/` — DWG/DXF parser stub, `scripts/parse_dwg.py`.

Status: **stub**. The production parser is not yet implemented here; some logic lives in
`workspace-scaffold/skills` and `~/.local` LibreDWG. This module's job is to become the
single canonical parser for TOTaLi's DWG ingestion.

## Purpose
Parse DWG and DXF files into a structured JSON report (entities, layers, block references,
survey tags). Used by: `cad_shielding/` (DWG round-trip), `AUTOMATICCAD/` (accessibility probe),
and TOTaLi tests.

## Plan
1. **DP-1 Library strategy.** Primary: LibreDWG (installed at `~/.local`, requires
   `PKG_CONFIG_PATH` for CMake). Fallback: `ezdxf` for DXF only. Record the chosen path per file.
2. **DP-2 Parser contract.** Output JSON schema: `{file, format, version, layers[], entities[], blocks[], survey_tags[], errors[]}`.
   Versioned `schemaVersion` on the root object.
3. **DP-3 Untrusted-input discipline.** Treat every length / offset / count in the input as
   hostile. Never trust header-declared sizes. Bound every read. See `Docs/CXX_AGENTIC_RULES.md`
   §1.4 and §2 for rules that apply if/when we call C++ parsers directly.
4. **DP-4 Fuzz harness.** A fuzz target for each entry point (DWG and DXF) lives in
   `scripts/fuzz/`. Runs in CI as a smoke step (short budget); full fuzz offline.
5. **DP-5 Encoding.** Strings carry an encoding tag. No `char*` → `str` decode without tag.
6. **DP-6 Round-trip (DXF).** DXF read → schema emit → DXF re-write via `ezdxf` preserves
   layer/entity counts and a hash of the canonical entity table.
7. **DP-7 CLI.**
   `python scripts/parse_dwg.py <file> [--project-tag X ...] [--output report.json]`
   exits non-zero on unreadable input; emits a schema-valid JSON on success.

## Rules
- No direct invocation of binary converters that have not been preflight-checked.
- No silent fallback from LibreDWG to ezdxf on a DWG file — DWG failure is DWG failure,
  surface it.
- Untrusted-input rules from `Docs/CXX_AGENTIC_RULES.md` §1.4 apply if any native parser
  is invoked.
- Parser never mutates the input file. Outputs go to `--output` or stdout only.

## Gates
1. `pytest tests/test_dwg_parser.py -v` green.
2. Running on a known DXF fixture produces a schema-valid JSON report with expected layer
   and entity counts.
3. Running on a corrupted DXF fails loudly (non-zero exit, structured error record).
4. Fuzz smoke runs in CI without hitting an unhandled crash.

## Tests required
Existing:
- `tests/test_dwg_parser.py` (TOTaLi-level integration)

Missing / to add (in this module or `tests/`):
- `tests/test_dwg_parser_schema.py` — output validates against JSON schema.
- `tests/test_dwg_parser_corrupt.py` — handcrafted truncation triggers structured error.
- `tests/test_dwg_parser_encoding.py` — mixed CP1252/UTF-8 strings round-trip.

## Dependencies
- **Upstream:** `ezdxf`, LibreDWG (`~/.local`, `PKG_CONFIG_PATH=$HOME/.local/lib/pkgconfig`).
- **Downstream:** `totali/cad_shielding/`, `AUTOMATICCAD/` accessibility probe, TOTaLi tests.

## Open questions / known debts
- DWG writing is out of scope today. Keep write path as `NotImplementedError`.
- Whether to vendor LibreDWG via git submodule vs rely on operator install — current state
  relies on operator install + `reference_libredwg_install.md`.

## Definition of Done
- DP-1..DP-7 implemented with tests.
- Schema JSON committed at `scripts/schema.json`; README links to it.
- Fuzz smoke passes on CI.
- Round-trip DXF test green.
- README updated to reflect production status (drop the "stub" language).

## Progress (append-only)
- _(empty)_
