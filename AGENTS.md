# AGENTS.md — TOTaLi

Spatial / geodetic pipelines, DWG parsing, segmentation, CAD shielding, audit.

1. Read `CLAUDE.md` and `Docs/` before modifying pipeline or parser code.
2. CAD shielding: never bypass the audit log (`audit_logs/`) — all DWG mutations must be recorded.
3. DWG parsing uses LibreDWG via `dwg-tool-parser/`. LibreDWG is installed at `~/.local` and requires `PKG_CONFIG_PATH` for CMake builds.
4. Do not scan `Datasets/`, `artifacts/`, or `audit_logs/` recursively — they are large and not source.
5. Geodetic units: metres + EPSG codes explicit. Never silently reproject; always log the source and target CRS.
6. Deep research PDFs in root are authoritative references — consult before proposing architectural changes.
7. Test outcomes live in `BV_BASE_data_reference_and_test_outcomes.md` — update it when test expectations change.
8. Any C/C++ edit (in `dwg-tool-parser/`, auracad bridge, FFI surface, or vendored native deps) must follow `Docs/CXX_AGENTIC_RULES.md` — dangers, hard rules, sanitizer-backed workflows, debug strategies, review practices, pre-merge checklist. Read it before editing; propose amendments via PR, do not silently exempt.
9. Fully agentic / autonomous work must follow `AGENTIC_COMPLETION_PLAN.md` (top-level wire) and the target module's `AGENTIC.md`. Read order per session: `AGENTIC_COMPLETION_PLAN.md` → `<module>/AGENTIC.md` → its Plan steps → tests → gates. One `AGENTIC.md` exists per module under `totali/`, per tooling dir (`tests/`, `tools/`, `skills/`), and per sibling subproject (`survey-automation-roadmap/`, `AUTOMATICCAD/`, `laser-suite/`, `dwg-tool-parser/`, `totali-baton/`, `groundtruthos-data/`, `data-reroute/`).
