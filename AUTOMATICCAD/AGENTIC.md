# AUTOMATICCAD — Agentic Completion Plan

Scope: `AUTOMATICCAD/` — CAD corpus discovery, deduplication, and curation.

## Purpose
Build a reliable, deduplicated, user-relevant CAD corpus of DWG/DXF files drawn from the
user's machine, ready for downstream TOTaLi automation. Not a pipeline phase — a dataset
curation subproject with its own goals and weekly plan.

## Current baseline (from `GOALS.md` / `NEXT_STEPS.md`)
- Files scanned: 3,064,920
- CAD files matched: 382
- Unique/canonical: 160
- Duplicates: 200
- Copy errors: 0
- Timeout/other errors: 22
- Source mix: Applications=126, Users=30, Library=4
- Unique user DWG/DXF candidates: 10
- Files staged under `files/`: 163 (includes DS_Store)

## Named goals (from `GOALS.md`)
1. Complete CAD Discovery — resolve all 22 timeout/error paths; `0` error count.
2. Maintain Canonical Deduplicated Corpus — sha256 dedupe, canonical-path rules documented.
3. Curate for Project-Relevant Content — exclude system/app noise by rule.
4. Prioritize High-Value DWG/DXF — production vs sample vs test_fixture labels; accessibility verified.
5. Deliver Reproducible Pipeline — runbook + QA checklist + final report.

## Plan (from `NEXT_STEPS.md`, day-by-day)
Day 1 — resolve all 22 failed paths (`error_retry_<date>.csv`, `automaticcad_summary_retry_<date>.json`).
Day 2 — write + freeze curation rules (`CURATION_RULES_<date>.md`).
Day 3 — build curated manifest + curated files directory (`automaticcad_manifest_curated_<date>.csv`).
Day 4 — DWG/DXF priority list labeling (`dwg_dxf_priority_<date>.csv`).
Day 5 — runbook + final report (`RUNBOOK.md`, `AUTOMATICCAD_CURATED_REPORT_<date>.md`).

## Rules
- Curation rules are **frozen** before corpus rebuild. A rebuild against un-frozen rules
  is a non-reproducible run and its output is invalid.
- `sha256` is the canonical dedupe mechanism. Do not substitute a weaker hash.
- System/app paths (`/Applications`, `/Library`) excluded unless explicitly flagged as
  required by a documented project rule.
- Fixture vs production DWG/DXF labels are mandatory before the file enters TOTaLi's
  `Datasets/` staging.
- Every manifest row traces back to a canonical source path.

## Gates
Per-day success gates from `NEXT_STEPS.md`:
1. Day 1: all 22 failures have final disposition (`recovered`, `excluded`, `still_unreachable`).
2. Day 2: curation rules are specific enough that two independent reruns yield the same scope.
3. Day 3: curated corpus is traceable row-by-row to manifest.
4. Day 4: priority DWG/DXF list complete; every production file is accessible.
5. Day 5: full rerun is documented and final report is regeneratable on demand.

Final DoD (from `GOALS.md`):
- All five named goals marked Done with evidence links.
- Final manifest + report regenerable from documented steps.

## Tests required
Curation is data-driven, not unit-tested. Required verifications:
- Checksum verification script: every manifest row's sha256 matches its file.
- Accessibility probe: every `production`-labeled DWG/DXF opens with `ezdxf` or `LibreDWG`
  (pairs with `dwg-tool-parser/`).
- Dedupe audit: no two canonical entries share a sha256.
- Re-run equivalence: two runs of the pipeline against frozen rules produce identical manifests.

## Dependencies
- **Upstream:** user filesystem scan.
- **Downstream:** `dwg-tool-parser/` for DWG readability checks, TOTaLi `Datasets/` staging.

## Open questions / known debts
- Rule-file schema is markdown-only today. Consider machine-readable YAML once rules are
  stable, to enable diffable rule changes.
- 22 timeout paths may be cloud-sync dependent; retry strategy must account for
  Google Drive hydration (see `BV_BASE_data_reference_and_test_outcomes.md`).

## Definition of Done
- `GOALS.md` final checklist all Done.
- `RUNBOOK.md` exists with exact rerun procedure.
- `AUTOMATICCAD_CURATED_REPORT_<latest>.md` published with final metrics.
- Dedupe audit + accessibility probe + re-run equivalence checks green.

## Progress (append-only)
- 2026-06-17 — Day 2 partial: `CURATION_RULES_2026-06-17.md` frozen; pruned 163→10 git-tracked files (Applications/Library/mesh noise removed); `.gitignore` guards re-import.
