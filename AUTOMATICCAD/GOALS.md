# AUTOMATICCAD Goals

Last updated: 2026-02-22

Source of truth:
- `/Users/seanburdges/AUTOMATICCAD/manifests/AUTOMATICCAD_REPORT_2026-02-18.md`
- `/Users/seanburdges/AUTOMATICCAD/manifests/automaticcad_summary.json`
- `/Users/seanburdges/AUTOMATICCAD/manifests/automaticcad_manifest.csv`

## Mission
Build a reliable, deduplicated, and user-relevant CAD corpus that is ready for downstream automation and analysis.

## Baseline (Current State)
- Files scanned: `3,064,920`
- CAD files matched: `382`
- Unique/canonical files: `160`
- Duplicate files: `200`
- Copy errors: `0`
- Other errors/timeouts: `22`
- Unique/canonical source split: `Applications=126`, `Users=30`, `Library=4`
- Unique user DWG/DXF candidates currently captured: `10`
- Files currently under `/Users/seanburdges/AUTOMATICCAD/files`: `163` (includes 3 `.DS_Store`)

## Named Goals

### 1) Complete CAD Discovery
Status: In progress

Definition of done:
- All `22` timeout/error paths are retried.
- Every failed path is either captured successfully or explicitly excluded with a documented reason.
- Error count for reachable targets is `0`.

What remains:
- Re-run scan/hash for cloud-backed timeout paths.
- Record final disposition for each previously failed path.

### 2) Maintain a Canonical Deduplicated Corpus
Status: In progress

Definition of done:
- Every copied file maps to a unique canonical hash entry.
- Duplicate handling is deterministic and reproducible.
- Manifest clearly indicates canonical source for each deduped group.

What remains:
- Keep `sha256`-based dedupe as the canonical mechanism.
- Validate canonical path selection rules and document them.

### 3) Curate for Project-Relevant Content
Status: Not done

Definition of done:
- Curated dataset is focused on intended project scope (not app/test noise).
- Include/exclude rules are written and versioned.
- System/app bundles are excluded unless explicitly needed.

What remains:
- Define scope rules (include likely user/project paths, exclude `/Applications`, `/Library`, tool test fixtures unless required).
- Produce a curated manifest and curated files folder from those rules.

### 4) Prioritize High-Value CAD Inputs (DWG/DXF)
Status: In progress

Definition of done:
- Priority DWG/DXF files are verified and accessible.
- Required survey/civil inputs are all present in curated output.
- Test fixtures are labeled separately from production inputs.

What remains:
- Validate the current unique DWG/DXF set.
- Separate user production drawings from validation fixtures and samples.

### 5) Deliver a Reproducible Pipeline
Status: Not done

Definition of done:
- A rerun produces the same output structure and comparable metrics.
- Runbook documents command, scope, exclusions, and expected outputs.
- Final report includes metrics and unresolved exceptions.

What remains:
- Document full rerun procedure.
- Add a final QA checklist and publish a final curated report.

## Completion Plan (Priority Order)
1. Resolve and classify all `22` timeout/error paths.
2. Lock include/exclude curation rules.
3. Rebuild curated corpus and curated manifest.
4. Validate DWG/DXF priority set and mark fixture vs production.
5. Run final QA pass on counts, hashes, and file accessibility.
6. Publish final report with "done/not done" status for each goal.

## Exit Criteria
Project is complete when all five named goals are marked done and the final curated manifest/report can be regenerated from documented steps.
