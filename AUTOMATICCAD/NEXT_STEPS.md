# AUTOMATICCAD Next Steps

Last updated: 2026-02-22
Owner: Sean (with Codex support)

This plan turns `/Users/seanburdges/AUTOMATICCAD/GOALS.md` into an execution checklist with dated milestones and completion gates.

## Current Snapshot
- Files scanned: `3,064,920`
- CAD files matched: `382`
- Unique/canonical: `160`
- Duplicates: `200`
- Timeout/errors: `22`
- Unique source mix: `Applications=126`, `Users=30`, `Library=4`
- Unique user DWG/DXF candidates: `10`

## Week Plan (2026-02-23 to 2026-02-27)

### Day 1 — Monday, February 23, 2026
Goal focus: Complete CAD discovery.

Checklist:
- [ ] Create `/Users/seanburdges/AUTOMATICCAD/manifests/error_retry_2026-02-23.csv` with all 22 failed paths.
- [ ] Retry each failed path from cloud storage and record status (`recovered`, `excluded`, `still_unreachable`).
- [ ] Add exclusion reason for any non-recoverable path.
- [ ] Update counts in a new summary file after retry.

Day 1 deliverables:
- `/Users/seanburdges/AUTOMATICCAD/manifests/error_retry_2026-02-23.csv`
- `/Users/seanburdges/AUTOMATICCAD/manifests/automaticcad_summary_retry_2026-02-23.json`

Day 1 success gate:
- All 22 failures have a final disposition.

### Day 2 — Tuesday, February 24, 2026
Goal focus: Curate for project-relevant content.

Checklist:
- [ ] Write explicit include/exclude rules.
- [ ] Confirm default exclusions for system/app noise unless explicitly required.
- [ ] Identify fixture/test paths and classify them separately from production CAD.
- [ ] Freeze curation rules for reproducible reruns.

Day 2 deliverables:
- `/Users/seanburdges/AUTOMATICCAD/manifests/CURATION_RULES_2026-02-24.md`

Day 2 success gate:
- Rules are specific enough that two independent reruns would produce the same curated scope.

### Day 3 — Wednesday, February 25, 2026
Goal focus: Build curated corpus.

Checklist:
- [ ] Generate curated manifest from `unique_or_canonical` using Day 2 rules.
- [ ] Build curated files directory from curated manifest.
- [ ] Ensure each curated file has a canonical hash reference.
- [ ] Verify no unexpected system/app-only artifacts remain.

Day 3 deliverables:
- `/Users/seanburdges/AUTOMATICCAD/manifests/automaticcad_manifest_curated_2026-02-25.csv`
- `/Users/seanburdges/AUTOMATICCAD/manifests/automaticcad_summary_curated_2026-02-25.json`
- `/Users/seanburdges/AUTOMATICCAD/files_curated/`

Day 3 success gate:
- Curated corpus is reproducible and traceable to manifest rows.

### Day 4 — Thursday, February 26, 2026
Goal focus: Prioritize high-value DWG/DXF inputs.

Checklist:
- [ ] Create a priority list of production DWG/DXF files.
- [ ] Tag each as `production`, `sample`, or `test_fixture`.
- [ ] Validate accessibility and readability of production DWG/DXF files.
- [ ] Confirm required survey/civil files are included in curated output.

Day 4 deliverables:
- `/Users/seanburdges/AUTOMATICCAD/manifests/dwg_dxf_priority_2026-02-26.csv`

Day 4 success gate:
- Priority DWG/DXF list is complete and every production file is accessible.

### Day 5 — Friday, February 27, 2026
Goal focus: Reproducible pipeline and final signoff package.

Checklist:
- [ ] Write runbook with exact rerun procedure and expected outputs.
- [ ] Run end-to-end QA on counts, hash consistency, and path accessibility.
- [ ] Publish final curated report with resolved exceptions.
- [ ] Mark each goal in `GOALS.md` as `Done` or `Not done` with evidence links.

Day 5 deliverables:
- `/Users/seanburdges/AUTOMATICCAD/RUNBOOK.md`
- `/Users/seanburdges/AUTOMATICCAD/manifests/AUTOMATICCAD_CURATED_REPORT_2026-02-27.md`

Day 5 success gate:
- Full rerun is documented and final report can be regenerated on demand.

## Final Definition of Complete
- [ ] Goal 1 done: all 22 failures resolved or excluded with reasons.
- [ ] Goal 2 done: canonical dedupe behavior verified and documented.
- [ ] Goal 3 done: curated dataset excludes non-project noise by rule.
- [ ] Goal 4 done: production DWG/DXF set validated and prioritized.
- [ ] Goal 5 done: runbook + QA + final report published and reproducible.

## Blocking Risks to Watch Daily
- Cloud sync latency or offline cloud files.
- Ambiguous boundary between fixtures and production CAD.
- Rule drift between runs without a frozen curation document.
- Silent copy omissions if destination paths change.

## Daily Standup Template
Use this to update progress each day in one short note.

Date:
Completed:
Blocked:
Next:
Metrics delta:
