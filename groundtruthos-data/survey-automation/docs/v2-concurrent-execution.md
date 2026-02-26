# v2.0.0 Concurrent Execution Tracker

Last updated: 2026-02-13

This tracker runs streams 1-6 concurrently for v2 delivery while preserving sequential promotion gates (`Gate A`, `Gate B`, `Gate C`).

## Kickoff evidence

1. Command: `scripts/v2_release_candidate_gate.sh`
2. PT II gate run id: `pt2-gate-20260213T092345Z`
3. Tests: `55 passed, 1 skipped`
4. Pipeline accepted exit: `2`
5. Golden verification: `PASS`
6. Production-profile run id: `v2-kickoff-20260213T074227Z` (exit `2`)
7. Corrected production-profile run id: `v2-kickoff-fixed-20260213T074455Z` (exit `2`)
8. Noise-tuned production run id: `v2-noise-tuned2-20260213T084839Z` (exit `2`, warnings `20`)
9. Fixed production run id: `v2-noise-fixed-20260213T092320Z` (exit `0`, warnings `0`)
10. Warning hotspot report: `docs/v2-warning-hotspots.md`
11. Consecutive PT II CI passes:
   - `21979007569` ([run link](https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21979007569))
   - `21978984071` ([run link](https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21978984071))
12. Latest main gate evidence:
   - `CI`: `21981787789` ([run link](https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21981787789))
   - `PT2 Roadmap Gate`: `21982453407` ([run link](https://github.com/sburdges-eng/survey-automation-roadmap/actions/runs/21982453407))

## Stream status

1. Stream 1 (Scope + release bar): in progress
   - Evidence: `docs/roadmap-pt2.md`, `docs/release-candidate-checklist.md`
   - Next action: finalize explicit go/no-go decision owners for release signoff.
2. Stream 2 (Data + infra readiness): in progress
   - Evidence: `config/pipeline.ci.yaml`, `config/pipeline.prod.yaml`, `scripts/dataset_mirror.sh`
   - Next action: record ownership and refresh cadence for the active dataset source (`.local-datasets/TOTaLi/`).
3. Stream 3 (Candidate quality run): in progress
   - Evidence: `scripts/pt2_quality_gate.sh`, `artifacts/pt2-gate-20260213T092345Z/`, CI runs `21979007569` and `21978984071`
   - Next action: keep PT II CI gate green through release tagging.
4. Stream 4 (Product integration): in progress
   - Evidence: `survey-automation validate --config config/pipeline.prod.yaml` PASS, `survey-automation check-converter --config config/pipeline.prod.yaml` PASS, `artifacts/v2-noise-fixed-20260213T092320Z/manifest/run_manifest.json`
   - Next action: preserve clean-run baseline during final release signoff.
5. Stream 5 (RC hardening): in progress
   - Evidence: `scripts/v2_release_candidate_gate.sh`, `docs/release-candidate-checklist.md`
   - Next action: preserve gate evidence and prepare release tag handoff.
6. Stream 6 (Rollout prep): in progress
   - Evidence: `.github/workflows/pt2-roadmap-gate.yml`, `docs/operations.md`, runs `21979007569`, `21978984071`, `21981787789`, `21982453407`
   - Next action: maintain pass streak through release cut.

## Gate order (sequential decisions)

1. Gate A: latest candidate passes local quality + golden verification baseline.
2. Gate B: release-candidate checklist is fully satisfied including CI history checks.
3. Gate C: `main` gate pass, no in-scope unresolved P0/P1 defects, and tag approval.
