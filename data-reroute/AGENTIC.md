# Data Reroute — Agentic Completion Plan

Scope: `data-reroute/` — dataset inventory, large-file reroute to external SSD, symlink
management, duplicate-group detection.

## Purpose
Keep large TOTaLi datasets off primary storage while leaving the repo usable. Produces a
deterministic manifest of moved files, symlinks, and duplicates. Operator-driven, not
pipeline-invoked.

## Current baseline (from `README.md`)
- schemaVersion: `1.0.0`
- runTimestampUtc: `2026-02-28T09:08:42+00:00`
- thresholdBytes: `786432000` (750 MB)
- totalLargeCandidates: `7`
- eligibleCandidates: `6`
- movedRecords: `6`
- duplicateGroupsDetected: `0`
- purgedQuarantineFiles: `0`
- symlinksWritten: `6`
- errors: `0`

## Artifact layout
```
inventory/
  candidates_750mb_plus.csv
  classification_map.csv
  hash_manifest.csv
plans/
  move_plan.csv
  symlink_plan.csv
  seans_ssd_reroute_manifest.csv
reports/
  duplicate_report.csv
  empty_dirs_before.txt
  empty_dirs_after.txt
logs/
  move_log.tsv
  purge_log.tsv
  error_log.tsv
tools/
  apply_seans_ssd_reroute.py
```

## Plan
1. **DR-1 Schema stability.** `schemaVersion: 1.0.0` for all manifests. Bump on any field change.
2. **DR-2 Dry-run first.** `tools/apply_seans_ssd_reroute.py --dry-run` is the default posture
   for any new reroute job. Actual moves require an explicit flag.
3. **DR-3 Copy-then-symlink.** Copy to target first, verify sha256 matches source, then
   symlink + remove source. Never the other order. A partial move leaves the source intact.
4. **DR-4 Duplicate handling.** Duplicates are flagged in `reports/duplicate_report.csv`.
   The tool never silently deletes duplicates; dedupe is a separate, operator-approved step.
5. **DR-5 Hash manifest.** Every candidate's sha256 is recorded before move. Post-move
   verification re-hashes and confirms.
6. **DR-6 Logs as chain of custody.** `logs/move_log.tsv`, `purge_log.tsv`, `error_log.tsv`
   are append-only. Paired reviews check these are intact.
7. **DR-7 Reversibility.** Symlink plan is invertible — a follow-up tool can re-absorb
   files from external back to local if disk pressure eases.

## Rules
- No symlink written without a verified-on-target copy.
- No deletion of a source without a verified symlink at the original path.
- No cross-filesystem moves without pre-checking free space on the target.
- Datasets governance from TOTaLi CLAUDE.md applies here: the datasets themselves are never
  committed to git; only the manifests + reports are.

## Gates
1. Dry-run on a current snapshot completes with `errors: 0` and a self-consistent plan.
2. Real-run hash verification: every post-move sha256 matches the pre-move entry.
3. Symlink integrity: every entry in `plans/symlink_plan.csv` exists and resolves.
4. Empty-dir report: no protected directory deleted (sanity check against exclusion list).

## Tests required
Missing / to add (under `data-reroute/tests/`):
- `test_dry_run.py` — dry-run matches expected plan on fixture filesystem.
- `test_copy_verify_symlink.py` — simulated failure mid-copy leaves source intact.
- `test_hash_manifest.py` — sha256 reproducibility.
- `test_reversibility.py` — invert a symlink plan restores original layout.

## Dependencies
- **Upstream:** TOTaLi `Datasets/` and repo-wide large-file surfaces.
- **Downstream:** operators + any consumer reading symlinked paths.
- **External:** stdlib only (sha256, os, shutil).

## Open questions / known debts
- External root default is `/Volumes/Sean's SSD/Datasets`. Respect
  `feedback_dev_workspace_rules.md` and the `DEV_OPS_RUNBOOK.md` when rerouting paths that
  IDE workspaces point at.
- Cross-machine reproducibility — manifests include absolute paths; consider a relocatable
  manifest format.

## Definition of Done
- DR-1..DR-7 implemented with tests.
- Dry-run + real-run sequence documented in README.
- Hash manifest verification green on latest run.
- Reversibility test green.

## Progress
- 2026-02-28 — baseline run: 6 moved, 6 symlinks written, 0 errors, 0 duplicates.

## Progress (append-only)
- _(append dated entries as reroute runs complete)_
