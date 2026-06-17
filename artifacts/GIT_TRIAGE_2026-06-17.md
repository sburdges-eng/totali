# Git triage — 2026-06-17

**Branch at triage:** `main` (behind `origin/main` by 2)  
**Action taken:** created `agentic/geodetic-quarantine-trigger-2026-06-17` for B-1 work; **did not stage deletions**

## Summary

| Category | Count | Recommendation |
|----------|------:|----------------|
| Tracked deletions (`D`) | 149 | **Hold** — do not commit without explicit human approval |
| Modified tracked | 2 | `AGENTS.md`, `totali/__pycache__/__init__.cpython-314.pyc` — review separately |
| Untracked | ~40+ | Mostly docs, `artifacts/`, `data-reroute/`, `totali-baton/` — do not bulk-add |

## Deletion breakdown

Almost all `D` entries are under `AUTOMATICCAD/files/`:

- `.DS_Store` files (already in `.gitignore` but were committed historically)
- **FreeCAD.app** bundle test fixtures (`*.FCStd`, `*.stp`, `*.stl`, etc.) — committed in `41ee36f7` initial import
- **Shapr3D.app** gizmo OBJ assets
- Other `AUTOMATICCAD/files/Library/...` paths

**Interpretation:** Files appear **removed from disk** while still tracked in git. This is either intentional corpus cleanup or accidental deletion of the staged AUTOMATICCAD mirror. Mass-removing ~961k lines from git history in one commit is high blast-radius.

**Recommended next step (human decision):**

1. **If cleanup intended:** dedicated PR `chore/automaticcad-corpus-prune` with `AUTOMATICCAD/AGENTIC.md` rationale + updated `.gitignore` for `AUTOMATICCAD/files/Applications/`
2. **If accidental:** `git restore AUTOMATICCAD/` (and other deleted paths) before any pull/merge
3. **Either way:** run `git pull` only after worktree is clean or deletions are intentionally committed

## Safe to ignore for agentic work

- `__pycache__/*.pyc` modifications
- `.DS_Store` deletions (can restore or commit removal in a hygiene PR)

## Pull status

`main...origin/main [behind 2]` — fast-forward after triage resolution:

```bash
git fetch origin
git pull --ff-only origin main   # on main, after worktree clean
```

## Agentic branch discipline

All B-1 work proceeds on `agentic/geodetic-quarantine-trigger-2026-06-17` touching only:

- `totali/geodetic/gatekeeper.py`
- `config/pipeline.yaml` (geodetic section)
- `tests/test_geodetic_quarantine_trigger.py`
- `tests/test_geodetic_mixed_datum.py` (stale comment fix)
- `totali/geodetic/AGENTIC.md` (Progress)
- `artifacts/completion_ledger.jsonl`
