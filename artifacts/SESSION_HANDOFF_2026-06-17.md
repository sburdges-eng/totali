# Dual-session agentic handoff — TOTaLi + CAD platform

**Created:** 2026-06-17 (Cursor)  
**Heavy lane:** tmux pane `[5] 0:2.1.179*` — Claude Code v2.1.179, Opus 4.8 (1M), xhigh effort  
**Fast lane:** Cursor Agent (this workspace)  
**Repo:** `~/Dev/CAD/projects/TOTaLi` (`main` @ `f06c3b1c`, synced with `origin/main`)  
**Verified baseline:** `.venv/bin/python -m pytest -q` → **948 passed, 2 skipped**; laser-suite **23/23**; RC gate **PASS** (2026-06-17)

---

## 1. Role split

| Lane | Session | Best for | Avoid |
|------|---------|----------|-------|
| **Heavy** | `[5] Claude Code 2.1.179` | Cross-module architecture, gap analysis vs `PRODUCTION_DESIGN_REFERENCE.md`, multi-file refactors, AGENTIC.md / ledger reconciliation, laser-suite math oracles, dwg C++ strategy, InGENeer↔TOTaLi contract design, long E2E reasoning | Quick gate runs, one-file fixes, git hygiene, CI triage |
| **Fast** | **Cursor** | `pytest`/`ruff`/bootstrap, scoped plan-step implementation, commits on `agentic/*`, PR prep, env fixes, subagent dispatch, ledger append after green gates | Reading entire 21× `AGENTIC.md` trees in one turn; platform-wide redesign without checkpoint |

**Checkpoint rule (both lanes):** halt at ~80% context; append `artifacts/completion_ledger.jsonl`; never edit prior ledger rows.

---

## 2. Mandatory read order (every session start)

1. `AGENTS.md` + `CLAUDE.md`
2. `AGENTIC_COMPLETION_PLAN.md` §1 (invariants) + §11 (autonomous protocol)
3. `Docs/TOTALI_MAPPING_TO_PRODUCTION_DESIGN.md` (gap map; note §10 landed vs §10.2 out-of-scope)
4. `artifacts/completion_ledger.jsonl` **tail** (source of truth for done work)
5. `artifacts/SESSION_HANDOFF_2026-06-17.md` (this file)
6. Target module `<module>/AGENTIC.md` Plan step only

**Stale warning:** `AGENTIC_ORCHESTRATION.md` Block 6 → **G-9** (geodetic determinism). Trust **ledger + mapping doc** for history.

---

## 3. Current baseline (2026-06-17)

### Green / landed (ledger + PRs #96–#115)

- Core invariants codified (`auto_promote`, PLS signature, DRAFT layers, audit chain)
- Full 5-phase pipeline E2E + audit verify (PR #104)
- `totali/external/` frozen contracts + `DxfAuracadAdapter` + `StubL4LInferenceAdapter` + composition (PR #100–103)
- Audit A-5 allowlist, A-7 fsync/close, A-8 sealed write API
- dwg-tool-parser DP-2/DP-3 (ezdxf DXF), laser-suite LS-2/LS-3 oracles + **config/adjustment fixes (#112)**
- Models M-1 ONNX loader; REPL R-1 contracts; quarantine Q-4 audit
- **Geodetic G-5/G-6** — sub-threshold CRS inference → quarantine UI (#109)
- **Geodetic G-7/G-8** — GEOID allowlist + `pyproj_version`/`proj_version` on all geodetic audit payloads (#115)
- **Handoff docs** — post-#113 state + Block 6 reconcile (#114)
- **AUTOMATICCAD corpus prune** — 157 system-app mirror paths removed; 10 DWG/DXF fixtures remain (#111)
- **Cloud bootstrap** — `pyarrow>=16.0.0` + venv pip repair docs (#113)
- **`v2_release_candidate_gate.sh` PASS** — PT II golden 6/6, eval + arbitration green

### Red / immediate (Fast lane owns)

| ID | Issue | Owner | First action |
|----|-------|-------|--------------|
| **ENV-1** | ~~Local `pytest` collection errors~~ | Cursor | **Resolved** — `.venv/bin/python -m pytest -q` (**948 passed** / 2 skipped) |
| **GIT-1** | ~~Dirty tree (mass `AUTOMATICCAD/files/` deletions)~~ | Cursor | **Resolved** — corpus prune merged (#111); stashes cleared |
| **B-1** | ~~Geodetic G-5..G-8~~ | Cursor | **Done** — #109 (G-5/G-6), #115 (G-7/G-8) |
| **GIT-2** | ~~`main` behind `origin/main`~~ | Cursor | **Resolved** — synced @ `f06c3b1c` |
| **RC-1** | ~~`v2_release_candidate_gate.sh` unverified~~ | Cursor | **Resolved** — PASS after `pyarrow` install |
| **DOC-1** | Module `AGENTIC.md` Progress sections empty despite ledger entries | Heavy | Reconcile ledger → Progress append-only sections |
| **ORCH-1** | ~~`AGENTIC_ORCHESTRATION.md` Block 6 stale~~ | Cursor | **Resolved** — `current_generation` → **G-9** |

### Owed for §9 Definition of Done (project-level)

| # | Criterion | Status | Lane |
|---|-----------|--------|------|
| 1 | All §2 modules report DoD in `AGENTIC.md` | **Partial** — tests exist; Progress/DoD checkboxes not maintained | Heavy reconcile, then Fast implement gaps |
| 2 | `pytest -q` zero pending-work skips | **Green locally** — 948 passed / 2 skipped; laser-suite 23/23 | — |
| 3 | BV_BASE golden `.las` → certified DXF E2E | **Blocked** — Google Drive hydration timeouts (`BV_BASE_data_reference_and_test_outcomes.md`) | Human + data-reroute |
| 4 | `v2_release_candidate_gate.sh` PASS | **PASS** (2026-06-17) | — |
| 5 | Byte-reproducible artifacts (2 machines) | Not demonstrated | Heavy design test matrix |
| 6 | Audit chain E2E verify | **Landed** in integration tests | — |
| 7 | Zero P0/P1 | laser-suite fixed (#112); groundtruthos `pip install -e` broken | Heavy (GTOS) |
| 8 | CHANGELOG / release notes | Not current | Fast after scope freeze |

---

## 4. Work queue — dependency order

Use `select_next_task`: lowest pending plan step whose upstream gates are green.

### Phase A — Stabilize (Fast lane) — **complete**

1. ~~Bootstrap env + establish `pytest -q` / `ruff` baseline on `main`~~
2. ~~`git status` triage — AUTOMATICCAD corpus cleanup~~ (#111)
3. ~~Pull `origin/main`; re-run gates~~
4. ~~Run `v2_release_candidate_gate.sh`~~ PASS

### Phase B — Core pipeline completion (split)

| Step | Module | Plan IDs | Lane | Notes |
|------|--------|----------|------|-------|
| B-1 | geodetic | ~~G-5..G-8~~ **done** (#109, #115); **G-9** next — byte-reproducible `_geodetic_report.json` (partial: `test_geodetic_deterministic.py` covers unit events only) | Fast | Block 6 `current_generation` |
| B-2 | segmentation | S-2 device fallback docs, ONNX stub for CI | Fast | |
| B-3 | cad_shielding | C-5..C-7 if any open post-PR #97 | Fast | Mapping says C-7 landed |
| B-4 | linting | L-6+ if any beyond L-5 | Fast | L-5 deferred flow landed |
| B-5 | agents | prompt_builder hardening, runtime tool policy | Heavy → Fast | |
| B-6 | repl | R-2..R-4 Civil3D bridge live path | Heavy | Windows/Civil3D env |
| B-7 | quarantine_ui | Q-1..Q-3 if not covered by Q-4/Q-5 | Fast | |

### Phase C — Sibling subprojects

| Subproject | Priority steps | Lane | Blocker |
|------------|----------------|------|---------|
| **laser-suite** | LS-1, LS-4..LS-7 | Fast fixes, Heavy formulas | config/adjustment fixed (#112); .NET Civil3D for LS-6 |
| **dwg-tool-parser** | DP-4 fuzz, DP-6 round-trip, DP-7 CLI hardening; LibreDWG path | Heavy strategy, Fast tests | LibreDWG install |
| **survey-automation** | SA maintenance only | Fast | v2 scope locked |
| **data-reroute** | DR tests DR-1..DR-7 | Fast | |
| **groundtruthos-data** | GT-1..GT-8; fix `pyproject.toml` build-backend | Heavy | PostgreSQL+PostGIS |
| **AUTOMATICCAD** | Day 1–5 curation plan; resolve 22 error paths | Heavy (rules), Fast (scripts) | Operator corpus decisions |
| **totali-baton** | TS corpus tooling (untracked) | Heavy scope | New; needs AGENTIC sync |

### Phase D — CAD platform (cross-repo, Heavy lane primary)

Owned outside TOTaLi git root but required for "near complete" platform:

| Project | Path | Owes production design |
|---------|------|------------------------|
| **auracad** | `~/Dev/CAD/projects/auracad/` | Geometry kernel phases 1–9; FFI to TOTaLi `AuracadAdapter` beyond DXF stub |
| **InGENeer** | `~/Dev/CAD/projects/InGENeer/` | AutonomAtIon orchestration, intent schema, iCAD bridge; no AGENTIC.md yet |
| **Liberals4Liberty** | `~/Dev/CAD/projects/Liberals4Liberty/` | Real `L4LInferenceAdapter` (replace stub); JEPA host candidate |
| **FloorPlanDesigner** | `~/Dev/CAD/projects/FloorPlanDesigner/` | 2D kernel slice |

**Explicitly out of TOTaLi scope** (orchestrator tier): vector memory, Temporal, Sigstore, full LNP, JEPA training.

---

## 5. Handoff message formats

### Cursor → Claude Code `[5]` (paste to start heavy work)

```
TOTaLi heavy-lane task. Read artifacts/SESSION_HANDOFF_2026-06-17.md + ledger tail.

Branch: <branch>
Baseline: pytest <N> passed / <M> skipped (after bootstrap)
Active step: <module>-<plan-id> from handoff §4

Deliver:
1. Architecture / gap analysis OR multi-file implementation (scope: <paths>)
2. Updated <module>/AGENTIC.md Progress entry (draft text)
3. Ledger row JSON for Fast lane to commit after gates
4. Escalation block if blocked (§10 format)

Do NOT commit unless on agentic/* and gates green. Do not touch audit_logs/, Datasets/, artifacts/volume_import/.
```

### Claude Code `[5]` → Cursor (end of heavy turn)

```
HANDOFF_TO_CURSOR
module: <name>
step: <plan-id>
status: COMPLETE | PARTIAL | BLOCKED
files_touched: [...]
ledger_row: { ... json ... }
next_fast_actions:
  - <command or edit>
blockers: <none | describe>
```

### Both lanes — escalation (AGENTIC_COMPLETION_PLAN §10)

```
Module:   <name>
Step:     <plan step>
Attempts: <n>
Observed: <what happened>
Expected: <plan says>
Blocker:  <specific unknown>
Options:  <2-3 paths>
```

---

## 6. Suggested immediate assignments

### Cursor (now)

1. **G-9** — extend `tests/test_geodetic_deterministic.py` for byte-identical `_geodetic_report.json` on golden LAS input (two runs)
2. Close geodetic module DoD in `totali/geodetic/AGENTIC.md` after G-9 green
3. Append ledger; PR on `agentic/*`

### Claude Code `[5]` (paste next)

```
Read origin/main @ f06c3b1c (#109–#115 merged). artifacts/SESSION_HANDOFF_2026-06-17.md + ledger tail.

Task HEAVY-1: Reconcile completion_ledger.jsonl against all totali/*/AGENTIC.md Progress sections. Produce append-only Progress drafts per module.

Task HEAVY-2: Cross-platform gap memo — auracad + InGENeer milestones for §9 items 3 (BV_BASE E2E) and 5 (byte reproducibility). Max 2 pages.

Task HEAVY-3: AUTOMATICCAD Day-1 retry plan for 22 timeout paths (post-#111 corpus).

Output HANDOFF_TO_CURSOR blocks. No commits.
```

---

## 7. Invariants (never weaken)

From `AGENTIC_COMPLETION_PLAN.md` §1 — both lanes enforce:

1. `auto_promote: false` hardcoded  
2. `require_pls_signature: true` hardcoded  
3. AI output → `TOTaLi-*-DRAFT` only  
4. `audit_logs/` append-only SHA-256 chain  
5. CAD writes via `cad_shielding/` only  
6. CRS change → re-run geodetic  
7. Deterministic geometry paths  

---

## 8. Success picture ("near complete")

**TOTaLi repo:** §9 Definition of Done met; `agentic/*` merged to `main`; RC gate PASS; BV_BASE hydrated and golden E2E green.

**CAD platform:** TOTaLi `external/` contracts have live auracad + L4L adapters (not stubs); InGENeer can orchestrate a survey intent packet into TOTaLi phases; laser-suite + survey-automation feed normalized bundles; AUTOMATICCAD corpus curated and reproducible.

**Orchestration:** Ledger + handoff docs stay current; Heavy/Fast lanes do not duplicate work; halts are explicit with evidence.
