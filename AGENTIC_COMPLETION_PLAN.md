# TOTaLi — Agentic Completion Plan

This is the top-level wire for fully agentic completion of TOTaLi. Per-module plans
live in `AGENTIC.md` files under each module or subproject; this document sequences
them, sets global rules and gates, and defines "done."

Read order for any agent starting work:

1. `AGENTS.md` (project doctrine; replaces old CLAUDE.md/GEMINI.md)
2. 
3. This document
4. The `AGENTIC.md` for the specific module being touched
5. `Docs/CXX_AGENTIC_RULES.md` if any C/C++ is involved

---

## 1. Mission (invariant)

TOTaLi is a defensible spatial drafting pipeline. The invariants an agent may **never** weaken:

1. No geometry auto-promotes to certified status. `auto_promote: false` is hardcoded.
2. `require_pls_signature: true` is hardcoded.
3. All AI/ML output is non-authoritative and lands on `TOTaLi-*-DRAFT` layers only.
4. `audit_logs/` is append-only SHA-256-chained JSONL. Retroactive edits are forbidden.
5. DWG/DXF/DGN writes go through `cad_shielding/` middleware. No direct certified-layer writes.
6. CRS changes require re-running the geodetic phase from scratch.
7. Deterministic outputs — no `-ffast-math`, no nondeterministic parallelism in geometry paths.

Any proposal that contradicts 1–7 requires a PR that amends `CLAUDE.md` and human approval.
Agents do not grant themselves exemptions.

## 2. Module inventory and per-module docs

Each row links to the module's own `AGENTIC.md`, which contains its purpose, plan,
rules, gates, tests, and definition of done.

### Core pipeline (python package `totali/`)

| Module                 | Path                                | Doc                                       |
|------------------------|-------------------------------------|-------------------------------------------|
| Pipeline orchestration | `totali/pipeline/`                  | `totali/pipeline/AGENTIC.md`              |
| Geodetic Gatekeeper    | `totali/geodetic/`                  | `totali/geodetic/AGENTIC.md`              |
| ML Segmentation        | `totali/segmentation/`              | `totali/segmentation/AGENTIC.md`          |
| Deterministic Extract  | `totali/extraction/`                | `totali/extraction/AGENTIC.md`            |
| CAD Shielding          | `totali/cad_shielding/`             | `totali/cad_shielding/AGENTIC.md`         |
| Surveyor Linting       | `totali/linting/`                   | `totali/linting/AGENTIC.md`               |
| Audit / chain-of-cust. | `totali/audit/`                     | `totali/audit/AGENTIC.md`                 |
| Agents                 | `totali/agents/`                    | `totali/agents/AGENTIC.md`                |
| Models (ONNX / proj.)  | `totali/models/`                    | `totali/models/AGENTIC.md`                |
| Quarantine UI          | `totali/quarantine_ui/`             | `totali/quarantine_ui/AGENTIC.md`         |
| Civil 3D REPL          | `totali/repl/`                      | `totali/repl/AGENTIC.md`                  |

### Test + tooling

| Area                   | Path                                | Doc                                       |
|------------------------|-------------------------------------|-------------------------------------------|
| Pytest suite           | `tests/`                            | `tests/AGENTIC.md`                        |
| Tooling / snippets     | `tools/`                            | `tools/AGENTIC.md`                        |
| Reusable skills        | `skills/`                           | `skills/AGENTIC.md`                       |

### Sibling subprojects

| Subproject             | Path                                | Doc                                       |
|------------------------|-------------------------------------|-------------------------------------------|
| Survey automation CLI  | `survey-automation-roadmap/`        | `survey-automation-roadmap/AGENTIC.md`    |
| CAD corpus curation    | `AUTOMATICCAD/`                     | `AUTOMATICCAD/AGENTIC.md`                 |
| ALTA/NSPS suite        | `laser-suite/`                      | `laser-suite/AGENTIC.md`                  |
| DWG/DXF parser stub    | `dwg-tool-parser/`                  | `dwg-tool-parser/AGENTIC.md`              |
| Baton/corpus (TS)      | `totali-baton/`                     | `totali-baton/AGENTIC.md`                 |
| GroundTruthOS data     | `groundtruthos-data/`               | `groundtruthos-data/AGENTIC.md`           |
| Data reroute           | `data-reroute/`                     | `data-reroute/AGENTIC.md`                 |

## 3. Dependency order (ship sequence)

Build/ship modules in this order. An agent may only declare a module "done" when
all upstream dependencies are done and its own gates pass.

```
 audit  ──►  pipeline (base_phase/context/models/orchestrator)
                 │
    ┌────────────┼────────────┬─────────────┬─────────────┐
    ▼            ▼            ▼             ▼             ▼
 geodetic   segmentation   extraction   cad_shielding   linting
    │            │            │             │             │
 quarantine_ui   models       │             │             │
                              └──────┬──────┴──────┬──────┘
                                     ▼             ▼
                                  tools/        repl (civil3d)
                                     │
                              ┌──────┴──────────────────────┐
                              ▼              ▼              ▼
                  survey-automation-roadmap  AUTOMATICCAD  laser-suite
                              │              │              │
                              └──────────────┼──────────────┘
                                             ▼
                                          tests/ (integration)
                                             │
                                             ▼
                              release-candidate gate (scripts/v2_release_candidate_gate.sh)
```

Parallelizable lanes:
- `geodetic` + `quarantine_ui` (quarantine UI depends on geodetic inference contracts, not code)
- `segmentation` + `models`
- sibling subprojects once the core phase they depend on is green

## 4. Global rules (apply to every module)

1. **No direct edits to `audit_logs/`, `Datasets/`, `artifacts/`.** Write via module APIs.
2. **`config/pipeline.yaml` is the sole source of thresholds, CRS lists, layer names, paths.** No hardcoded values in source. A config edit that changes semantics (tolerances, CRS allowlist, confidence thresholds) requires updating the module's golden tests in the same PR.
3. **Tests-first for new behavior.** A behavioral change lands with a failing-then-passing test demonstrating it.
4. **Audit emission is part of the API.** Any new pipeline action emits an audit event with a documented event name and payload schema; see `totali/audit/AGENTIC.md`.
5. **Phase contract is immutable.** Every phase implements `PipelinePhase` from `totali/pipeline/base_phase.py` and declares `get_required_inputs()` / `get_provided_outputs()`. Break the contract → break the orchestrator.
6. **Determinism.** Any randomness in a pipeline phase takes a seed from `config`. No wall-clock, no `random.random()` without explicit `Random(seed)`.
7. **No C/C++ edits without following `Docs/CXX_AGENTIC_RULES.md`.**
8. **No destructive git/filesystem operations** without explicit human approval — see §8 of `Docs/CXX_AGENTIC_RULES.md`, applies project-wide.
9. **Layer names follow `TOTaLi-<DISCIPLINE>-<FEATURE>-DRAFT`.** `TOTaLi-QA-*` layers are the only exception (no DRAFT suffix).
10. **PR body must carry the completion checklist from §7.**

## 5. Global gates (every change must pass)

Run from repo root before claiming completion:

| # | Gate                                   | Command                                                      |
|---|----------------------------------------|--------------------------------------------------------------|
| 1 | Lint / format                          | `ruff check . && ruff format --check .`                      |
| 2 | Type check (if mypy configured)        | `mypy totali` (allow missing; never downgrade existing)      |
| 3 | Full test suite                        | `pytest -q`                                                  |
| 4 | Module-scoped test                     | `pytest tests/test_<module>.py -v`                           |
| 5 | Integration test                       | `pytest tests/test_integration.py -v`                        |
| 6 | Config validation                      | `python -c "import yaml, sys; yaml.safe_load(open('config/pipeline.yaml'))"` |
| 7 | Golden regression (survey-automation)  | `cd survey-automation-roadmap && python validation/verify_golden.py` |
| 8 | PT II quality gate                     | `survey-automation-roadmap/scripts/pt2_quality_gate.sh`      |
| 9 | RC gate (before release tag)           | `survey-automation-roadmap/scripts/v2_release_candidate_gate.sh` |
| 10 | Audit log integrity                    | audit chain verifier (see `totali/audit/AGENTIC.md`)         |

Any red gate blocks merge. Green gates + unsigned review = still not merged.

## 6. Agentic workflow (the outer loop)

Every turn of autonomous work executes this sequence:

1. **Pick a task.** Use `TaskList` → lowest-ID pending task with no unresolved dependencies.
   If none, read §3 and pick the next unstarted module.
2. **Open the module's `AGENTIC.md`.** Follow its Plan section step by step. Do not jump ahead.
3. **Write or extend a failing test first.** Reference the specific behavior from the Plan.
4. **Implement the minimum to make the test pass.** No scope creep.
5. **Run module gates + global gates 1–6.** Gate 3 (full `pytest -q`) is non-negotiable.
6. **Update the module's `AGENTIC.md` Progress section** with: date, turn summary, gate evidence.
7. **Commit.** Never skip hooks. Never `--amend` past the session boundary.
8. **If stuck after 3 failed attempts on the same step, stop.** Write the blocker into the module's
   AGENTIC.md "Open questions" section and surface it to the human. Do not "try one more thing."
9. **Mark the task completed** only when §5 gates 1–5 are green AND the module's Definition of Done is met.

Never:
- Delete files in the same turn you detect they are unused (see `Docs/CXX_AGENTIC_RULES.md` §2).
- Edit `audit_logs/`, `Datasets/`, or `artifacts/` by hand.
- Change `auto_promote`, `require_pls_signature`, or layer-name patterns.
- Weaken a test assertion to make it pass. If a test is wrong, fix the test in a separate commit with justification.

## 7. Pre-merge checklist (copy into every PR body)

```
- [ ] Module AGENTIC.md Plan step(s) reference this PR
- [ ] Failing-then-passing test included
- [ ] ruff clean
- [ ] pytest -q green (N passed, M skipped)
- [ ] Integration test tests/test_integration.py green
- [ ] Config validates
- [ ] Audit events emitted for any new pipeline action
- [ ] Layer names conform to TOTaLi-<DISC>-<FEAT>-DRAFT
- [ ] No hardcoded thresholds / CRS / paths introduced
- [ ] auto_promote / require_pls_signature untouched
- [ ] Golden regression (if survey-automation touched): PASS
- [ ] PT II gate (if CLI touched): PASS
- [ ] CXX rules followed (if any C/C++): sanitizer build evidence attached
- [ ] Module AGENTIC.md Progress section updated with date + run evidence
```

## 8. Completion ledger

An agent maintains a rolling ledger at `artifacts/completion_ledger.jsonl` (append-only).
One JSONL record per module-step completion:

```json
{"ts":"2026-04-22T18:14:03Z","module":"geodetic","step":"G-3","pr":"#42","gates":["ruff","pytest","integration"],"evidence":"artifacts/<run-id>/..."}
```

The ledger is the single source of truth for "what's done." Agents read it at the
start of every session to avoid re-doing completed work.

## 9. Definition of Done (project-level)

TOTaLi is complete when:

1. All modules in §2 report DoD met in their `AGENTIC.md`.
2. `pytest -q` passes with zero skipped-for-pending-work (skips only for optional backends).
3. End-to-end run from `.las` → certified DXF on the BV_BASE golden dataset produces
   the expected artifacts, audit log, and PLS-signable deliverable.
4. `scripts/v2_release_candidate_gate.sh` (survey-automation) PASS with golden verification PASS.
5. A fresh `git clone` + `pip install -e .` + `python -m totali.main --input Datasets/<golden>.las --config config/pipeline.yaml` produces a byte-reproducible artifact set (up to audit timestamps) on two independent machines.
6. `audit_logs/` chain integrity verifies end-to-end.
7. Zero P0/P1 defects. Deferred items have written disposition notes.
8. Release notes and CHANGELOG reflect shipped behavior.

## 11. Continued Completion Protocol (autonomous-iteration authorization)

This section authorizes — under specific, narrow conditions — autonomous git
commits on long-running `agentic/*` branches so the orchestration loop can
continue across sessions without re-handshaking each turn.

### 11.1 Branch discipline

- All autonomous work happens on a branch named `agentic/<purpose>-<YYYY-MM-DD>`.
  Examples: `agentic/continuation-2026-04-22`, `agentic/audit-hardening-2026-05-03`.
- The agent **never** commits directly to `main` or to any non-`agentic/*` branch.
- The agent **never** force-pushes, deletes branches, or rewrites published
  history.
- The agent **never** pushes to a remote without a fresh per-session human
  authorization. Local commits on `agentic/*` are durable enough to resume from
  on the next turn; pushing is a blast-radius operation that needs explicit OK.

### 11.2 Per-commit gates (mandatory before any autonomous commit)

A commit may only be created when **all** of the following are true on the
current working tree:

1. `pytest -q` exits 0 with no failures and no new skips compared to the last
   ledger entry's evidence.
2. `ruff check totali/ tests/` is clean (or, for changes that touch only
   docs/artifacts, ruff is unchanged from prior state).
3. `python -c "import yaml; yaml.safe_load(open('config/pipeline.yaml'))"` is OK.
4. The commit changes files within the **scoped module(s)** described in the
   active task. Cross-module sweeps require an explicit "INFRA" or "RUFF-CLEANUP"
   ledger label.
5. No file under `audit_logs/`, `Datasets/`, or `artifacts/volume_import/` is
   touched.
6. No `.env`, key, credential, or token file is staged.
7. The commit message names the plan step (e.g. `feat(audit): A-5 event allowlist`)
   and ends with the standard `Co-Authored-By` trailer.
8. Pre-commit hooks run (no `--no-verify`).

If any gate fails, the agent halts the autonomous loop and surfaces an
escalation per §10 instead of committing.

### 11.3 Resume protocol (start of every autonomous session)

1. `git branch --show-current` — confirm we are on the expected `agentic/*` branch.
   If not, halt and escalate.
2. Read `artifacts/completion_ledger.jsonl` — this is the source of truth for
   what is done. Do not re-derive from git log.
3. Read `AGENTIC_ORCHESTRATION.md` Block 6 (`current_generation`) — confirms
   the active module and plan step.
4. `python -m pytest -q` — establish a green baseline before any edit. If the
   baseline is red, **fix the regression first** (no new feature work on a red
   tree).
5. Call `select_next_task(state)`:
   - lowest-ID pending task, not blocked
   - if none, advance to the next module in `ship_order` whose dependencies are met
   - if no implementable task remains without external inputs, halt with a
     final report

### 11.4 Stopping conditions (halt the loop and surface to human)

The agent **must** halt and surface, not auto-continue, when any of these is
true:

- Three consecutive failed attempts on the same plan step.
- A proposed change would weaken an invariant in §1.
- A blocker requires external data (BV_BASE hydration, ONNX weights, Civil 3D
  Windows env, AUTOMATICCAD operator presence).
- A blocker requires a destructive op (`git reset --hard`, `git push --force`,
  worktree/branch deletion, dependency downgrade).
- The remaining tasks are all blocked by a single architectural decision that
  needs human judgment (e.g. "should the REPL bridge unify with laser-suite
  dotnet?").
- Token / context budget exceeds 80 % of the session window — checkpoint, ledger,
  halt.

### 11.5 Per-turn evidence record (append to ledger)

Every autonomous commit MUST be paired with a ledger entry written **before**
the commit:

```jsonl
{"ts":"<ISO-8601 UTC>","module":"<name>","step":"<plan-step-id>","gates":["pytest_q","ruff_check","..."],"evidence":"<short summary of what passed>","files_touched":["<paths>"]}
```

The commit message references the ledger entry's step id. The ledger is the
durable bridge across stateless turns — never edit prior entries; always append.

### 11.6 What "complete" looks like (project-level)

Continued completion stops being meaningful once §9 Definition of Done is
fully met. At that point the agent's final action is:

1. Write a `artifacts/PROJECT_COMPLETE_<date>.md` summary mapping every §9
   bullet to evidence.
2. Append a final `step: PROJECT-COMPLETE` ledger entry.
3. Halt and request human review for merge to `main`.

The agent does **not** open the PR or merge — that crosses the push/shared-
state boundary which always requires per-session authorization.

## 10. Escalation

Agent halts and surfaces to the human when:

- A gate fails three times on the same change.
- A proposed fix would require weakening an invariant in §1.
- A destructive operation (git reset --hard, file deletion in data directories,
  dependency downgrade) would be the simplest path.
- CRS, geoid, or unit semantics look wrong on the golden dataset.
- Audit chain verification fails at any point.

Escalation format (post to conversation or open an issue):

```
Module:   <name>
Step:     <plan step>
Attempts: <n>
Observed: <what actually happened>
Expected: <what the plan says should happen>
Blocker:  <specific unknown>
Options:  <2-3 paths forward>
```
