# HITL Completion To M2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry TOTaLi from the in-person M1 kit to M2 paid-pilot proof while keeping partner and PLS decisions human-owned.

**Architecture:** TOTaLi stays an offline-first deterministic survey-to-CAD pipeline. Agents may complete technical dry-runs, tests, docs, and commits on `agentic/*` branches; humans provide DXF deliverable approval, PLS certification field sign-off, partner LAS/jobs, manual baseline hours, and merge approval. If a requirement depends on paid work or external partner input, mimic completion only by producing technical evidence and marking the human gate pending.

**Tech Stack:** Python 3.14, pytest, ruff, laspy, pyproj, ezdxf, LibreDWG `dwg2dxf`, YAML config, append-only JSONL audit ledger, optional C++/CMake/ASAN only when DWG native or auracad code is touched.

## Global Constraints

- Work in an isolated worktree or a concrete `agentic/*` branch such as `agentic/hitl-completion-2026-06-18`.
- Never commit to `main`; final merge to `main` waits for human-in-the-loop approval.
- Never push, force-push, delete worktrees, delete branches, or rewrite history without fresh human approval.
- No runtime dependency on cloud APIs or runtime AI.
- All AI or ML classifications remain advisory and land on `TOTaLi-*-DRAFT` layers only.
- `auto_promote: false` and `require_pls_signature: true` must not weaken.
- No direct hand edits to `audit_logs/`, `Datasets/`, or `artifacts/volume_import/`.
- Preserve deterministic config and serialization; no hidden wall-clock or randomness in outputs except audit timestamps.
- Treat partner, PLS, legal, licensing, and paid-job evidence as human gates; record technical mimic evidence but do not mark the gate complete.
- Re-plan before entering a new subsystem, after three failed fixes on one issue, or when the path enters unknown architecture.
- Reset context at about 80 percent window usage, before cross-repo architecture changes, before any C++ public-header work, or after three failed debug attempts.

---

## Completion Ladder

| Level | Product meaning | Exit evidence |
|---|---|---|
| M1 first-sale bar, about 72 percent | v1 runnable on partner-style data | In-person demo green, U1-U6 technical evidence, DXF and PLS specs prepared for sign-off |
| M2 pilot, target finished v1, about 80 percent | Partner uses the pipeline on paid jobs | SC1-SC4 evidence: stampable DXF, measured time savings, independent audit verification, paid job IDs |
| M3 design-ready | Civil 3D and Carlson drop-in | Host layer/block crosswalk validated |
| M4 breadth | Multi-CRS, more survey types, optional DWG output | Per-deal units, DWG licensing disposition |
| M5 civil automation | InGENeer L6 plus auracad execution | Real host mutation E2E |

Critical path is M1 to M2 in `projects/TOTaLi`. auracad, InGENeer, Liberals4Liberty, and FloorPlanDesigner are deal-gated expansion lanes.

## Event Sequence

1. **Current technical baseline:** `./tools/in_person_demo.sh` green; BV coded survey, USGS LAS, sales key flow, DP-1 LibreDWG, batch ASC smoke, G-9 branch evidence, and U1 spike artifacts exist on the in-person branch.
2. **In-person meeting:** run the demo, show DXFs under `IN_PERSON_OUT`, capture DXF deliverable spec, PLS fields, classifier direction, partner LAS location, and manual baseline method.
3. **Post-meeting commit:** update docs and ledger with human decisions; commit only to `agentic/*`.
4. **M1 code close:** partner LAS U4 E2E, jurisdiction zones, classifier decision memo, DP-1 resolver, batch ASC status, CI gates.
5. **M2 paid pilot:** run paid job, collect manual hours, verify audit independently, iterate DXF spec under PLS review.
6. **M3-M5 expansion:** start only after M2 evidence or explicit human direction.

## Task 1: Session Start And Context Reset Discipline

**Files:**
- Read: `AGENTS.md`
- Read: `CLAUDE.md`
- Read: `AGENTIC_COMPLETION_PLAN.md`
- Read: `artifacts/completion_ledger.jsonl`
- Read: latest `artifacts/SESSION_HANDOFF_*.md`
- Read: target module `AGENTIC.md`

**Interfaces:**
- Consumes: branch state, ledger tail, handoff state.
- Produces: one scoped task selected for the turn.

- [ ] **Step 1: Confirm branch and worktree**

Run:
```bash
git branch --show-current
git -c core.fsmonitor=false status --short --untracked-files=all
```

Expected: branch starts with `agentic/`; status contains only intended files.

- [ ] **Step 2: Read durable state**

Run:
```bash
tail -n 40 artifacts/completion_ledger.jsonl
ls -lt artifacts/SESSION_HANDOFF_*.md
```

Expected: latest ledger and handoff identify the active module and blockers.

- [ ] **Step 3: Establish baseline**

Run:
```bash
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -m pytest -q
```

Expected: zero failures. If red, stop feature work and debug the baseline first.

- [ ] **Step 4: Context reset rule**

When the session approaches 80 percent context, or the work crosses into a new module, write:
```text
artifacts/SESSION_HANDOFF_2026-06-18_HITL_COMPLETION.md
```

The handoff must include branch, commit, current test evidence, touched files, human blockers, next command, and rollback-free resume instructions.

## Task 2: In-Person Human Gate Capture

**Files:**
- Read: `Docs/DXF_DELIVERABLE_SPEC.md`
- Read: `Docs/PLS_CERTIFICATION_FIELDS.md`
- Read: `artifacts/IN_PERSON_FOLLOWUP_PLAN_2026-06-18.md`
- Update after meeting: `artifacts/SESSION_HANDOFF_2026-06-18_HITL_COMPLETION.md`
- Append after meeting: `artifacts/completion_ledger.jsonl`

**Interfaces:**
- Consumes: partner and PLS decisions.
- Produces: signed or explicitly pending M1 human gates.

- [ ] **Step 1: Run the demo kit before the meeting**

Run:
```bash
PYTHONDONTWRITEBYTECODE=1 IN_PERSON_OUT="$PWD/.tmp/in_person_demo" ./tools/in_person_demo.sh
```

Expected: exit 0; BV coded DXF, USGS LAS DXF, and sales flow DXF paths printed.

- [ ] **Step 2: Capture DXF deliverable decisions**

Record each item as `approved`, `revise`, or `blocked`:
```text
Layer names:
Block policy:
Naming convention:
Draft layer visibility:
Carlson import target:
Civil 3D import target:
Required companion files:
```

- [ ] **Step 3: Capture PLS certification decisions**

Record each item as `approved`, `revise`, or `blocked`:
```text
Board fields:
ALTA fields:
Seal/signature representation:
Audit attachment format:
Certification export blocker:
```

- [ ] **Step 4: Capture paid-work blockers honestly**

If partner cannot provide a paid job or partner LAS during the meeting, write:
```text
Mimic completion status: technical dry-run approved; paid-job evidence pending.
Blocked evidence: partner job LAS, paid job ID, manual baseline hours.
```

Expected: no M2 gate is marked complete without real partner evidence.

## Task 3: Post-Meeting Commit And Ledger

**Files:**
- Modify: `Docs/DXF_DELIVERABLE_SPEC.md`
- Modify: `Docs/PLS_CERTIFICATION_FIELDS.md`
- Modify: `artifacts/SESSION_HANDOFF_2026-06-18_HITL_COMPLETION.md`
- Append: `artifacts/completion_ledger.jsonl`

**Interfaces:**
- Consumes: Task 2 meeting notes.
- Produces: durable M1 human-gate state.

- [ ] **Step 1: Write meeting decisions**

Update the two docs with exact partner/PLS outcomes. Do not infer approval from silence.

- [ ] **Step 2: Append ledger**

Append one JSONL row:
```json
{"ts":"2026-06-18T18:00:00Z","module":"HITL","step":"M1-MEETING-GATES","gates":["in_person_demo"],"evidence":"DXF and PLS decisions recorded; paid-job evidence status stated explicitly","files_touched":["Docs/DXF_DELIVERABLE_SPEC.md","Docs/PLS_CERTIFICATION_FIELDS.md","artifacts/SESSION_HANDOFF_2026-06-18_HITL_COMPLETION.md"]}
```

- [ ] **Step 3: Verify and commit**

Run:
```bash
ruff check totali/ tests/
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -m pytest -q
```

Expected: ruff clean; pytest zero failures.

Commit:
```bash
git add Docs/DXF_DELIVERABLE_SPEC.md Docs/PLS_CERTIFICATION_FIELDS.md artifacts/SESSION_HANDOFF_2026-06-18_HITL_COMPLETION.md artifacts/completion_ledger.jsonl
git commit -m "docs(hitl): record M1 partner gate decisions"
```

## Task 4: Partner LAS U4 E2E

**Files:**
- Modify if needed: `config/pipeline_in_person.yaml`
- Modify if needed: `config/pipeline.yaml`
- Modify if needed: `tools/run_partner_las_e2e.py`
- Test: `tests/test_e2e_topo_real.py`
- Append: `artifacts/completion_ledger.jsonl`

**Interfaces:**
- Consumes: partner LAS path and agreed jurisdiction.
- Produces: U4 evidence on partner data, or a specific partner-data blocker.

- [ ] **Step 1: Confirm the LAS exists**

Run:
```bash
test -f "$TOTALI_PARTNER_LAS"
```

Expected: exit 0. If exit 1, halt with blocker `partner LAS not provided or not hydrated`.

- [ ] **Step 2: Run partner LAS**

Run:
```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python tools/run_partner_las_e2e.py --config config/pipeline_in_person.yaml --output-dir .tmp/partner_las --audit-dir .tmp/partner_las_audit --project-id PARTNER-U4
```

Expected: JSON `success: true`, entity count present, DXF output present, audit verify green.

- [ ] **Step 3: If geodetic gate halts**

Do not weaken the gate. Record:
```text
Observed CRS:
Coordinate bounds:
Configured zones:
Required partner decision:
```

Then add or correct a jurisdiction zone only after human/partner confirmation.

## Task 5: U1 Classifier Direction

**Files:**
- Modify if needed: `tools/classifier_spike.py`
- Modify: `Docs/CLASSIFIER_DIRECTION_U1.md`
- Append: `artifacts/completion_ledger.jsonl`

**Interfaces:**
- Consumes: USGS or partner LAS with reference labels.
- Produces: adapt-pretrained vs train-custom recommendation.

- [ ] **Step 1: Run current spike**

Run:
```bash
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python tools/classifier_spike.py /Users/seanburdges/Dev/data/bv_base/lidar/USGS_CO_SanLuis_8764.las
```

Expected: rule baseline metrics artifact produced.

- [ ] **Step 2: Evaluate data quality**

If the LAS contains fewer than five useful semantic classes, write:
```text
Classifier decision: blocked by inadequate labels.
Technical mimic completion: rule baseline measured; ML path unavailable or unevaluable.
Human/partner evidence needed: labeled partner LAS or rich ASPRS tile.
```

- [ ] **Step 3: Do not train from scratch prematurely**

Only recommend custom training if partner supplies enough labeled points per class and agrees the class set. Otherwise recommend pretrained evaluation, then fine-tune.

## Task 6: G-9 And Jurisdiction Determinism

**Files:**
- Modify if needed: `totali/geodetic/gatekeeper.py`
- Modify if needed: `tests/test_geodetic_deterministic.py`
- Modify if needed: `config/pipeline.yaml`
- Append: `artifacts/completion_ledger.jsonl`

**Interfaces:**
- Consumes: stable CRS config and golden LAS fixture.
- Produces: byte-identical geodetic report evidence.

- [ ] **Step 1: Run module tests**

Run:
```bash
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -m pytest tests/test_geodetic_deterministic.py -q
```

Expected: all tests pass.

- [ ] **Step 2: If reports differ**

Follow systematic debugging: compare JSON canonical key ordering, CRS metadata, seed, timestamps, and file path fields. Do not normalize away a real geodetic mismatch.

## Task 7: DWG Parser DP-1 To DP-7

**Files:**
- Modify: `survey-automation-roadmap/dwg-tool-parser/scripts/parse_dwg.py`
- Modify: `tests/test_dwg_parser_libredwg.py`
- Modify: `dwg-tool-parser/AGENTIC.md`
- Future C++ only: files named in `Docs/CXX_AGENTIC_RULES.md` scope

**Interfaces:**
- Consumes: vendored LibreDWG at CAD-level `vendor/libredwg`.
- Produces: deterministic DWG to DXF to JSON path; later fuzz and round-trip evidence.

- [ ] **Step 1: Keep DP-1 resolver green**

Run:
```bash
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -m pytest tests/test_dwg_parser_libredwg.py -q
```

Expected: resolver precedence tests and real DWG round trip pass.

- [ ] **Step 2: C++ safety gate before DP-4 or native parser work**

Before any C++ or FFI edit, read `Docs/CXX_AGENTIC_RULES.md` and run:
```bash
clang-format --dry-run --Werror dwg-tool-parser/src/parser.cpp
cmake --build build/debug
ctest --test-dir build/debug --output-on-failure
```

If the changed C++ file has a different path, replace `dwg-tool-parser/src/parser.cpp` only after writing that exact path in the task handoff. If a native parser is introduced, add fuzz target first, run ASAN/UBSAN, and never pass untrusted `(ptr,len)` across FFI without a matching free function.

- [ ] **Step 3: Stop after three failed native/debug attempts**

Write the attempts, invariant, sanitizer output, and next options into the handoff. Do not attempt a fourth fix without re-plan.

## Task 8: M2 Paid Pilot Evidence

**Files:**
- Modify: `artifacts/SESSION_HANDOFF_2026-06-18_M2_PILOT.md`
- Append: `artifacts/completion_ledger.jsonl`
- Modify after approval: partner-facing docs under `Docs/`

**Interfaces:**
- Consumes: paid job ID, partner LAS, manual baseline hours, PLS review.
- Produces: SC1-SC4 evidence.

- [ ] **Step 1: Run paid-job pipeline**

Run the same command shape as Task 4 with project ID `PAID-PILOT-001` until the human supplies the real paid-job identifier. Mark SC4 blocked while that identifier is synthetic.

Expected: DRAFT DXF, audit log, metrics JSON.

- [ ] **Step 2: Record SC1-SC4**

Use exact fields:
```text
SC1 stampable DXF: approved | revise | blocked
SC2 manual hours: blocked until human supplies measured hours; pipeline seconds: record numeric seconds from metrics JSON
SC3 independent audit verify: pass | fail
SC4 paid job ID: blocked until human supplies job ID; invoice or contract reference held by human
```

- [ ] **Step 3: Public claims gate**

Do not write public time-savings claims until SC2 has real manual hours and partner approval.

## Task 9: Verification And Commit Protocol

**Files:**
- Append: `artifacts/completion_ledger.jsonl`
- Update: target module `AGENTIC.md`

**Interfaces:**
- Consumes: finished task changes.
- Produces: local commit on `agentic/*`.

- [ ] **Step 1: Run required gates**

Run:
```bash
ruff check totali/ tests/
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -m pytest -q
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -m pytest tests/test_integration.py -v
PYTHONPATH="$PWD" /Users/seanburdges/Dev/CAD/projects/TOTaLi/.venv/bin/python -c "import yaml; yaml.safe_load(open('config/pipeline.yaml'))"
```

Expected: all exit 0. If a C++ lane was touched, sanitizer and CTest evidence are also required.

- [ ] **Step 2: Append ledger before commit**

Append a JSONL row that names the module, step, gates, evidence, and files touched.

- [ ] **Step 3: Commit locally only**

Run:
```bash
git add artifacts/completion_ledger.jsonl dwg-tool-parser/AGENTIC.md survey-automation-roadmap/dwg-tool-parser/scripts/parse_dwg.py tests/test_dwg_parser_libredwg.py Docs/superpowers/plans/2026-06-18-hitl-completion-to-m2.md artifacts/SESSION_HANDOFF_2026-06-18_HITL_COMPLETION.md
git commit -m "fix(dwg-tool-parser): DP-1 linked-worktree LibreDWG discovery"
```

Expected: commit lands on `agentic/*`. Do not push or merge without human approval.

## Task 10: M1/M2 Exit Review

**Files:**
- Create when ready: `artifacts/PROJECT_COMPLETE_2026-06-18.md`
- Append: `artifacts/completion_ledger.jsonl`

**Interfaces:**
- Consumes: all M1 or M2 evidence.
- Produces: human review request.

- [ ] **Step 1: Map evidence to gates**

Write one row per gate:
```text
Gate:
Evidence:
Command:
Human owner:
Status:
```

- [ ] **Step 2: Halt for approval**

When M1 or M2 is evidence-complete, stop and ask for human approval to push, open PR, or merge. The agent does not complete the final main merge.
