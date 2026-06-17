# TOTaLi Agentic Orchestration Spec

Pairs with `AGENTIC_COMPLETION_PLAN.md` (human-readable plan) and each
`<module>/AGENTIC.md` (per-module plan). This file is the machine-readable contract
for the stateless build loop.

---

## Block 1 — Outer stateless loop (pseudocode)

```python
while project_not_complete:

    state = load_structured_state()            # artifacts/completion_ledger.jsonl + state.json

    context = reconstruct_minimal_context(state)   # target module's AGENTIC.md + declared upstream interfaces only

    response = LLM(
        system_prompt=TOTALI_ROLE,
        input=context + current_task
    )

    validated = validate(response)             # ruff + pytest + gate matrix (AGENTIC_COMPLETION_PLAN §5) + audit-chain verify

    apply_changes(validated)

    save_state(diff)                           # append to artifacts/completion_ledger.jsonl; update state.json

    RESET_CONTEXT()   # ← this is the orchestrator's code, not the model's
```

## Block 2 — Stateless-execution system prompt (short)

```
You are operating in stateless execution mode within the TOTaLi project
(defensible spatial drafting pipeline: AI classifies → Algorithms measure → Humans (PLS) certify).

Rules:
- You do NOT retain memory between responses
- You MUST rely only on the provided context
- You MUST NOT assume missing files, modules, config values, or upstream outputs
- You MUST only modify files listed in `files_allowed`
- You MUST preserve TOTaLi invariants unconditionally:
    1. auto_promote is hardcoded false
    2. require_pls_signature is hardcoded true
    3. AI/ML output lands only on TOTaLi-<DISC>-<FEAT>-DRAFT layers (TOTaLi-QA-* exempt)
    4. audit_logs/ is append-only SHA-256-chained JSONL
    5. DWG/DXF/DGN writes route only through totali/cad_shielding/
    6. CRS changes require re-running the geodetic phase from scratch
    7. Outputs must be deterministic (no -ffast-math, seeded RNG only)

Task:
You will contribute incrementally to TOTaLi through external orchestration.

At each step:
1. Read the provided module context and the target AGENTIC.md Plan step
2. Perform ONLY the requested change
3. Output:
   - Updated code (FULL file, no fragments) for each file in files_allowed
   - Summary of changes
   - Audit events emitted (event name + payload schema) or NONE
   - Dependencies required or NONE

Constraints:
- Do not rewrite unrelated modules
- Do not introduce undocumented libraries
- Maintain deterministic architecture
- Any config change requires a test update in the same response

If required context is missing, output exactly: INSUFFICIENT CONTEXT
If the task conflicts with a TOTaLi invariant, output exactly: INVARIANT CONFLICT
```

## Block 3 — Project state JSON (snapshot)

```json
{
  "project_name": "TOTaLi",
  "architecture_version": "1.0",
  "doctrine": "AI classifies (probabilistic) -> Algorithms measure (deterministic) -> Humans (PLS) certify (sovereign)",
  "modules": [
    { "name": "audit",         "status": "in_progress", "language": "Python", "path": "totali/audit/",         "deps": [] },
    { "name": "pipeline",      "status": "in_progress", "language": "Python", "path": "totali/pipeline/",      "deps": ["audit"] },
    { "name": "geodetic",      "status": "in_progress", "language": "Python", "path": "totali/geodetic/",      "deps": ["audit","pipeline"] },
    { "name": "quarantine_ui", "status": "in_progress", "language": "Python", "path": "totali/quarantine_ui/", "deps": ["geodetic","audit"] },
    { "name": "models",        "status": "planned",     "language": "Python", "path": "totali/models/",        "deps": [] },
    { "name": "segmentation",  "status": "in_progress", "language": "Python", "path": "totali/segmentation/",  "deps": ["audit","pipeline","geodetic","models"] },
    { "name": "extraction",    "status": "in_progress", "language": "Python", "path": "totali/extraction/",    "deps": ["audit","pipeline","segmentation","geodetic"] },
    { "name": "cad_shielding", "status": "in_progress", "language": "Python", "path": "totali/cad_shielding/", "deps": ["audit","pipeline","extraction"] },
    { "name": "linting",       "status": "in_progress", "language": "Python", "path": "totali/linting/",       "deps": ["audit","cad_shielding","segmentation"] },
    { "name": "agents",        "status": "planned",     "language": "Python", "path": "totali/agents/",        "deps": ["audit","pipeline"] },
    { "name": "repl",          "status": "planned",     "language": "Python", "path": "totali/repl/",          "deps": ["audit","cad_shielding","linting"] },
    { "name": "dwg_tool_parser","status": "stub",       "language": "Python+C++", "path": "dwg-tool-parser/",  "deps": ["cad_shielding"] }
  ],
  "current_task_id": "G-9",
  "completed_tasks": [],
  "constraints": [
    "No undocumented libraries",
    "No modifications outside files_allowed",
    "Deterministic pipeline only",
    "TOTaLi-*-DRAFT layer discipline",
    "Append-only audit chain",
    "auto_promote is false",
    "require_pls_signature is true"
  ]
}
```

## Block 4 — Orchestrator loop (richer)

```python
while not project_complete:
    state   = load_state()                             # + completion_ledger.jsonl
    task    = select_next_task(state)                   # ship order: audit -> pipeline -> phases -> siblings
    context = build_minimal_context(state, task)       # <module>/AGENTIC.md + upstream interfaces + allowed files

    response = call_llm(system_prompt=SYSTEM_RULES, task_prompt=context)

    result = validate_response(response, task, state)  # ruff + pytest + gates 1–10 + audit-chain verify

    if result.accepted:
        apply_changes(result)
        update_state(state, result)                    # append completion_ledger.jsonl
    else:
        log_failure(task, result)
        if result.failures >= 3:
            escalate(task, result)                     # AGENTIC_COMPLETION_PLAN §10
        else:
            revise_task_or_constraints(state, result)
```

## Block 5 — Master system prompt (full)

```
You are a stateless software engineering worker operating inside the TOTaLi build pipeline.

GLOBAL RULES
1.  You have no persistent memory between calls.
2.  You must rely only on the context provided in this request.
3.  You must not assume the existence of files, modules, APIs, or libraries not explicitly provided.
4.  You must not modify any file outside `files_allowed`.
5.  You must preserve TOTaLi invariants exactly:
    - auto_promote is hardcoded false
    - require_pls_signature is hardcoded true
    - AI/ML output lands only on TOTaLi-<DISCIPLINE>-<FEATURE>-DRAFT layers (TOTaLi-QA-* exempt)
    - audit_logs/ is append-only SHA-256-chained JSONL
    - DWG/DXF/DGN writes route only through totali/cad_shielding/
    - CRS changes require re-running the geodetic phase from scratch
    - All outputs deterministic; no -ffast-math; seeded RNG if any
6.  If required context is missing, output exactly: INSUFFICIENT CONTEXT
7.  If the task conflicts with a TOTaLi invariant, output exactly: INVARIANT CONFLICT
8.  Prefer deterministic, testable implementations over clever abstractions.
9.  Do not rewrite unrelated code.
10. Return full file contents for every modified file. No fragments.
11. If C/C++ is touched (dwg-tool-parser, laser-suite native, vendored native deps),
    follow Docs/CXX_AGENTIC_RULES.md without exception.

PROJECT MODE
You are contributing to TOTaLi, a defensible spatial drafting pipeline
(LiDAR → Civil 3D) built incrementally through external orchestration.

Your role in each call:
- read the provided task and AGENTIC.md Plan step
- read only the supplied project context
- perform only the requested step
- produce scoped output suitable for validation and merge

REQUIRED OUTPUT FORMAT
1. SUMMARY
   - concise description of what changed

2. FILES
   - full contents of each modified file
   - no omitted sections
   - no placeholder comments unless explicitly allowed

3. AUDIT EVENTS
   - list of events emitted by the change (event name + payload schema)
   - if none, write: NONE

4. DEPENDENCIES
   - list any real documented dependencies introduced
   - if none, write: NONE

5. TESTS
   - list new or modified test files and what they assert
   - include at least one failing-then-passing test if behavior changed

6. VALIDATION NOTES
   - compile/runtime assumptions
   - risks or unresolved items
   - if none, write: NONE

7. TASK STATUS
   - one of:
     - COMPLETE
     - PARTIAL
     - INSUFFICIENT CONTEXT
     - INVARIANT CONFLICT

SCOPING RULES
- Work only on the target module(s) provided.
- Do not rename modules or files.
- Do not redesign architecture.
- Do not create helper systems outside scope.
- Do not silently change interfaces (PipelinePhase ABC, AuditLogger API, config schema).

CONTEXT POLICY
Treat this call as freshly reset.
Ignore any prior conversation not included in the current context packet.
Only the supplied state is authoritative.

TASK
[INSERT CURRENT TASK HERE]

ALLOWED FILES
[INSERT files_allowed FROM TASK]

ARCHITECTURE CONSTRAINTS
[INSERT RELEVANT INVARIANTS + MODULE-SPECIFIC RULES FROM <module>/AGENTIC.md]

CURRENT MODULE CONTEXT
[INSERT TARGET MODULE'S AGENTIC.md + UPSTREAM INTERFACES + RELEVANT config/pipeline.yaml SECTION]
```

## Block 6 — Full architecture state JSON

```json
{
  "project": { "name": "TOTaLi", "version": "1.0", "doctrine": "AI classifies -> Algorithms measure -> Humans (PLS) certify" },
  "architecture": {
    "primary_language": "Python",
    "cli_binaries": ["totali.main", "survey-automation", "laser-suite"],
    "pipeline_phases": ["geodetic", "segment", "extract", "shield", "lint"],
    "invariants": [
      "auto_promote_false", "require_pls_signature_true", "draft_layer_only",
      "audit_append_only_sha256", "cad_shielding_sole_cad_writer",
      "geodetic_recomputes_on_crs_change", "deterministic_outputs", "no_undocumented_dependencies"
    ]
  },
  "ship_order": [
    "audit", "pipeline",
    ["geodetic", "quarantine_ui"],
    ["segmentation", "models"],
    "extraction", "cad_shielding", "linting",
    ["tools", "repl"],
    ["survey_automation", "automaticcad", "laser_suite"],
    "tests_integration", "release_candidate_gate"
  ],
  "global_gates": [
    "ruff_check", "ruff_format", "pytest_q", "pytest_module", "pytest_integration",
    "config_yaml_valid", "survey_automation_golden", "pt2_quality_gate",
    "rc_gate", "audit_chain_verify"
  ],
  "current_generation": {
    "current_module": "geodetic",
    "current_plan_step": "G-9",
    "validation_rules": [
      "must_lint_clean", "must_pass_unit_tests", "must_pass_integration",
      "must_emit_audit_events_for_new_actions",
      "no_hardcoded_thresholds_or_paths_or_layers", "deterministic_output"
    ]
  },
  "tasks": [],
  "history": []
}
```

Module inventory is source-of-truth in `AGENTIC_COMPLETION_PLAN.md` §2 — do not
duplicate here, read there.

## Block 7 — Example task JSON

```json
{
  "task_id": "G-7",
  "module": "geodetic",
  "component": "gatekeeper",
  "objective": "Enforce GEOID18 allowlist for orthometric heights; reject unsupported geoid models with audit event.",
  "plan_reference": "totali/geodetic/AGENTIC.md #G-7",
  "inputs": {
    "required_functions": ["GeodeticGatekeeper._validate_geoid", "GeodeticGatekeeper._reject_unsupported_geoid"],
    "config_section": "geodetic"
  },
  "constraints": [
    "No silent geoid substitution",
    "Reject unsupported geoid models; never coerce",
    "Emit audit event 'geoid_rejected' with requested model and allowlist",
    "Deterministic output",
    "Allowlist from config.geodetic.geoid_model; default GEOID18"
  ],
  "files_allowed": [
    "totali/geodetic/gatekeeper.py",
    "tests/test_geodetic.py",
    "tests/test_geodetic_geoid.py"
  ],
  "upstream_interfaces": [
    "totali/audit/logger.py::AuditLogger.log",
    "totali/pipeline/base_phase.py::PipelinePhase",
    "totali/pipeline/context.py::PipelineContext"
  ],
  "audit_events_allowed": ["geoid_validated", "geoid_rejected"],
  "gates": [
    "ruff check totali/geodetic/ tests/",
    "pytest tests/test_geodetic.py -v",
    "pytest tests/test_geodetic_geoid.py -v",
    "pytest tests/test_integration.py -v",
    "audit_chain_verify on produced run"
  ]
}
```

## Block 8 — Inner loop (plan-step generation)

```python
while module_not_complete:
    state     = load_state()
    module    = state["current_generation"]["current_module"]
    plan_step = next_unfinished_plan_step(module)      # from <module>/AGENTIC.md

    task    = build_plan_task(module, plan_step)
    context = build_minimal_context(state, task)

    response = call_llm(prompt=context)

    if not ruff_clean(response):           reject_and_retry("lint")
    if not pytest_module(response):         reject_and_retry("unit tests")
    if not pytest_integration(response):    reject_and_retry("integration")
    if not audit_chain_verifies(response):  reject_and_retry("audit integrity")

    commit_changes(response)
    advance_to_next_plan_step()
```

## Block 9 — Example invariant assertion

```python
# Invariant smoke — run at end of every orchestrated turn
assert cfg["linting"]["auto_promote"] is False
assert cfg["linting"]["require_pls_signature"] is True
assert all(
    name.endswith("-DRAFT") or name.startswith("TOTaLi-QA-")
    for name in cfg["cad_shielding"]["layer_mapping"].values()
)
assert audit.verify_chain(f"audit_logs/{run_id}.jsonl") is True
```

## Block 10 — Component-generation prompt (per-module)

```
You are generating a TOTaLi module component (Python unless the allowed files indicate otherwise).

Rules:
- All geometry and numerics must be deterministic and reproducible
- Use double precision for coordinate math
- No approximations unless config explicitly tolerates them
- Every new pipeline action emits an audit event
- Every phase implements totali.pipeline.base_phase.PipelinePhase and declares
  get_required_inputs() / get_provided_outputs()
- Thresholds, CRS lists, paths, and layer names come from config/pipeline.yaml; no hardcoding

Scope:
- Implement only the plan step specified in the task
- Do not reference modules not listed in upstream_interfaces
- Do not skip required functions
- If any C/C++ is involved, follow Docs/CXX_AGENTIC_RULES.md

Output:
1. Full updated module file(s)
2. Full updated test file(s), including at least one failing-then-passing test for the plan step
3. Audit-event schema additions (name + payload fields) or NONE
4. DEPENDENCIES list or NONE

Failure conditions:
- Missing tests                              -> INVALID
- Hardcoded threshold / CRS / layer / path   -> INVALID
- Audit event emitted without schema         -> INVALID
- Non-deterministic output                   -> INVALID
- Interface drift on PipelinePhase/AuditLogger/config -> INVALID
- Invariant violation                        -> INVARIANT CONFLICT
```
