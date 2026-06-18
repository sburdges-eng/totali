# Agents — Agentic Completion Plan

Scope: `totali/agents/` — runtime agentic helpers (`context_sanitizer.py`,
`prompt_builder.py`) and any future helpers that execute inside the pipeline
runtime. (The former `coder_agent.py` in-process LLM codegen driver was removed —
see the note below.)

## Purpose
In-process agentic components that can be driven by the pipeline or operator tooling to
extend pipeline behavior without bypassing the phase contract. These are **not** the
outer-loop autonomous development agents (those live in `.claude/agents/` and workspace-scaffold);
these are runtime helpers with bounded authority.

## Plan
1. **AG-1 Authority model.** Every agent in this module declares its permissions up front:
   which `PipelineContext` fields it may read, which artifacts it may write, whether it may
   emit audit events. Declarations are checked at construction, not at call time.
2. **AG-2 No CAD writes.** Runtime agents never write to CAD directly. They operate on
   context artifacts and request `cad_shielding/` to finalize.
3. **AG-3 No silent promotion.** Agents have no path to promote a feature past `-DRAFT`.
4. **AG-4 Deterministic defaults.** Any agent that calls an LLM or a sampler must accept an
   explicit `seed` argument and record it in the audit payload. Nondeterministic runs are
   explicit and rare.
5. **AG-5 Testable contract.** Every agent has a thin interface file (`contracts.py` or inline)
   and at least one unit test stubbing the underlying model.
6. **AG-6 Kill-switch.** Config gates every runtime agent: `agents.<name>.enabled: false` is the
   default. Turning one on is a deliberate per-project decision.

## Rules
- Agents never bypass `AuditLogger`; every side effect produces an event.
- Agents never read from `audit_logs/` to "remember" state — they read from context/artifacts.
- No agent imports `totali.pipeline.orchestrator`.
- No network calls by default. Agents that do require network explicitly declare so and are
  disabled in CI.

## Gates
1. `pytest tests/test_agents_*.py -v` (to be added per agent) green.
2. Default config has all agents disabled; enabling one requires an explicit project config.
3. Every agent in the module has a declared permission block.

> **Removed:** `coder_agent.py` (in-process 70B-LLM early-fusion codegen driver)
> and its planned `tests/test_coder_agent.py` were dropped when the pipeline moved
> away from in-process LLM codegen. Current runtime helpers are
> `context_sanitizer.py` (prompt-injection scrub) + `prompt_builder.py`.

## Tests required
- `tests/test_agent_context_sanitizer.py` — injection-scrub coverage.
- `tests/test_prompt_builder.py` — context-capsule assembly + audit emission.

## Dependencies
- **Upstream:** `totali/audit/`, `totali/pipeline/`.
- **Downstream:** none today; agents are opt-in tooling consumed by the orchestrator or CLI.

## Open questions / known debts
- No formal permission schema today. Define Pydantic model `AgentPermissions` in AG-1.
- Whether to host agent prompts in the repo (versioned) or in config (per-project) — default to
  versioned, override in config.

## Definition of Done
- AG-1..AG-6 implemented and tested.
- Every existing agent file has a permission block.
- `agents.<name>.enabled: false` is the default in `config/pipeline.yaml`.

## Progress (append-only)
- _(no work recorded in completion_ledger.jsonl yet)_
