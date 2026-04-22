# Skills — Agentic Completion Plan

Scope: `skills/` — reusable pipeline skills (currently empty; shared skills live in
`workspace-scaffold/skills/`).

## Purpose
Project-local skill modules that are specific to TOTaLi (domain-specific workflows,
guarded actions, pipeline-aware helpers). Shared-across-projects skills live in
`workspace-scaffold/skills/` and are symlinked or referenced — not duplicated here.

## Plan
1. **SK-1 Decide scope.** Either: (a) leave this directory empty and point all agent skills
   to `workspace-scaffold/skills/`, or (b) create TOTaLi-specific skills here that
   encapsulate pipeline-specific actions (e.g., "promote a feature out of -DRAFT with PLS
   signature").
2. **SK-2 If (b): skill template.** Each skill lives in its own subdirectory with
   `SKILL.md` (description + when-to-use), `PROCEDURE.md` (steps), and any scripts.
3. **SK-3 Authority.** A TOTaLi skill may never bypass the invariants in
   `AGENTIC_COMPLETION_PLAN.md` §1. Skills that touch `audit_logs/`, certified layers, or
   `auto_promote` are forbidden.
4. **SK-4 Test hooks.** Every skill has at least one integration-style test exercising its
   procedure end-to-end against fixtures.

## Rules
- No skill is a shortcut around a phase. If a skill would bypass orchestration, it's a misuse.
- Skills are read-only on `Datasets/`.
- Skill discovery is explicit — symlinks in `.claude/skills/` pointing here, not auto-load
  from the pipeline.

## Gates
1. Each skill has `SKILL.md` + `PROCEDURE.md`.
2. Each skill has at least one test.

## Tests required
Missing / to add (once skills exist):
- `tests/test_skills_<name>.py` per skill.

## Dependencies
- **Upstream:** varies per skill.
- **Downstream:** `.claude/skills/` symlinks.

## Open questions / known debts
- Decide SK-1 (a) vs (b). Default today: (a). Record the decision once made.

## Definition of Done
- Directory is either empty with a pointer to `workspace-scaffold/skills/`, or contains
  at least one project-local skill with SK-1..SK-4 met.

## Progress (append-only)
- _(empty)_
