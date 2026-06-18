# Claude Code — TOTaLi

Use [AGENTS.md](AGENTS.md) as the canonical entrypoint.

Defensible spatial drafting pipeline: AI classifies (probabilistic, advisory) → algorithms measure (deterministic) → humans certify (PLS-sovereign).

## Read before coding

- [AGENTS.md](AGENTS.md) — sub-project layout, cloud bootstrap, known issues.
- [Docs/CXX_AGENTIC_RULES.md](Docs/CXX_AGENTIC_RULES.md) — hard rules for any C/C++ touched here (DWG parser, auracad bridge, FFI surfaces).
- [Docs/PRODUCTION_DESIGN_REFERENCE.md](Docs/PRODUCTION_DESIGN_REFERENCE.md) — architectural north-star.

## Hard rules to re-assert

- AI output goes to a **DRAFT layer only**; certified geometry requires human accept/reject.
- Audit chain (`audit_logs/`) is append-only and defensibility-critical.
- C++/FFI changes follow `Docs/CXX_AGENTIC_RULES.md` without exception.
