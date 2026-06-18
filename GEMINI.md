# Gemini — TOTaLi

Use [AGENTS.md](AGENTS.md) as the canonical entrypoint.

Re-assert before generating code:

- AI output is advisory and routes to the DRAFT layer; humans certify.
- Audit logs (`audit_logs/`) are append-only — never modify in place.
- Any C/C++ change follows [Docs/CXX_AGENTIC_RULES.md](Docs/CXX_AGENTIC_RULES.md): no `-ffast-math`, no exceptions across `extern "C"`, explicit FFI ownership.
