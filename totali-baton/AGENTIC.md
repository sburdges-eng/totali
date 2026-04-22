# TOTaLi-Baton (TS) — Agentic Completion Plan

Scope: `totali-baton/` — Node.js/TypeScript deterministic baton/corpus pipeline.

## Purpose
Deterministic corpus/baton pipeline for TOTaLi's TS-side tooling. Complement to the
Python pipeline, focused on file-level baton passing (immutable artifact handoffs between
stages) and deterministic corpus build for downstream consumers.

## Surface
- Build: `npm run build` (tsc)
- Test: `npm test` (builds then runs `node --test dist/tests/**/*.test.js`)
- Start: `node dist/src/cli.js`
- Node ≥ 20.

## Plan
1. **TB-1 Strict TS config.** `tsconfig.json` with `strict: true`, `noImplicitAny`,
   `noFallthroughCasesInSwitch`, `exactOptionalPropertyTypes`.
2. **TB-2 Pure functions.** Pipeline stages are pure functions that take an immutable
   input artifact and return an immutable output. Side effects are isolated to an
   IO boundary module.
3. **TB-3 Hashing.** Every emitted artifact carries a `sha256` field computed over its
   canonical JSON. Consumers verify before use.
4. **TB-4 Determinism.** Sorted keys, no `Date.now()` inside artifacts (accept injected `now`),
   no `Math.random()` without explicit seeded RNG.
5. **TB-5 CLI contract.** `cli.js` exposes `--help`, exits non-zero on error, writes to
   stdout or explicit `--out`.
6. **TB-6 No cross-language imports.** This module is TS-only. It reads files emitted by
   Python-side TOTaLi via stable JSON schemas; never imports Python or embeds it.

## Rules
- No `any` in production code. `unknown` + narrowing instead.
- No network access in CLI default path (tests gate explicitly).
- No wall-clock in artifacts.
- Artifacts are JSON. Binary formats are opaque byte streams with sha256 headers only.

## Gates
1. `npm run build` clean (no TS errors).
2. `npm test` green.
3. Determinism test: run the CLI twice on the same input, diff outputs — empty.
4. Hash test: emitted artifact `sha256` matches recomputed canonical hash.

## Tests required
Existing:
- Tests under `tests/` built to `dist/tests/`.

Missing / to add:
- `tests/determinism.test.ts` — double-run parity.
- `tests/hash.test.ts` — artifact hash contract.
- `tests/cli.test.ts` — `--help`, error exit codes.

## Dependencies
- **Upstream:** Python-side TOTaLi artifact JSON (read-only).
- **Downstream:** downstream TS consumers (TBD).
- **External:** none at runtime (pure TS + Node stdlib).

## Open questions / known debts
- Whether to publish to an internal registry or keep `private: true` — stays private today.
- No linter configured. Add ESLint with a strict ruleset if this module grows.

## Definition of Done
- TB-1..TB-6 implemented with tests.
- `npm test` green.
- Determinism test green.
- README documents surface + run procedure.

## Progress (append-only)
- _(empty)_
