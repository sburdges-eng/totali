# LASER Suite — Agentic Completion Plan

Scope: `laser-suite/` — deterministic geodetic adjustment + encroachment detection +
Civil 3D export for 2026 ALTA/NSPS workflows. Python + dotnet (Civil 3D-side bridge).

## Purpose
Weighted least-squares adjustment over a survey network, relative-precision analysis,
encroachment tabulation (ALTA Table A Item 20), and a Civil 3D payload exporter. Fed by
TOTaLi's validated data; emits certifiable numerical artifacts.

## Surface
```
laser-suite run            --bundle-dir <dir> --config <yaml> --out <dir> [--run-id <id>]
laser-suite laser          --bundle-dir <dir> --config <yaml> --out <dir> [--run-id <id>]
laser-suite encroachment   --bundle-dir <dir> --config <yaml> --out <dir> [--run-id <id>]
laser-suite export-civil3d --run-root <dir> [--out <dir>]
```

Input bundle (CSVs under `bundle-dir`):
`stations.csv`, `observations.csv`, `weights.csv`, `adjacency.csv`, `boundaries.csv`,
`improvements.csv`, `easements.csv`, `setbacks.csv`.

## Key formulas (contract — treat as test oracles)
- Weighted least-squares correction: `(AᵀPA)⁻¹ AᵀPl`
- Pair covariance propagation: `ΣΔ = Σᵢᵢ + Σⱼⱼ − Σᵢⱼ − Σⱼᵢ`
- Relative precision actual: `RPP_actual = 2.448 · √λmax`
- Relative precision allowable: `RPP_allowable = 0.02 + (50e-6 · distance_m)`

## Plan
1. **LS-1 Bundle validation.** Every bundle CSV validated against a Pydantic schema on
   load. Missing/misnamed columns fail fast with an actionable message.
2. **LS-2 Adjustment determinism.** The LS solve is deterministic bit-for-bit on the same
   input. No `-ffast-math` in any C/C++ pulled in.
3. **LS-3 RPP adjacency table.** `rpp_adjacency.csv` is the audit artifact for relative-precision
   conformance; every pair appears with actual vs allowable and a pass/fail flag.
4. **LS-4 Encroachment tabulation.** `table_a_item20.csv` matches ALTA Table A Item 20 format.
   Any change to the column set requires a regulatory review note.
5. **LS-5 Civil 3D payload.** `civil3d_payload.json` is the handoff contract to the dotnet
   side. Schema is versioned; changes bump `schemaVersion`.
6. **LS-6 Dotnet bridge.** `laser-suite/dotnet` consumes the payload, pushes to Civil 3D via
   the managed API. Never manipulates the payload — transform is Python-side, dotnet is transport.
7. **LS-7 Run manifest.** Every run writes `manifest/run_manifest.json` with tool version,
   input SHA-256s, config hash, and output paths.

## Rules
- Formulas above are the canonical contract. Tests assert numerical output matches to the
  documented tolerance. Rewriting the math is a design review, not an inline change.
- No CRS transforms in this suite — the input bundle is assumed CRS-consistent upstream.
  A mismatched bundle fails fast; this suite does not reproject.
- `civil3d_payload.json` is append-schema-only. Removing a field breaks the dotnet contract.
- The dotnet side never writes to certified layers; all Civil 3D writes are on DRAFT or
  QA layers, consistent with TOTaLi's invariant §1.
- Determinism: identical bundle → identical artifacts, byte-for-byte.

## Gates
1. `pytest -q` green on the suite's pytest tree (see `pyproject.toml`).
2. Formula oracle tests: LS correction, pair covariance, RPP actual, RPP allowable, all
   within documented numerical tolerance on reference inputs.
3. Determinism test: two runs of `laser-suite run` on the same bundle produce byte-identical
   outputs (excluding timestamps in manifest).
4. Schema validation: `civil3d_payload.json` validates against its JSON schema.

## Tests required
- `tests/test_bundle_validation.py` — schema failures on malformed bundles.
- `tests/test_formulas.py` — each of LS-1..LS-4 formulas vs hand-computed reference.
- `tests/test_determinism.py` — double-run equivalence.
- `tests/test_civil3d_payload_schema.py` — schema + field-presence.

## Dependencies
- **Upstream:** TOTaLi normalized data (or direct operator bundle).
- **Downstream:** Civil 3D (via dotnet side).
- **External:** `numpy` / `scipy` for LS math; .NET 8+ for dotnet side.

## Open questions / known debts
- `.NET` unit test coverage — confirm dotnet side has parity test hooks. If not, add under
  `laser-suite/dotnet/tests/`.
- Document canonical tolerance (e.g., `|delta| < 1e-9 ft`) in `docs/formulas.md`; reuse in tests.

## Definition of Done
- LS-1..LS-7 implemented with tests.
- Oracle tests within tolerance.
- Determinism test green.
- Dotnet bridge consumes current `schemaVersion` payload without modification.
- `manifest/run_manifest.json` contains tool version, config hash, output SHA-256s.

## Progress (append-only)
- 2026-06-17 — config numeric coercion (YAML `1.0e15` strings); adjustment accepts dof==0; pass_case sample gains redundant obs D–B; `python/tests/` 23/23 green.
