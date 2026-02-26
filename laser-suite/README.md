# LASER Suite

Deterministic geodetic adjustment and compliance pipeline for 2026 ALTA/NSPS workflows.

## Install

```bash
pip install -e .
```

## Commands

- `laser-suite run --bundle-dir <dir> --config <yaml> --out <dir> [--run-id <id>]`
- `laser-suite laser --bundle-dir <dir> --config <yaml> --out <dir> [--run-id <id>]`
- `laser-suite encroachment --bundle-dir <dir> --config <yaml> --out <dir> [--run-id <id>]`
- `laser-suite export-civil3d --run-root <dir> [--out <dir>]`

## Canonical Input Bundle

Required CSV files in `bundle-dir`:

- `stations.csv`
- `observations.csv`
- `weights.csv`
- `adjacency.csv`
- `boundaries.csv`
- `improvements.csv`
- `easements.csv`
- `setbacks.csv`

## Key Formulas

- Weighted least-squares correction: `(A^T P A)^-1 A^T P l`
- Pair covariance propagation: `ΣΔ = Σii + Σjj - Σij - Σji`
- Relative precision actual: `RPP_actual = 2.448 * sqrt(λmax)`
- Relative precision allowable: `RPP_allowable = 0.02 + (50e-6 * distance_m)`

## Artifacts

- `artifacts/<run-id>/laser/adjustment_report.json`
- `artifacts/<run-id>/laser/rpp_adjacency.csv`
- `artifacts/<run-id>/encroachment/encroachment_report.json`
- `artifacts/<run-id>/encroachment/table_a_item20.csv`
- `artifacts/<run-id>/civil3d/civil3d_payload.json`
- `artifacts/<run-id>/manifest/run_manifest.json`
