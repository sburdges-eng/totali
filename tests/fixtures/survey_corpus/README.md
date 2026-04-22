# Synthetic Survey Corpus

Test-only fixture for `DeterministicExtractor` regression + determinism harnesses.

## Provenance

Synthetic, seeded. Not derived from any real survey. Generated with
`numpy.random.default_rng(seed=42)` on 2026-04-22 and committed to git.
The `.npy` bytes are the source of truth — regenerating is not required
for tests. If the fixture is ever regenerated, the SHA-256 recorded in
`test_extractor_corpus_determinism.py` should be updated in the same
commit.

## File: `synthetic_500pt.npy`

- NumPy `.npy` format (`allow_pickle=False`)
- Shape: `(500, 3)`
- Dtype: `float64`
- Columns: `x, y, z` in US survey feet

## Coordinate ranges

- `x`: `[100, 200]` ft
- `y`: `[100, 200]` ft
- `z`: `[100, 160]` ft, layered into 4 bands (200/100/100/100 points)
  to mirror the `sample_classification` fixture in `tests/conftest.py`

## CRS assumption

EPSG:2231 (NAD83 Colorado Central, US survey foot) post-validation.
Tests that need a validated CRS attach `CRSMetadata(epsg_code=2231,
is_valid=True)` to the `PipelineContext`.

## Intended use

- `DeterministicExtractor` determinism harness
  (`tests/test_extractor_corpus_determinism.py`)
- Any future extractor-level regression test that needs stable, committed
  point-cloud input. Do not use for end-to-end pipeline tests — those
  should exercise the full geodetic → segmentation → extraction chain on
  LAS input, not on this bypass fixture.
