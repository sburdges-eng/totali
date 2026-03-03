# TOTaLi – Copilot Coding Agent Instructions

## What This Repository Does

TOTaLi is a **defensible spatial drafting pipeline** for land-survey work. It turns raw LiDAR point clouds into auditable DXF deliverables. Core principle: **AI Classifies → Algorithms Measure → Humans Certify**. No AI-generated geometry is ever auto-promoted to certified status; every entity requires explicit PLS (Professional Land Surveyor) acceptance.

The repo has four main components:

| Directory | Purpose |
|-----------|---------|
| `totali/` | 5-phase LiDAR pipeline (Python package) |
| `laser-suite/` | WLS/RPP geodetic adjustment engine |
| `survey-automation-roadmap/` | CSV/DXF/CRD survey data ingestion & QC |
| `AUTOMATICCAD/` | CAD corpus curation (in progress) |

## Critical Invariant – Never Bypass

`linting.auto_promote` is hardcoded `false` in `config/pipeline.yaml` and enforced in `totali/linting/surveyor_lint.py`. **Do not set it to `true`, remove the guard, or add any code path that promotes AI output to CERTIFIED status without human review.**

---

## Install & Bootstrap

**Python 3.11** is the target runtime (used in CI).

```bash
bash tools/bootstrap_cloud_agent_env.sh
```

The bootstrap script preinstalls test dependencies (`pytest`, `scipy`, `pydantic`), installs test-relevant root requirements from `requirements.txt`, and installs editable packages for `totali`, `laser-suite`, and `survey-automation-roadmap`.

For optional extras: `pip install -e ".[ml]"` (ONNX/Open3D), `pip install -e ".[cad]"` (ezdxf), `pip install -e ".[full]"` (everything).

---

## Running Tests

```bash
python3 -m pytest tests/          # full suite (~280 tests)
python3 -m pytest tests/ -v --tb=short   # verbose (same as CI)
```

`pytest.ini` sets `testpaths = tests`, so running `pytest` from the repo root is equivalent. All tests are in `tests/`. Test files follow the pattern `test_*.py`.

**No test requires a real LAS file or ONNX model** – all heavy dependencies are mocked in `tests/conftest.py`.

---

## CI Workflow

`.github/workflows/ci.yml` runs on push/PR to `main`:

1. Checkout
2. Set up Python 3.11
3. `pip install -e . && pip install pytest`
4. `python -m pytest tests/ -v --tb=short`

There is no separate lint or type-check step in CI. Ensure tests pass before opening a PR.

---

## Key Source Files

| File | Role |
|------|------|
| `totali/main.py` | CLI entry point (`click`); use `python -m totali.main` |
| `totali/pipeline/orchestrator.py` | Runs phases in sequence |
| `totali/pipeline/context.py` | `PipelineConfig` Pydantic model |
| `totali/pipeline/models.py` | Shared data models |
| `totali/audit/logger.py` | SHA-256-chained JSONL audit log |
| `totali/geodetic/gatekeeper.py` | Phase 1 – CRS/epoch validation |
| `totali/segmentation/classifier.py` | Phase 2 – ONNX ML inference |
| `totali/extraction/extractor.py` | Phase 3 – DTM/TIN/contours |
| `totali/cad_shielding/shield.py` | Phase 4 – DXF output via ezdxf |
| `totali/linting/surveyor_lint.py` | Phase 5 – ghost suggestions, accept/reject |
| `config/pipeline.yaml` | Single config file for all phases |
| `setup.py` | Package metadata and optional extras |
| `requirements.txt` | Full dependency list for development |

---

## Architecture Notes

- **Layer naming:** All AI-generated DXF layers use the `TOTaLi-SURV-*-DRAFT` / `TOTaLi-PLAN-*-DRAFT` convention. The `-DRAFT` suffix is removed only upon PLS promotion.
- **Audit trail:** Every pipeline event is appended to a SHA-256-chained JSONL file under `audit_logs/`. Call `AuditLogger.verify_chain()` to check integrity.
- **Pydantic v2:** `PipelineConfig` and related models use Pydantic v2 (`model_validate`, `model_dump`).
- **Click CLI:** `totali/main.py` uses Click. The entry point `totali-pipeline` is registered in `setup.py`.
- **Laser suite** lives in `laser-suite/python/laser_suite/` and has its own tests under `laser-suite/python/tests/`. Its CI is separate from the root `ci.yml`.
- **Survey automation** lives in `survey-automation-roadmap/` with its own README and test suite.

---

## Making Changes

1. Run `python3 -m pytest tests/` before and after your change to confirm no regressions.
2. For changes to `config/pipeline.yaml`, validate that `auto_promote` remains `false`.
3. For new pipeline phases, inherit from `totali/pipeline/base_phase.py` and register in `orchestrator.py`.
4. For audit-sensitive operations, call `audit.log(event_type, payload)` at the start and end.
5. Trust these instructions; only search the codebase further if something here is incomplete or appears incorrect.
