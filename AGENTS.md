# AGENTS.md

## Cursor Cloud specific instructions

This is a pure-Python monorepo with four sub-projects. No Docker, Node.js, or external services are required for development.

### Sub-projects

| Sub-project | Directory | Install | Test |
|---|---|---|---|
| **totali** (core pipeline) | `/workspace` (root) | `pip install -e .` | `python3 -m pytest tests/` |
| **laser-suite** | `laser-suite/` | `pip install -e laser-suite/` | `cd laser-suite && python3 -m pytest python/tests/` |
| **survey-automation** | `survey-automation-roadmap/` | `pip install -e survey-automation-roadmap/` | `cd survey-automation-roadmap && python3 -m pytest tests/` |
| **groundtruthos-data** | `groundtruthos-data/` | deps installed manually (broken `pyproject.toml` build-backend) | Requires PostgreSQL+PostGIS (optional) |

### Known issues (pre-existing, not introduced by setup)

- **laser-suite**: 6 of 10 tests fail due to a `TypeError` in `laser_suite/config.py` (line 66, `condition_number_limit` compared as string vs int) and an `AdjustmentError` in `adjustment.py`. These are existing code bugs, not environment issues.
- **groundtruthos-data**: The `pyproject.toml` specifies an invalid build backend (`setuptools.backends._legacy:_Backend`), so `pip install -e .` fails. Dependencies are installed directly via `pip install` of the listed packages instead.
- **ruff** lint: Running `ruff check totali/` shows 13 pre-existing lint warnings (unused imports). No ruff config is committed; the linter runs with defaults.

### Running the CLIs

- `python3 -m totali.main --help` — TOTaLi pipeline (geodetic, segment, extract, shield, lint phases)
- `python3 -m survey_automation.cli --help` — Survey automation (run, validate, profile, doctor, etc.)
- `python3 -m laser_suite.cli --help` — Laser suite (run, laser, encroachment, export-civil3d)

### Notes

- Ensure `$HOME/.local/bin` is on `PATH` for pip-installed scripts (`pytest`, `ruff`, `laser-suite`, etc.).
- CI (`.github/workflows/ci.yml`) uses Python 3.11; the Cloud VM has Python 3.12 which is compatible.
- All totali tests are fully mocked — no real LAS files or ONNX models needed. See `tests/conftest.py`.
- The copilot instructions at `.github/copilot-instructions.md` contain authoritative guidance on the codebase structure, key files, and the critical `auto_promote = false` invariant.
