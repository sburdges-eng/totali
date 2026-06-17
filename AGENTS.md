# AGENTS.md

## Dev-root boundary

This file is project-local. `~/Dev` is only the workspace container; once a session is inside this repo, use this file plus `CLAUDE.md` and project docs as the active rules.

- Scope searches, build commands, tests, terrain-pipeline checks, and dataset validation to this repository unless a sibling repo is explicitly named.
- Load project-specific code intelligence, schema checks, GIS/database tooling, dataset tooling, and CI checks from this repo's docs only.
- If work touches another folder inside `~/Dev`, switch to that folder's own `AGENTS.md` before editing there.
- Do not refresh or rely on a `~/Dev`-wide index as the source of truth for this project.

## Cursor Cloud specific instructions

This is a pure-Python monorepo with four sub-projects. No Docker, Node.js, or external services are required for development.

### Cloud bootstrap (run first)

Use the shared bootstrap script to preinstall test dependencies and editable packages:

```bash
bash tools/bootstrap_cloud_agent_env.sh
```

This installs `pytest`, `scipy`, `pydantic`, and `pyarrow` (RC gate / parquet) explicitly, then installs the root test-relevant subset from `requirements.txt` (`numpy`, `scipy`, `laspy`, `pyproj`, `pyyaml`, `click`, `pydantic`) and editable installs for `totali`, `laser-suite`, and `survey-automation-roadmap`.

### Sub-projects

| Sub-project | Directory | Install | Test |
|---|---|---|---|
| **totali** (core pipeline) | `/workspace` (root) | `pip install -e .` | `python3 -m pytest tests/` |
| **laser-suite** | `laser-suite/` | `pip install -e laser-suite/` | `cd laser-suite && python3 -m pytest python/tests/` |
| **survey-automation** | `survey-automation-roadmap/` | `pip install -e survey-automation-roadmap/` | `cd survey-automation-roadmap && python3 -m pytest tests/` |
| **groundtruthos-data** | `groundtruthos-data/` | deps installed manually (broken `pyproject.toml` build-backend) | Requires PostgreSQL+PostGIS (optional) |

### Known issues (pre-existing, not introduced by setup)

- **venv pip**: If `.venv/bin/pip` fails with `pip._vendor.rich._emoji_codes`, reinstall pip: `uv pip install --reinstall pip --python .venv/bin/python` (or recreate the venv).
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

## Agentic completion protocol

For fully agentic / autonomous work, follow `AGENTIC_COMPLETION_PLAN.md` (top-level wire) and the target module's `AGENTIC.md`. Read order per session: `AGENTIC_COMPLETION_PLAN.md` → `<module>/AGENTIC.md` → its Plan steps → tests → gates.

One `AGENTIC.md` exists per module under `totali/`, per tooling dir (`tests/`, `tools/`, `skills/`), and per sibling subproject (`survey-automation-roadmap/`, `AUTOMATICCAD/`, `laser-suite/`, `dwg-tool-parser/`, `totali-baton/`, `groundtruthos-data/`, `data-reroute/`).

Any C/C++ edit (in `dwg-tool-parser/`, auracad bridge, FFI surface, or vendored native deps) must follow `Docs/CXX_AGENTIC_RULES.md` — dangers, hard rules, sanitizer-backed workflows, debug strategies, review practices, pre-merge checklist. Read it before editing; propose amendments via PR, do not silently exempt.
