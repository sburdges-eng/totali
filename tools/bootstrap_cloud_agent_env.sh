#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

install_editable_if_present() {
  local project_dir="$1"
  if [[ -f "${project_dir}/setup.py" || -f "${project_dir}/pyproject.toml" ]]; then
    python3 -m pip install -e "${project_dir}"
  fi
}

python3 -m pip install --upgrade pip

# Explicit baseline requested for test execution.
python3 -m pip install pytest scipy pydantic

# Install only the requirements.txt entries exercised by the root test suite.
requirements_file="${ROOT_DIR}/requirements.txt"
if [[ -f "${requirements_file}" ]]; then
  mapfile -t root_test_requirements < <(python3 - "${requirements_file}" <<'PY'
from pathlib import Path
import re
import sys

wanted = {
    "numpy",
    "scipy",
    "laspy",
    "pyproj",
    "pyyaml",
    "click",
    "pydantic",
}

for raw in Path(sys.argv[1]).read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or line.startswith("-"):
        continue
    name = re.split(r"[<>=!~\[]", line, maxsplit=1)[0].strip().lower().replace("_", "-")
    if name in wanted:
        print(line)
PY
)
  if (( ${#root_test_requirements[@]} > 0 )); then
    python3 -m pip install "${root_test_requirements[@]}"
  fi
fi

# Editable installs ensure local package imports always resolve in tests.
install_editable_if_present "${ROOT_DIR}"
install_editable_if_present "${ROOT_DIR}/laser-suite"
install_editable_if_present "${ROOT_DIR}/survey-automation-roadmap"
