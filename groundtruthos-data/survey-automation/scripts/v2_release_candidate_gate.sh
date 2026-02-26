#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -z "${CRD_CONVERTER_COMMAND:-}" ]]; then
  export CRD_CONVERTER_COMMAND="$ROOT_DIR/scripts/converter --input {input} --output {output}"
fi

echo "[v2-rc-gate] Running PT II baseline gate"
scripts/pt2_quality_gate.sh

echo "[v2-rc-gate] Verifying documentation path references"
pytest -q tests/unit/test_docs_paths.py

echo "[v2-rc-gate] Validating production config discovery"
PYTHONPATH=src python3 -m survey_automation.cli validate \
  --input-dir . \
  --config config/pipeline.prod.yaml

echo "[v2-rc-gate] Running production converter preflight"
PYTHONPATH=src python3 -m survey_automation.cli check-converter \
  --config config/pipeline.prod.yaml

echo "[v2-rc-gate] Release-candidate gate complete"
