#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -z "${CRD_CONVERTER_COMMAND:-}" ]]; then
  export CRD_CONVERTER_COMMAND="$ROOT_DIR/scripts/converter --input {input} --output {output}"
fi

echo "[pt2-gate] Running tests"
pytest -q

echo "[pt2-gate] Running converter preflight"
PYTHONPATH=src python3 -m survey_automation.cli check-converter --config config/pipeline.ci.yaml

echo "[pt2-gate] Running CI-config pipeline"
RUN_ID="pt2-gate-$(date -u +%Y%m%dT%H%M%SZ)"
set +e
PYTHONPATH=src python3 -m survey_automation.cli run \
  --input-dir . \
  --config config/pipeline.ci.yaml \
  --output-dir artifacts \
  --run-id "$RUN_ID"
PIPELINE_EXIT_CODE=$?
set -e

if [[ "$PIPELINE_EXIT_CODE" -ne 0 && "$PIPELINE_EXIT_CODE" -ne 2 ]]; then
  echo "[pt2-gate] Fatal pipeline exit code: $PIPELINE_EXIT_CODE"
  exit "$PIPELINE_EXIT_CODE"
fi

echo "[pt2-gate] Accepted pipeline exit code: $PIPELINE_EXIT_CODE"
echo "[pt2-gate] Running golden verification"
python3 validation/verify_golden.py
python3 validation/write_last_validation.py

echo "[pt2-gate] Complete (run_id=$RUN_ID, pipeline_exit=$PIPELINE_EXIT_CODE)"
