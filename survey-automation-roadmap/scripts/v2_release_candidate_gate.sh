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

EVAL_GATE_METRICS_PATH="${EVAL_GATE_METRICS_PATH:-validation/eval/current_metrics.json}"
EVAL_GATE_BASELINE_PATH="${EVAL_GATE_BASELINE_PATH:-validation/eval/baseline_metrics.json}"
EVAL_GATE_THRESHOLDS_PATH="${EVAL_GATE_THRESHOLDS_PATH:-config/eval_gate.example.yaml}"
EVAL_GATE_REPORT_PATH="${EVAL_GATE_REPORT_PATH:-artifacts/eval_gate_report.json}"

if [[ ! -f "$EVAL_GATE_METRICS_PATH" ]]; then
  echo "[v2-rc-gate] Missing eval metrics file: $EVAL_GATE_METRICS_PATH" >&2
  exit 1
fi
if [[ ! -f "$EVAL_GATE_BASELINE_PATH" ]]; then
  echo "[v2-rc-gate] Missing eval baseline file: $EVAL_GATE_BASELINE_PATH" >&2
  exit 1
fi
if [[ ! -f "$EVAL_GATE_THRESHOLDS_PATH" ]]; then
  echo "[v2-rc-gate] Missing eval thresholds file: $EVAL_GATE_THRESHOLDS_PATH" >&2
  exit 1
fi

mkdir -p "$(dirname "$EVAL_GATE_REPORT_PATH")"
echo "[v2-rc-gate] Running continuous evaluation gate"
python3 scripts/eval_gate.py \
  --metrics "$EVAL_GATE_METRICS_PATH" \
  --baseline "$EVAL_GATE_BASELINE_PATH" \
  --thresholds "$EVAL_GATE_THRESHOLDS_PATH" \
  --output "$EVAL_GATE_REPORT_PATH"

echo "[v2-rc-gate] Validating artifact contract and strict arbitration"
LATEST_RUN_ROOT="$(find artifacts -mindepth 1 -maxdepth 1 -type d -name 'pt2-gate-*' | sort | tail -n 1)"
if [[ -z "$LATEST_RUN_ROOT" ]]; then
  echo "[v2-rc-gate] Unable to locate PT II run artifacts under artifacts/pt2-gate-*" >&2
  exit 1
fi
PYTHONPATH=src python3 -m survey_automation.cli bridge \
  --run-root "$LATEST_RUN_ROOT" \
  --rules config/bridge_rules.example.yaml
PYTHONPATH=src python3 -m survey_automation.cli arbitrate \
  --run-root "$LATEST_RUN_ROOT" \
  --eval-report "$EVAL_GATE_REPORT_PATH"

echo "[v2-rc-gate] Release-candidate gate complete"
