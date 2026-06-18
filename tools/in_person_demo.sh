#!/usr/bin/env bash
# TOTaLi in-person demo kit — offline, no SSD required.
#
#   ./tools/in_person_demo.sh
#
# Runs three tracks:
#   1. Coded survey (BV_BASE 12k points) — partner-style Carlson export
#   2. Real LAS (USGS tile) — LiDAR topo with geodetic gates + CRS inference
#   3. Seven-step sales Key Flow (synth LAS) — always-green narrative demo
#
# Prints paths to DXF outputs, audit logs, and pre-converted civil DWG→DXF files.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Local paths (override via environment). No SSD required at demo time.
DATA_ROOT="${TOTALI_DATA_ROOT:-$HOME/Dev/data}"
BV_BASE_ASC="${BV_BASE_ASC:-$DATA_ROOT/bv_base/from_ssd/BV_BASE_POINTS_ASCII.txt}"
TOTALI_FIELDCODE_FLD="${TOTALI_FIELDCODE_FLD:-$DATA_ROOT/bv_base/from_ssd/BV_BASE_MATCHING_FIELD_CODES.fld}"
TOTALI_PARTNER_LAS="${TOTALI_PARTNER_LAS:-$DATA_ROOT/bv_base/lidar/partner_candidate.las}"
SURVEY_CONVERTED="${SURVEY_CONVERTED:-$DATA_ROOT/survey/converted}"
LIBREDWG_PREFIX="${LIBREDWG_PREFIX:-$HOME/Dev/CAD/vendor/libredwg}"
export PATH="${LIBREDWG_PREFIX}/bin:${PATH}"
export DYLD_LIBRARY_PATH="${LIBREDWG_PREFIX}/lib${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
export TOTALI_DATA_ROOT="$DATA_ROOT"

PYTHON="${REPO}/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "error: missing venv at ${REPO}/.venv — run: pip install -e \".[dev]\" lazrs" >&2
  exit 1
fi

OUT="${IN_PERSON_OUT:-$HOME/Dev/data/in_person_demo}"
mkdir -p "$OUT"
CONFIG="${REPO}/config/pipeline_in_person.yaml"
MANUAL_BASELINE="${MANUAL_BASELINE_SEC:-14400}"

_fail() { echo "FAIL: $*" >&2; exit 1; }

_check_data() {
  local missing=0
  for f in "$BV_BASE_ASC" "$TOTALI_FIELDCODE_FLD" "$TOTALI_PARTNER_LAS"; do
    if [[ ! -f "$f" ]]; then
      echo "  missing: $f" >&2
      missing=1
    fi
  done
  [[ "$missing" -eq 0 ]] || _fail "hydrate data first (see ~/Dev/data/bv_base/)"
}

_check_libredwg() {
  if command -v dwg2dxf >/dev/null 2>&1; then
    echo "  LibreDWG: $(command -v dwg2dxf)"
  else
    echo "  warn: dwg2dxf not on PATH — DWG→DXF previews skipped" >&2
  fi
}

echo "=== TOTaLi in-person demo ==="
echo "repo:  $REPO"
echo "out:   $OUT"
echo ""
_check_data
_check_libredwg
echo ""

# --- Track 1: coded survey (BV_BASE) ---
echo "--- [1/3] Coded survey: BV_BASE (12,040 pts) ---"
export TOTALI_FIELDCODE_FLD
export REPO CODED_OUT BV_BASE_ASC
CODED_OUT="$OUT/bv_base_coded"
export CODED_OUT
export BV_BASE_ASC
rm -rf "$CODED_OUT"
mkdir -p "$CODED_OUT"
"$PYTHON" - <<'PY' || _fail "coded survey pipeline"
import os
from pathlib import Path
from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log
from totali.pipeline.orchestrator import PipelineOrchestrator
import yaml

repo = Path(os.environ["REPO"])
out = Path(os.environ["CODED_OUT"])
asc = os.environ["BV_BASE_ASC"]
with open(repo / "config/pipeline_in_person.yaml") as f:
    cfg = yaml.safe_load(f)
audit = AuditLogger(log_dir=str(out / "audit"), project_id="INPERSON-BVBASE")
r = PipelineOrchestrator(cfg, audit, out).run(asc, phase="all")
if not r.success:
    raise SystemExit(r.phases)
ok, err = verify_log(audit.log_path)
if not ok:
    raise SystemExit(err)
dxf = out / "totali_draft_output.dxf"
print(f"  dxf: {dxf} ({dxf.stat().st_size:,} bytes)" if dxf.exists() else "  no dxf")
PY
echo ""

# --- Track 2: real LAS (USGS partner substitute) ---
echo "--- [2/3] Real LAS: USGS tile (gates active, CRS inference) ---"
LAS_OUT="$OUT/usgs_las"
LAS_AUDIT="$OUT/audit_las"
rm -rf "$LAS_OUT" "$LAS_AUDIT"
mkdir -p "$LAS_AUDIT"
TOTALI_PARTNER_LAS="$TOTALI_PARTNER_LAS" "$PYTHON" "$REPO/tools/run_partner_las_e2e.py" \
  --config "$CONFIG" \
  --output-dir "$LAS_OUT" \
  --audit-dir "$LAS_AUDIT" \
  --project-id "INPERSON-LAS" | "$PYTHON" -c "import sys,json; s=json.load(sys.stdin); print('  success:', s['success'], 'entities:', s.get('entity_count')); sys.exit(0 if s['success'] else 1)"
echo ""

# --- Track 3: sales Key Flow (synth — narrative) ---
echo "--- [3/3] Sales Key Flow (synth topo, 7 steps) ---"
SALES_OUT="$OUT/sales_keyflow"
rm -rf "$SALES_OUT"
mkdir -p "$SALES_OUT"
"$PYTHON" -m totali.sales_demo \
  --input "$REPO/tests/fixtures/survey_corpus/synth_topo.las" \
  --config "$CONFIG" \
  --output "$SALES_OUT" \
  --project-id "INPERSON-SALES" \
  --manual-baseline-sec "$MANUAL_BASELINE"
echo ""

# --- Deliverables summary ---
echo "=== Deliverables (open in Civil 3D / Carlson / QGIS) ==="
echo ""
echo "Pipeline DXF (TOTaLi DRAFT layers):"
echo "  BV coded:    $CODED_OUT/totali_draft_output.dxf"
echo "  USGS LAS:    $LAS_OUT/totali_draft_output.dxf"
echo "  Sales flow:  $SALES_OUT/totali_draft_output.dxf"
echo ""
echo "Pre-converted civil DWG → DXF (LibreDWG, reference only):"
if [[ -d "${SURVEY_CONVERTED:-}" ]]; then
  ls -lh "$SURVEY_CONVERTED"/*.dxf 2>/dev/null | awk '{print "  "$NF, "("$5")"}' || true
fi
echo ""
echo "Bridge / Ramsour source DXF:"
echo "  ${TOTALI_DATA_ROOT}/bv_base/from_ssd/TOTL/XR23173-Sur.dxf"
echo ""
echo "Audit logs:"
echo "  $CODED_OUT/audit/"
echo "  $LAS_AUDIT/"
echo "  $SALES_OUT/audit/"
echo ""
echo "Chaffee jurisdiction GPKG (18,247 parcels, EPSG:6428):"
echo "  ${TOTALI_DATA_ROOT}/survey/chaffee_county/chaffee_parcels_EPSG6428.gpkg"
echo ""
echo "=== In-person demo complete ==="
