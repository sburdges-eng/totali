PYTHON ?= python3
PDAL_PIPELINE_TEMPLATE ?= config/pdal_pipeline_template.json
PDAL_BATCH_RUNNER ?= pipeline/run_pdal_batch.py
PDAL ?= pdal

INPUT_DIR ?= storage/input_lidar
INPUT_PATTERN ?= *.laz
OUTPUT_DIR ?= storage/pdal_batch
MANIFEST_FILE ?= $(OUTPUT_DIR)/manifest.json

INPUT_SRID ?= EPSG:26915
OUTLIER_MEAN_K ?= 8
OUTLIER_MULTIPLIER ?= 3
DECIMATION_STEP ?= 2
CHIPPER_CAPACITY ?= 500
PG_CONNECTION ?= host='localhost' dbname='lidar' user='lidar' password='lidar'
PG_TABLE ?= pc_patches
PG_SRID ?= 26915
COPC_THREADS ?= 8
RETRIES ?= 2

.PHONY: all batch validate manifest clean help

all: batch

help:
	@echo "Targets:"
	@echo "  batch     Run PDAL batch tiling pipeline for all inputs (default: storage/input_lidar/*.laz)"
	@echo "  validate  Validate runner config and print discovered inputs"
	@echo "  manifest  Show last manifest"
	@echo "  clean     Remove output artifacts in $(OUTPUT_DIR)"
	@echo ""
	@echo "Override examples:"
	@echo "  make batch INPUT_DIR=... OUTPUT_DIR=... PG_TABLE=... PG_CONNECTION='host=... dbname=... user=... password=...'"

validate:
	@test -f "$(PDAL_BATCH_RUNNER)" || (echo "Missing runner: $(PDAL_BATCH_RUNNER)" && exit 1)
	@test -f "$(PDAL_PIPELINE_TEMPLATE)" || (echo "Missing template: $(PDAL_PIPELINE_TEMPLATE)" && exit 1)
	@$(PYTHON) - <<'PY'
import glob
import sys

files = sorted(glob.glob("$(INPUT_DIR)/$(INPUT_PATTERN)"))
if not files:
    print("No input files matched", file=sys.stderr)
    sys.exit(1)
print(f"Found {len(files)} inputs:")
for path in files:
    print(path)
PY

batch: validate
	@mkdir -p "$(OUTPUT_DIR)/copc" "$(OUTPUT_DIR)/pipeline-json" "$(OUTPUT_DIR)/logs"
	$(PYTHON) "$(PDAL_BATCH_RUNNER)" \
	  --template "$(PDAL_PIPELINE_TEMPLATE)" \
	  --runner-pdal "$(PDAL)" \
	  --input-dir "$(INPUT_DIR)" \
	  --input-pattern "$(INPUT_PATTERN)" \
	  --output-dir "$(OUTPUT_DIR)" \
	  --manifest "$(MANIFEST_FILE)" \
	  --input-srid "$(INPUT_SRID)" \
	  --outlier-mean-k "$(OUTLIER_MEAN_K)" \
	  --outlier-multiplier "$(OUTLIER_MULTIPLIER)" \
	  --decimation-step "$(DECIMATION_STEP)" \
	  --chipper-capacity "$(CHIPPER_CAPACITY)" \
	  --pg-connection "$(PG_CONNECTION)" \
	  --pg-table "$(PG_TABLE)" \
	  --pg-srid "$(PG_SRID)" \
	  --copc-threads "$(COPC_THREADS)" \
	  --retries "$(RETRIES)"

manifest:
	@cat "$(MANIFEST_FILE)"

clean:
	@rm -rf "$(OUTPUT_DIR)"
