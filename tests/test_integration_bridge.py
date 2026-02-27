from pathlib import Path
import json
import pytest
from survey_automation.bridge import IntentBridge
from totali.pipeline.context import PipelineContext

def test_bridge_integration(tmp_path):
    # 1. Setup Mock Run Root
    run_root = tmp_path / "mock_run"
    norm_dir = run_root / "normalized"
    norm_dir.mkdir(parents=True)

    points_csv = norm_dir / "points.csv"
    points_csv.write_text(
        "point_id,northing,easting,elevation,description\n"
        "101,1000.0,2000.0,10.0,CP CONTROL\n"
        "102,1010.0,2010.0,11.0,CP CONTROL\n",
        encoding="utf-8"
    )

    # 2. Run Intent Bridge
    bridge = IntentBridge(run_root)
    bridge.bind_source()
    bridge.derive_intent(rules=[]) # Rules not implemented yet, using default grouping
    bridge.derive_geometry()

    output_dir = tmp_path / "bridge_out"
    bridge.export(output_dir)

    # 3. Verify Artifacts
    intent_path = output_dir / "intent_ir.json"
    assert intent_path.exists()

    with open(intent_path) as f:
        intent_data = json.load(f)
        assert intent_data["artifactType"] == "intent_ir"
        assert len(intent_data["data"]["features"]) == 1
        assert intent_data["data"]["features"][0]["group_name"] == "CP"

    # 4. Integrate with Totali Context
    ctx = PipelineContext(
        input_path="dummy.las",
        output_dir=tmp_path / "totali_out"
    )

    # Simulate loading external intent data
    ctx.merge_data({"survey_intent": intent_data})

    assert ctx.survey_intent is not None
    assert ctx.survey_intent["data"]["features"][0]["group_name"] == "CP"
