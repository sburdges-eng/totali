import numpy as np
from pathlib import Path
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import CRSMetadata, PointCloudStats, ClassificationResult, ExtractionResult, HealingReport, LintItem

def verify():
    # 1. Instantiate Context
    ctx = PipelineContext(
        input_path="fake.las",
        output_dir=Path("out"),
        points_xyz=np.zeros((10, 3)),
        crs=CRSMetadata(epsg_code=2231, is_valid=True),
        stats=PointCloudStats(point_count=10),
    )
    print("Context initialized.")

    # 2. Simulate Phase 2 (Segmentation) Output
    cls_result = ClassificationResult(
        labels=np.zeros(10, dtype=np.int32),
        confidences=np.ones(10, dtype=np.float32),
    )
    ctx.merge_data({"classification": cls_result})
    assert ctx.classification is not None
    print("Segmentation data merged.")

    # 3. Simulate Phase 3 (Extraction) Output
    ext_result = ExtractionResult(
        dtm_faces=np.zeros((5, 3), dtype=np.int32),
        breaklines=[np.zeros((2, 3))],
    )
    ctx.merge_data({"extraction": ext_result})
    assert ctx.extraction is not None
    print("Extraction data merged.")

    # 4. Simulate Phase 4 (Shielding) Output
    healing = HealingReport(input_entity_count=5, healed_count=1)
    manifest = {"entities": [{"id": "1", "type": "LINE", "layer": "L"}]}
    ctx.merge_data({"healing": healing, "manifest": manifest})
    assert ctx.healing is not None
    assert ctx.manifest is not None
    print("Shielding data merged.")

    # 5. Simulate Phase 5 (Linting) Output
    items = [LintItem(item_id="1", geometry_type="LINE", layer="L")]
    ctx.merge_data({"lint_items": items})
    assert len(ctx.lint_items) == 1
    print("Linting data merged.")

    print("Data flow verification successful.")

if __name__ == "__main__":
    verify()
