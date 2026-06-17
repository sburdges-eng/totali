"""Model component exports.

`totali.models` hosts the deterministic ONNX model loader (`loader.py`). The
former `projection.py` multimodal early-fusion projector (and its `coder_agent`
driver) were removed when the pipeline moved away from in-process LLM codegen;
import `totali.models.loader` directly.
"""

__all__: list[str] = []
