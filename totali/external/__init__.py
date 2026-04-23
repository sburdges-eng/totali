"""External-service contracts: how TOTaLi consumes auracad, L4L, and other sibling projects.

These modules are **contract surfaces only** — Pydantic models + Protocol
definitions that pin what TOTaLi expects from its external dependencies.
No actual RPC, FFI, or network code lives here. When the real integration
lands (future feature branches), the implementing adapter will import from
these contracts and be statically checkable against them.

Invariants enforced by the contracts:

- All external-service output carries `authoritative: bool = False` and
  refuses any truthy value at construction (mirrors S-7 in
  `totali/pipeline/models.py::ClassificationResult`).
- Every response model carries a `schema_version` string so adapter drift
  is explicit.
- No external service has a direct mutation path into TOTaLi's pipeline.
  Outputs enter the pipeline only through validator gates (e.g., the
  `SurveyorLinter` `DRAFT`-layer discipline + PLS review).

References:
- `Docs/PRODUCTION_DESIGN_REFERENCE.md` — north-star architecture
- `Docs/TOTALI_MAPPING_TO_PRODUCTION_DESIGN.md` §Integration — what TOTaLi owns vs siblings
- Sibling projects: `~/Dev/auracad/docs/PRODUCTION_DESIGN_ALIGNMENT.md`,
  `~/Dev/liberals4liberty/docs/PRODUCTION_DESIGN_ALIGNMENT.md`
"""

from totali.external.auracad_contract import (
    AuracadAdapter,
    AuracadHealReport,
    AuracadReadReport,
    AuracadTransformReport,
)
from totali.external.l4l_contract import (
    L4LAnomalyScore,
    L4LInferenceAdapter,
    L4LProposal,
    L4LSceneEmbedding,
)

__all__ = [
    "AuracadAdapter",
    "AuracadHealReport",
    "AuracadReadReport",
    "AuracadTransformReport",
    "L4LAnomalyScore",
    "L4LInferenceAdapter",
    "L4LProposal",
    "L4LSceneEmbedding",
]
