"""Contract surface for TOTaLi ↔ auracad integration.

auracad is the deterministic native core (geometry kernel, scene graph, CAD I/O).
TOTaLi expects three capabilities from auracad:

1. DWG/DXF read — returns a schema-valid dict matching
   `dwg-tool-parser/schema/parser_output.schema.json`.
2. Geometry healing — polyline/polygon repair with quarantine reporting
   (TOTaLi's shield can delegate to auracad's heavier-weight healer once the
   FFI is wired).
3. CRS transform — must route through the same PROJ version TOTaLi uses.

This module defines the contract only. The real adapter (FFI or subprocess
bridge) lives in a future feature branch and must satisfy the
`AuracadAdapter` Protocol.

See `~/Dev/auracad/docs/PRODUCTION_DESIGN_ALIGNMENT.md` for auracad's side
of the contract.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator


# Frozen contract version. Bump MAJOR when breaking; MINOR for additive fields.
AURACAD_CONTRACT_VERSION = "1.0.0"


class AuracadReadReport(BaseModel):
    """Output of `AuracadAdapter.read_cad(path)`. Must validate against
    `dwg-tool-parser/schema/parser_output.schema.json`.

    auracad is authoritative for the geometry it returns; this report is
    consumed by TOTaLi's `cad_shielding` phase as input to the healer.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(default=AURACAD_CONTRACT_VERSION)
    file: str
    format: str  # dxf | dwg
    version: str | None = None
    layers: list[dict[str, Any]] = Field(default_factory=list)
    entities: list[dict[str, Any]] = Field(default_factory=list)
    blocks: list[dict[str, Any]] = Field(default_factory=list)
    survey_tags: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("format")
    @classmethod
    def _known_format(cls, v: str) -> str:
        if v not in {"dxf", "dwg"}:
            raise ValueError(f"format must be 'dxf' or 'dwg', got {v!r}")
        return v


class AuracadHealReport(BaseModel):
    """Result of `AuracadAdapter.heal_geometry(...)`.

    Quarantine counts and healing stats. TOTaLi's shield emits a `heal` audit
    event with these values — keep field names stable across versions.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(default=AURACAD_CONTRACT_VERSION)
    input_entity_count: int = Field(ge=0)
    healed_count: int = Field(ge=0)
    quarantined_count: int = Field(ge=0)
    passed_count: int = Field(ge=0)
    issues: list[str] = Field(default_factory=list)
    quarantined_ids: list[str] = Field(default_factory=list)


class AuracadTransformReport(BaseModel):
    """Result of `AuracadAdapter.transform_crs(xyz, source_epsg, target_epsg)`.

    The transform itself must use the same PROJ version TOTaLi does — the
    report records the actual PROJ version used so TOTaLi can detect drift
    and emit a `transform` audit event with matching metadata.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(default=AURACAD_CONTRACT_VERSION)
    source_epsg: int
    target_epsg: int
    proj_version: str  # e.g. "9.4.1"
    point_count: int = Field(ge=0)
    bounds_pre: tuple[float, float, float, float, float, float] | None = None
    bounds_post: tuple[float, float, float, float, float, float] | None = None


class AuracadAdapter(Protocol):
    """The runtime contract an auracad adapter must satisfy.

    Implementations (future feature branches) may be:
    - C FFI wrappers via `ctypes` / `cffi`
    - pybind11 Python modules
    - Subprocess bridges emitting JSON over stdout

    Whatever the transport, the interface is the same.
    """

    def read_cad(self, path: str) -> AuracadReadReport:
        """Read a DWG/DXF file; return schema-valid report. Raises on I/O or
        parse failure — never returns silently incomplete data."""
        ...

    def heal_geometry(
        self,
        polylines: list[list[tuple[float, float, float]]],
        tolerance: float,
    ) -> AuracadHealReport:
        """Heal a batch of polylines. Tolerance is the close/snap threshold in
        coordinate units. Unhealable geometry goes to `quarantined_ids`, never
        into the returned healed set."""
        ...

    def transform_crs(
        self,
        xyz: list[tuple[float, float, float]],
        source_epsg: int,
        target_epsg: int,
    ) -> AuracadTransformReport:
        """CRS-to-CRS transform. MUST use the same PROJ version TOTaLi does;
        the returned `proj_version` lets TOTaLi detect drift."""
        ...
