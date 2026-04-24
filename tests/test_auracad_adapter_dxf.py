"""Tests for :class:`totali.external.auracad_adapter_dxf.DxfAuracadAdapter`.

Phase 1 of the AuracadAdapter roll-out: a local DXF-backed adapter that
closes ``AuracadAdapter.read_cad`` by delegating to the in-tree
ezdxf-backed parser at ``dwg-tool-parser/scripts/parse_dxf.py``.

These tests assert:

* the concrete class structurally satisfies the :class:`AuracadAdapter`
  Protocol (the Protocol is not ``@runtime_checkable``, so we probe each
  method by attribute + callability instead of using ``isinstance``),
* ``read_cad`` returns an :class:`AuracadReadReport` that schema-validates
  against the frozen ``dwg-tool-parser/schema/parser_output.schema.json``
  when dumped back with camelCase aliases,
* the fixture's TOTaLi-SURV draft layers round-trip through the adapter,
* the stub methods raise :class:`NotImplementedError` with actionable
  guidance,
* missing files propagate (exception type is parser-implementation detail
  so we only assert *something* was raised).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# ``parse_dxf`` needs ezdxf. Skip the whole module when it isn't installed.
pytest.importorskip("ezdxf")

from totali.external.auracad_adapter_dxf import DxfAuracadAdapter
from totali.external.auracad_contract import AuracadAdapter, AuracadReadReport

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DXF = REPO_ROOT / "dwg-tool-parser" / "fixtures" / "sample.dxf"
SCHEMA_PATH = REPO_ROOT / "dwg-tool-parser" / "schema" / "parser_output.schema.json"

_BOUNDARY_LAYER = "TOTaLi-SURV-BOUNDARY-DRAFT"
_MONUMENT_LAYER = "TOTaLi-SURV-MONUMENT-DRAFT"


@pytest.fixture
def adapter() -> DxfAuracadAdapter:
    return DxfAuracadAdapter()


@pytest.fixture(scope="module")
def sample_dxf_path() -> str:
    assert FIXTURE_DXF.exists(), (
        f"committed DXF fixture missing at {FIXTURE_DXF}; "
        "regenerate via tests/test_dwg_parser_ezdxf.py first"
    )
    return str(FIXTURE_DXF)


# ===================================================================
# 1. Protocol conformance
# ===================================================================


class TestProtocolConformance:
    """``DxfAuracadAdapter`` structurally satisfies :class:`AuracadAdapter`.

    The Protocol is not decorated ``@runtime_checkable`` — ``isinstance``
    against a bare Protocol raises ``TypeError``. We instead probe each
    method name for presence + callability; this is what static type
    checkers do under the hood for structural subtyping.
    """

    _PROTOCOL_METHODS = ("read_cad", "heal_geometry", "transform_crs")

    def test_instance_exposes_all_protocol_methods(self, adapter):
        for name in self._PROTOCOL_METHODS:
            attr = getattr(adapter, name, None)
            assert attr is not None, f"missing Protocol method {name!r}"
            assert callable(attr), f"{name!r} is not callable"

    def test_protocol_itself_declares_expected_methods(self):
        # Sanity-check: if the Protocol ever drops a method, this test
        # fails loudly so conformance doesn't silently degrade.
        for name in self._PROTOCOL_METHODS:
            assert hasattr(AuracadAdapter, name), (
                f"AuracadAdapter Protocol lost method {name!r}"
            )

    def test_isinstance_with_runtime_checkable_would_require_decorator(self):
        # Document the reason we don't use isinstance here: the Protocol
        # is intentionally not runtime-checkable (contracts file is
        # frozen). If someone adds @runtime_checkable later, this test
        # will start failing and signal the conformance check should
        # switch to ``isinstance``.
        with pytest.raises(TypeError):
            isinstance(DxfAuracadAdapter(), AuracadAdapter)


# ===================================================================
# 2. read_cad happy path
# ===================================================================


class TestReadCadHappyPath:
    def test_returns_read_report(self, adapter, sample_dxf_path):
        report = adapter.read_cad(sample_dxf_path)
        assert isinstance(report, AuracadReadReport)

    def test_format_is_dxf(self, adapter, sample_dxf_path):
        report = adapter.read_cad(sample_dxf_path)
        assert report.format == "dxf"

    def test_schema_version_pinned(self, adapter, sample_dxf_path):
        report = adapter.read_cad(sample_dxf_path)
        assert report.schema_version == "1.0.0"

    def test_totali_surv_layers_present(self, adapter, sample_dxf_path):
        report = adapter.read_cad(sample_dxf_path)
        names = {layer["name"] for layer in report.layers}
        assert _BOUNDARY_LAYER in names
        assert _MONUMENT_LAYER in names

    def test_at_least_one_entity(self, adapter, sample_dxf_path):
        report = adapter.read_cad(sample_dxf_path)
        assert len(report.entities) >= 1

    def test_errors_empty_on_clean_parse(self, adapter, sample_dxf_path):
        report = adapter.read_cad(sample_dxf_path)
        assert report.errors == []


# ===================================================================
# 3. Round-trip back through the JSON Schema
# ===================================================================


class TestReadCadRoundTripsThroughSchema:
    """The adapter's Pydantic output must still validate against the
    committed ``parser_output.schema.json`` when dumped with camelCase
    aliases (the schema's canonical shape)."""

    def test_model_dump_validates_against_parser_schema(
        self, adapter, sample_dxf_path,
    ):
        jsonschema = pytest.importorskip("jsonschema")

        report = adapter.read_cad(sample_dxf_path)

        # The Pydantic model uses snake_case ``schema_version`` but the
        # JSON Schema requires camelCase ``schemaVersion``. The model
        # doesn't declare an alias, so we rename manually on dump.
        dumped = report.model_dump()
        dumped["schemaVersion"] = dumped.pop("schema_version")

        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        jsonschema.validate(instance=dumped, schema=schema)


# ===================================================================
# 4. NotImplemented stubs
# ===================================================================


class TestNotImplementedMethods:
    def test_heal_geometry_raises_not_implemented(self, adapter):
        with pytest.raises(NotImplementedError) as excinfo:
            adapter.heal_geometry([[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)]], 0.001)
        msg = str(excinfo.value)
        assert "heal_geometry" in msg
        assert "not implemented" in msg

    def test_transform_crs_raises_not_implemented(self, adapter):
        with pytest.raises(NotImplementedError) as excinfo:
            adapter.transform_crs([(0.0, 0.0, 0.0)], 4326, 3857)
        msg = str(excinfo.value)
        assert "transform_crs" in msg
        assert "not implemented" in msg


# ===================================================================
# 5. Missing file propagation
# ===================================================================


class TestMissingFilePropagates:
    def test_missing_path_raises(self, adapter):
        # Pin to OSError (covers FileNotFoundError). parse_dxf explicitly
        # raises FileNotFoundError for missing paths; a KeyError / AttributeError
        # / ValidationError escaping this test would signal a code defect
        # masquerading as "something raised".
        with pytest.raises(OSError):
            adapter.read_cad("/nonexistent/path/definitely_not_a_dxf.dxf")
