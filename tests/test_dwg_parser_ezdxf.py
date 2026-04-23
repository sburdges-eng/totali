"""Round-trip tests for the ezdxf-backed DXF parser (DP-3).

See ``dwg-tool-parser/AGENTIC.md``: DP-3 is the smallest useful parser —
an ``ezdxf``-backed pipeline that turns a DXF file into the canonical JSON
shape defined by ``dwg-tool-parser/schema/parser_output.schema.json``.

These tests:

* regenerate the committed DXF fixture (``fixtures/sample.dxf``) when it's
  missing so the round-trip is reproducible on any machine,
* assert the parser output validates against the committed JSON Schema,
* assert the shape of the output matches the fixture (specific layers,
  specific entities, the ``MON_MARKER`` block, empty ``errors``),
* assert the CLI returns 0 for a valid DXF and 3 for a missing file.

Skips cleanly (via ``pytest.importorskip``) when either ``ezdxf`` or
``jsonschema`` is absent — the production toolchain installs both, but
light dev environments may not.
"""

from __future__ import annotations

import json
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

ezdxf = pytest.importorskip("ezdxf")
jsonschema = pytest.importorskip("jsonschema")


# ---------------------------------------------------------------------------
# Paths / module loading
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
PARSER_DIR = REPO_ROOT / "dwg-tool-parser"
SCRIPTS_DIR = PARSER_DIR / "scripts"
SCHEMA_PATH = PARSER_DIR / "schema" / "parser_output.schema.json"
FIXTURE_DXF = PARSER_DIR / "fixtures" / "sample.dxf"

_BOUNDARY_LAYER = "TOTaLi-SURV-BOUNDARY-DRAFT"
_MONUMENT_LAYER = "TOTaLi-SURV-MONUMENT-DRAFT"
_BLOCK_NAME = "MON_MARKER"


def _load_parser_module():
    """Load ``parse_dxf`` by path — the directory name has a hyphen, so
    ``import scripts.parse_dxf`` only works when cwd is ``dwg-tool-parser/``.
    Tests should not depend on cwd, so we load by spec.
    """
    path = SCRIPTS_DIR / "parse_dxf.py"
    spec = spec_from_file_location("parse_dxf_script", path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def parse_dxf_module():
    return _load_parser_module()


@pytest.fixture(scope="module")
def schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Fixture-building helper (used to regenerate sample.dxf if missing)
# ---------------------------------------------------------------------------

def _build_sample_dxf(target: Path) -> None:
    """Create the canonical DP-3 fixture DXF at ``target``.

    Two TOTaLi layers, one LINE + one closed LWPOLYLINE on the boundary
    layer, one INSERT on the monument layer referencing a two-LINE
    ``MON_MARKER`` block.
    """
    doc = ezdxf.new("R2018")
    doc.layers.add(_BOUNDARY_LAYER, color=1)
    doc.layers.add(_MONUMENT_LAYER, color=2)

    block = doc.blocks.new(name=_BLOCK_NAME)
    block.add_line((-1.0, 0.0, 0.0), (1.0, 0.0, 0.0))
    block.add_line((0.0, -1.0, 0.0), (0.0, 1.0, 0.0))

    msp = doc.modelspace()
    msp.add_line(
        (100.0, 200.0, 0.0),
        (350.5, 200.0, 0.0),
        dxfattribs={"layer": _BOUNDARY_LAYER},
    )
    msp.add_lwpolyline(
        [(350.5, 200.0), (350.5, 425.75), (100.0, 425.75)],
        close=True,
        dxfattribs={"layer": _BOUNDARY_LAYER},
    )
    msp.add_blockref(
        _BLOCK_NAME,
        (100.0, 200.0, 0.0),
        dxfattribs={"layer": _MONUMENT_LAYER},
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    doc.saveas(str(target))


# ===================================================================
# 1. Fixture generation + schema round-trip
# ===================================================================

class TestGenerateFixture:
    """Regenerate ``sample.dxf`` if missing, then verify round-trip."""

    def test_fixture_exists_or_regenerate(self):
        if not FIXTURE_DXF.exists():
            _build_sample_dxf(FIXTURE_DXF)
        assert FIXTURE_DXF.exists(), "sample.dxf should exist after regen"
        # Small enough to commit: should be well under 100 KB.
        assert FIXTURE_DXF.stat().st_size < 100_000

    def test_fixture_round_trips_through_schema(self, parse_dxf_module, schema):
        if not FIXTURE_DXF.exists():
            _build_sample_dxf(FIXTURE_DXF)

        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        # Must not raise — surface the validation path on failure.
        jsonschema.validate(instance=report, schema=schema)


# ===================================================================
# 2. Parser output shape
# ===================================================================

class TestParseDxf:
    """The parser output matches the DP-3 spec on the committed fixture."""

    def test_required_top_level_keys(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        required = {
            "schemaVersion", "file", "format", "version",
            "layers", "entities", "blocks", "survey_tags", "errors",
        }
        assert required.issubset(report.keys())

    def test_format_and_schema_version(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        assert report["format"] == "dxf"
        assert report["schemaVersion"] == "1.0.0"

    def test_dxf_version_is_non_empty_string(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        assert isinstance(report["version"], str)
        assert report["version"].startswith("AC"), (
            f"expected AutoCAD version string, got {report['version']!r}"
        )

    def test_layers_contain_totali_surv_names(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        names = {layer["name"] for layer in report["layers"]}
        assert _BOUNDARY_LAYER in names
        assert _MONUMENT_LAYER in names

    def test_boundary_layer_counts_entities(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        by_name = {layer["name"]: layer["entity_count"] for layer in report["layers"]}
        # LINE + LWPOLYLINE on BOUNDARY.
        assert by_name[_BOUNDARY_LAYER] == 2
        # INSERT on MONUMENT.
        assert by_name[_MONUMENT_LAYER] == 1

    def test_entities_include_line_lwpolyline_insert(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        types = {e["type"] for e in report["entities"]}
        assert {"LINE", "LWPOLYLINE", "INSERT"}.issubset(types)

    def test_every_entity_has_string_handle_and_known_layer(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        declared = {layer["name"] for layer in report["layers"]}
        for entity in report["entities"]:
            assert isinstance(entity["id"], str) and entity["id"]
            assert entity["layer"] in declared, (
                f"entity {entity['id']} references undeclared layer {entity['layer']!r}"
            )

    def test_insert_entity_properties(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        inserts = [e for e in report["entities"] if e["type"] == "INSERT"]
        assert len(inserts) == 1
        props = inserts[0]["properties"]
        assert props["block_name"] == _BLOCK_NAME
        assert props["insertion_point"] == [100.0, 200.0, 0.0]

    def test_lwpolyline_is_closed_with_three_vertices(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        polys = [e for e in report["entities"] if e["type"] == "LWPOLYLINE"]
        assert len(polys) == 1
        props = polys[0]["properties"]
        assert props["closed"] is True
        assert len(props["vertices"]) == 3

    def test_blocks_contain_mon_marker(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        block_names = {block["name"] for block in report["blocks"]}
        assert _BLOCK_NAME in block_names

    def test_blocks_exclude_model_and_paper_space(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        block_names = {block["name"].lower() for block in report["blocks"]}
        assert "*model_space" not in block_names
        assert "*paper_space" not in block_names

    def test_mon_marker_contains_two_entities(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        by_name = {block["name"]: block["entity_count"] for block in report["blocks"]}
        assert by_name[_BLOCK_NAME] == 2

    def test_survey_tags_empty(self, parse_dxf_module):
        # DP-3 is tag-less; DP-5 extends with survey tag mapping.
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        assert report["survey_tags"] == []

    def test_errors_empty_on_clean_parse(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(FIXTURE_DXF)
        assert report["errors"] == []

    def test_file_field_echoes_input(self, parse_dxf_module):
        report = parse_dxf_module.parse_dxf(str(FIXTURE_DXF))
        assert report["file"] == str(FIXTURE_DXF)


# ===================================================================
# 3. CLI exit behavior
# ===================================================================

class TestCLIExit:
    """``python -m scripts.parse_dxf`` has the documented exit contract."""

    def _run_cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "scripts.parse_dxf", *args],
            cwd=str(PARSER_DIR),
            capture_output=True,
            text=True,
            check=False,
        )

    def test_valid_dxf_exits_zero_and_writes_schema_valid_json(
        self, tmp_path, schema,
    ):
        # Guard: regenerate the fixture if somehow missing.
        if not FIXTURE_DXF.exists():
            _build_sample_dxf(FIXTURE_DXF)

        out_path = tmp_path / "report.json"
        result = self._run_cli("fixtures/sample.dxf", "--output", str(out_path))
        assert result.returncode == 0, (
            f"CLI exited {result.returncode}; stderr={result.stderr!r}"
        )
        assert out_path.exists(), "CLI did not write --output file"

        report = json.loads(out_path.read_text(encoding="utf-8"))
        jsonschema.validate(instance=report, schema=schema)

    def test_valid_dxf_stdout_is_schema_valid_when_no_output_flag(self, schema):
        if not FIXTURE_DXF.exists():
            _build_sample_dxf(FIXTURE_DXF)

        result = self._run_cli("fixtures/sample.dxf")
        assert result.returncode == 0
        report = json.loads(result.stdout)
        jsonschema.validate(instance=report, schema=schema)

    def test_missing_file_exits_three(self, tmp_path):
        missing = tmp_path / "does_not_exist.dxf"
        result = self._run_cli(str(missing))
        assert result.returncode == 3, (
            f"expected exit 3 for missing file, got {result.returncode}; "
            f"stderr={result.stderr!r}"
        )
