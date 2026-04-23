"""Structural tests for the dwg-tool-parser output JSON schema.

Covers DP-2 (`dwg-tool-parser/AGENTIC.md`): the parser output contract is
formalized as a JSON Schema + a golden example. This test file validates
the schema's own shape, verifies the golden example validates against it,
and asserts that invariants like `additionalProperties: false` and
required-field enforcement actually bite.

This is schema-only. DP-1, DP-3..DP-7 (the real parser) are out of scope
here — see the per-step test files listed in AGENTIC.md §"Tests required".
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path

import pytest

# jsonschema may not be installed in every dev environment; skip cleanly
# rather than hand-roll validation (which would defeat the whole point of
# pinning to a standards-compliant validator).
jsonschema = pytest.importorskip("jsonschema")


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "dwg-tool-parser" / "schema" / "parser_output.schema.json"
GOLDEN_PATH = REPO_ROOT / "dwg-tool-parser" / "fixtures" / "golden_example.json"

# Semver with optional pre-release / build-metadata suffixes.
_SEMVER_RE = re.compile(
    r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)

_REQUIRED_TOP_LEVEL = {
    "schemaVersion",
    "file",
    "format",
    "version",
    "layers",
    "entities",
    "blocks",
    "survey_tags",
    "errors",
}


def _load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _load_golden() -> dict:
    return json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))


class TestSchemaShape:
    """The schema document itself is well-formed and declares the DP-2 contract."""

    def test_schema_file_exists(self):
        assert SCHEMA_PATH.exists(), f"schema missing at {SCHEMA_PATH}"

    def test_schema_is_valid_json(self):
        schema = _load_schema()
        assert isinstance(schema, dict)

    def test_declares_draft_and_id(self):
        schema = _load_schema()
        assert "$schema" in schema, "missing $schema declaration"
        assert "$id" in schema, "missing $id declaration"
        # Draft-07 or newer JSON Schema meta-schema URL.
        assert "json-schema.org" in schema["$schema"]

    def test_root_is_object_type(self):
        schema = _load_schema()
        assert schema.get("type") == "object"

    def test_root_closes_additional_properties(self):
        schema = _load_schema()
        assert schema.get("additionalProperties") is False, (
            "root object must set additionalProperties: false so unknown "
            "top-level keys are rejected"
        )

    def test_required_top_level_fields_declared(self):
        schema = _load_schema()
        required = set(schema.get("required", []))
        assert _REQUIRED_TOP_LEVEL.issubset(required), (
            f"missing required fields: {_REQUIRED_TOP_LEVEL - required}"
        )

    def test_required_top_level_fields_have_properties(self):
        schema = _load_schema()
        props = schema.get("properties", {})
        for key in _REQUIRED_TOP_LEVEL:
            assert key in props, f"declared required but not defined: {key}"

    def test_schema_version_is_semver_shaped(self):
        schema = _load_schema()
        pat = schema["properties"]["schemaVersion"].get("pattern", "")
        # The schema itself must pin schemaVersion to semver shape.
        assert pat, "schemaVersion has no pattern constraint"
        # The pattern must accept canonical semver.
        assert re.compile(pat).match("1.0.0")
        assert re.compile(pat).match("10.20.30-rc.1+build.7")
        assert not re.compile(pat).match("v1.0")

    def test_format_is_enum_dxf_or_dwg(self):
        schema = _load_schema()
        fmt = schema["properties"]["format"]
        assert fmt.get("enum") == ["dxf", "dwg"]

    def test_version_allows_null(self):
        schema = _load_schema()
        version_type = schema["properties"]["version"].get("type")
        # Accept either ["string","null"] or {"oneOf":[...]} forms.
        assert version_type == ["string", "null"] or version_type == [
            "null",
            "string",
        ]

    def test_errors_severity_enum(self):
        schema = _load_schema()
        sev = schema["properties"]["errors"]["items"]["properties"]["severity"]
        assert set(sev.get("enum", [])) == {"info", "warn", "error"}

    def test_entity_count_fields_are_non_negative_ints(self):
        schema = _load_schema()
        for key in ("layers", "blocks"):
            items = schema["properties"][key]["items"]
            ec = items["properties"]["entity_count"]
            assert ec.get("type") == "integer"
            assert ec.get("minimum", 0) >= 0

    def test_epsg_is_non_negative_int(self):
        schema = _load_schema()
        epsg = schema["properties"]["survey_tags"]["items"]["properties"]["epsg"]
        assert epsg.get("type") == "integer"
        assert epsg.get("minimum", 0) >= 0


class TestGoldenMatches:
    """The committed golden example is itself schema-valid and JSON-stable."""

    def test_golden_exists(self):
        assert GOLDEN_PATH.exists(), f"golden example missing at {GOLDEN_PATH}"

    def test_golden_validates_against_schema(self):
        schema = _load_schema()
        golden = _load_golden()
        # Raises jsonschema.ValidationError on failure — surface the path.
        jsonschema.validate(instance=golden, schema=schema)

    def test_golden_schema_version_is_semver(self):
        golden = _load_golden()
        assert _SEMVER_RE.match(golden["schemaVersion"])
        assert golden["schemaVersion"] == "1.0.0"

    def test_golden_shape_matches_spec(self):
        """Spec: 2 layers, 3 entities (LINE+POLYLINE+INSERT), 1 block, 2 tags, 0 errors."""
        golden = _load_golden()
        assert len(golden["layers"]) == 2
        assert len(golden["entities"]) == 3
        assert len(golden["blocks"]) == 1
        assert len(golden["survey_tags"]) == 2
        assert len(golden["errors"]) == 0

        entity_types = {e["type"] for e in golden["entities"]}
        assert "LINE" in entity_types
        # Accept either POLYLINE or LWPOLYLINE — ezdxf emits the latter for
        # the modern 2D-only variant, which is what the spec intends.
        assert "LWPOLYLINE" in entity_types or "POLYLINE" in entity_types
        assert "INSERT" in entity_types

    def test_golden_uses_totali_surv_layer_names(self):
        golden = _load_golden()
        names = [layer["name"] for layer in golden["layers"]]
        assert all(
            name.startswith("TOTaLi-SURV-") for name in names
        ), f"expected TOTaLi-SURV-* layer names, got {names}"

    def test_golden_entity_layers_exist_in_layers_block(self):
        golden = _load_golden()
        declared = {layer["name"] for layer in golden["layers"]}
        used = {e["layer"] for e in golden["entities"]}
        assert used.issubset(declared), (
            f"entities reference undeclared layer(s): {used - declared}"
        )

    def test_golden_round_trips_sorted_json(self):
        """json.loads(json.dumps(golden, sort_keys=True)) must equal golden.

        This catches sneaky cases where a field contains a non-JSON-native
        type (e.g. tuples, sets, NaN) that would survive naive serialization
        but silently mutate on reload.
        """
        golden = _load_golden()
        reloaded = json.loads(json.dumps(golden, sort_keys=True))
        assert reloaded == golden


class TestSchemaInvariants:
    """The schema actually rejects bad inputs — not just decorative."""

    def test_missing_required_field_fails(self):
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        del broken["errors"]  # required field
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_unknown_top_level_key_fails(self):
        """additionalProperties: false must bite."""
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        broken["unknown_top_level_thing"] = "rejected"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_bad_format_enum_fails(self):
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        broken["format"] = "pdf"  # not in enum
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_bad_severity_enum_fails(self):
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        broken["errors"] = [
            {"severity": "critical", "message": "boom", "location": None}
        ]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_negative_entity_count_fails(self):
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        broken["layers"][0]["entity_count"] = -1
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_negative_epsg_fails(self):
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        broken["survey_tags"][0]["epsg"] = -2903
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_bad_semver_shape_fails(self):
        schema = _load_schema()
        golden = _load_golden()
        broken = deepcopy(golden)
        broken["schemaVersion"] = "v1.0"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=broken, schema=schema)

    def test_null_version_is_allowed(self):
        """`version` is nullable — source file version may be undetectable."""
        schema = _load_schema()
        golden = _load_golden()
        ok = deepcopy(golden)
        ok["version"] = None
        jsonschema.validate(instance=ok, schema=schema)  # must not raise
