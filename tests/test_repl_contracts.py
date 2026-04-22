"""R-1 partial: totali.repl.contracts schema and validators.

Guards the contract-surface helpers in `totali/repl/contracts.py`: schema
version, top-level key set, absolute-path detection, path-traversal
detection, metadata scalar-only enforcement.
"""

import pytest

from totali.repl import contracts as c


class TestSchemaConstants:
    def test_schema_version(self):
        assert c.SCHEMA_VERSION == "1.0.0"

    def test_top_level_keys_expected(self):
        assert set(c.CONTRACT_TOP_LEVEL_KEYS) >= {
            "schemaVersion",
            "artifactType",
            "invariants",
            "metadata",
            "paths",
            "data",
        }

    def test_base_invariants_nonempty(self):
        assert len(c.BASE_INVARIANTS) >= 3


class TestAbsolutePath:
    @pytest.mark.parametrize(
        "path",
        ["/usr/local/bin", "/tmp/x.las", "C:\\path\\to\\file", "\\\\server\\share"],
    )
    def test_detects_absolute(self, path):
        assert c._is_absolute_path(path) is True

    @pytest.mark.parametrize(
        "path",
        ["relative/x", "x", "./x", "../up", "artifacts/run-1/x.dxf"],
    )
    def test_detects_relative(self, path):
        assert c._is_absolute_path(path) is False


class TestPathTraversal:
    @pytest.mark.parametrize(
        "path",
        ["../etc/passwd", "a/../b", "foo/bar/../../baz", "..\\win\\secret"],
    )
    def test_detects_traversal(self, path):
        assert c._has_path_traversal(path) is True

    @pytest.mark.parametrize(
        "path",
        ["a/b", "artifacts/run/x.json", "/abs/no/traversal"],
    )
    def test_detects_safe(self, path):
        assert c._has_path_traversal(path) is False


class TestMetadataValidation:
    def test_scalars_accepted(self):
        m = {"str": "x", "int": 1, "float": 1.5, "bool": True, "none": None}
        assert c._validate_metadata(m) == m

    def test_dict_value_rejected(self):
        with pytest.raises(ValueError):
            c._validate_metadata({"nested": {"x": 1}})

    def test_list_value_rejected(self):
        with pytest.raises(ValueError):
            c._validate_metadata({"arr": [1, 2, 3]})

    def test_empty_key_rejected(self):
        with pytest.raises(ValueError):
            c._validate_metadata({"": "value"})

    def test_non_string_key_rejected(self):
        with pytest.raises(ValueError):
            c._validate_metadata({1: "value"})
