"""Tests for the field-code (.fld) taxonomy loader (U1 foundation).

Uses synthetic fixtures only — never the confidential partner library — so CI
stays fully mocked and no client data is required or committed.
"""

from __future__ import annotations

import pytest

from totali.fieldcodes import (
    DESCRIPTIONS_PATH_ENV,
    FLD_PATH_ENV,
    FieldCode,
    FieldCodeTable,
    load_default,
    load_field_codes,
)

# Mirrors the real Carlson 2010V shape: a #-header, a FIELD CODE template row,
# data rows where col1=layer/col2=symbol, and a duplicate code to dedupe.
SAMPLE_FLD = """#2010V# Code|Description|Symbol|Symbol Size|Layer
FIELD CODE|Layer|Symbol|0.0000|none
TOPO|TOPO|DOT1|0.0000|none
CP|CONTROL_POINT|CTRLPT|0.0000|none
CREEK|CREEK|WATER|0.0000|none
FRAC|SURVEY_MARKER|MONUMENT|0.0000|none
FRAC|SURVEY_MARKER|MONUMENT|0.0000|none
"""

SAMPLE_CSV = (
    "Field Code,Description\n"
    "TOPO,\"Field code 'TOPO' is placed on layer 'TOPO', uses symbol 'DOT1', "
    "and linework is set to 'YES'.\"\n"
    "CP,\"Field code 'CP' is placed on layer 'CONTROL_POINT', uses symbol "
    "'CTRLPT', and linework is set to 'NO'.\"\n"
    "CREEK,\"Field code 'CREEK' is placed on layer 'CREEK', uses symbol 'WATER', "
    "and linework is set to 'YES'.\"\n"
    "FRAC,\"Field code 'FRAC' is placed on layer 'SURVEY_MARKER', uses symbol "
    "'MONUMENT', and linework is set to 'NO'.\"\n"
)


@pytest.fixture
def fld_file(tmp_path):
    p = tmp_path / "codes.fld"
    p.write_text(SAMPLE_FLD, encoding="utf-8")
    return p


@pytest.fixture
def csv_file(tmp_path):
    p = tmp_path / "descriptions.csv"
    p.write_text(SAMPLE_CSV, encoding="utf-8")
    return p


class TestParsing:
    def test_header_and_template_rows_skipped(self, fld_file):
        table = load_field_codes(fld_file)
        # 4 distinct codes (FRAC deduped); header + FIELD CODE row excluded.
        assert set(table.codes()) == {"TOPO", "CP", "CREEK", "FRAC"}
        assert "FIELD CODE" not in table
        assert len(table) == 4

    def test_layer_is_column_one_symbol_is_column_two(self, fld_file):
        table = load_field_codes(fld_file)
        assert table.layer_for("CP") == "CONTROL_POINT"
        assert table.symbol_for("CP") == "CTRLPT"
        assert table.layer_for("CREEK") == "CREEK"
        assert table.symbol_for("CREEK") == "WATER"

    def test_returns_field_code_objects(self, fld_file):
        table = load_field_codes(fld_file)
        fc = table["TOPO"]
        assert isinstance(fc, FieldCode)
        assert fc.code == "TOPO" and fc.layer == "TOPO" and fc.symbol == "DOT1"


class TestClasses:
    def test_class_set_is_distinct_sorted_layers(self, fld_file):
        table = load_field_codes(fld_file)
        assert table.classes() == ("CONTROL_POINT", "CREEK", "SURVEY_MARKER", "TOPO")


class TestLinework:
    def test_linework_from_descriptions_csv(self, fld_file, csv_file):
        table = load_field_codes(fld_file, csv_file)
        assert table.is_linework("TOPO") is True
        assert table.is_linework("CREEK") is True
        assert table.is_linework("CP") is False
        assert table.is_linework("FRAC") is False

    def test_point_vs_linework_partitions(self, fld_file, csv_file):
        table = load_field_codes(fld_file, csv_file)
        assert set(table.linework_codes()) == {"TOPO", "CREEK"}
        assert set(table.point_codes()) == {"CP", "FRAC"}
        assert table["CP"].is_point_feature is True
        assert table["TOPO"].is_point_feature is False

    def test_linework_unknown_without_descriptions(self, fld_file):
        table = load_field_codes(fld_file)
        assert table.is_linework("TOPO") is None
        assert table["CP"].is_point_feature is False  # unknown != point

    def test_missing_descriptions_path_tolerated(self, fld_file, tmp_path):
        table = load_field_codes(fld_file, tmp_path / "nope.csv")
        assert table.is_linework("TOPO") is None


SAMPLE_CSV_FLD = (
    "Field Code,Layer,Symbol,Linework\n"
    "7V1,TOPO,DOT1,YES\n"
    "FND,TOPO,DOT1,YES\n"
    "CP,CONTROL_POINT,CTRLPT,NO\n"
    "WL,WATERLINE,WATER,YES\n"
)


class TestCsvFormat:
    def test_csv_library_parses_with_inline_linework(self, tmp_path):
        p = tmp_path / "universal.fld"
        p.write_text(SAMPLE_CSV_FLD, encoding="utf-8")
        table = load_field_codes(p)  # no descriptions CSV needed
        assert set(table.codes()) == {"7V1", "FND", "CP", "WL"}
        assert table.layer_for("CP") == "CONTROL_POINT"
        assert table.symbol_for("WL") == "WATER"
        assert table.is_linework("WL") is True
        assert table.is_linework("CP") is False
        assert table.classes() == ("CONTROL_POINT", "TOPO", "WATERLINE")

    def test_csv_header_row_skipped(self, tmp_path):
        p = tmp_path / "universal.fld"
        p.write_text(SAMPLE_CSV_FLD, encoding="utf-8")
        table = load_field_codes(p)
        assert "Field Code" not in table


class TestErrorsAndDefaults:
    def test_missing_fld_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_field_codes(tmp_path / "absent.fld")

    def test_load_default_uses_env(self, fld_file, csv_file, monkeypatch):
        monkeypatch.setenv(FLD_PATH_ENV, str(fld_file))
        monkeypatch.setenv(DESCRIPTIONS_PATH_ENV, str(csv_file))
        table = load_default()
        assert isinstance(table, FieldCodeTable)
        assert table.is_linework("CP") is False

    def test_load_default_without_env_raises(self, monkeypatch):
        monkeypatch.delenv(FLD_PATH_ENV, raising=False)
        with pytest.raises(RuntimeError):
            load_default()
