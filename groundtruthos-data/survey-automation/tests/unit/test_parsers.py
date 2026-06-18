from copy import deepcopy
from pathlib import Path

from survey_automation.config import DEFAULT_CONFIG
from survey_automation.parsers import parse_ascii_dxf, parse_csv_file


def test_parse_mixed_csv_splits_sections(tmp_path: Path) -> None:
    config = deepcopy(DEFAULT_CONFIG)
    mixed = tmp_path / "mixed.csv"
    mixed.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n"
        "BAD,ROW\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,100,200,300,CP START,CP START,PNTS,No,,Other,\n",
        encoding="utf-8",
    )

    result = parse_csv_file(mixed, "mixed_csv", config, "run-test")
    assert len(result.points) == 1
    assert len(result.field_code_rules) == 1
    assert any(item.reason == "bad_column_count" for item in result.quarantined_rows)


def test_parse_malformed_field_row_goes_to_quarantine(tmp_path: Path) -> None:
    config = deepcopy(DEFAULT_CONFIG)
    malformed = tmp_path / "bad.csv"
    malformed.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n"
        "PNTS,CULVERT_FEATURE,YES\n",
        encoding="utf-8",
    )

    result = parse_csv_file(malformed, "field_code_csv", config, "run-test")
    assert len(result.field_code_rules) == 1
    assert len(result.quarantined_rows) == 1
    assert result.quarantined_rows[0].reason == "bad_column_count"


def test_parse_ascii_dxf_extracts_entities(repo_root: Path) -> None:
    result = parse_ascii_dxf(repo_root / "samples/input/sample_ascii.dxf", "run-test")
    assert len(result.dxf_entities) == 2
    assert all(entity.source_file.endswith("sample_ascii.dxf") for entity in result.dxf_entities)


def test_parse_csv_uses_configured_column_counts(tmp_path: Path) -> None:
    config = deepcopy(DEFAULT_CONFIG)
    config["validation"]["required_point_columns"] = [
        "Point#",
        "Northing",
        "Easting",
        "Elevation",
        "Description",
        "DWG Description",
        "DWG Layer",
        "Locked",
        "Group",
        "Category",
        "LS Number",
        "Extra Point Column",
    ]
    config["validation"]["required_field_code_columns"] = [
        "Field Code",
        "Layer",
        "Symbol",
        "Linework",
        "Extra Field Column",
    ]
    mixed = tmp_path / "mixed.csv"
    mixed.write_text(
        "Field Code,Layer,Symbol,Linework,Extra Field Column\n"
        "CP,PNTS,DOT1,YES,EXTRA\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number,Extra Point Column\n"
        "1,100,200,300,CP START,CP START,PNTS,No,,Other,,EXTRA\n",
        encoding="utf-8",
    )

    result = parse_csv_file(mixed, "mixed_csv", config, "run-test")
    assert len(result.points) == 1
    assert len(result.field_code_rules) == 1
    assert len(result.quarantined_rows) == 0


def test_parse_csv_auto_remediates_blank_field_code(tmp_path: Path) -> None:
    config = deepcopy(DEFAULT_CONFIG)
    config["remediation"]["enabled"] = True
    config["remediation"]["fix_blank_field_codes"] = True
    path = tmp_path / "field_codes.csv"
    path.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        ",PNTS,DOT1,YES\n",
        encoding="utf-8",
    )

    result = parse_csv_file(path, "field_code_csv", config, "run-test")
    assert len(result.field_code_rules) == 1
    assert result.field_code_rules[0].field_code.startswith("AUTO_PNTS_")
    assert len(result.quarantined_rows) == 0
    assert any(finding.code == "auto_remediated_blank_field_code" for finding in result.findings)


def test_parse_csv_auto_remediates_duplicate_tail_and_footer(tmp_path: Path) -> None:
    config = deepcopy(DEFAULT_CONFIG)
    config["remediation"]["enabled"] = True
    config["remediation"]["drop_duplicate_tail_blocks"] = True
    config["remediation"]["drop_malformed_footer_rows"] = True
    mixed = tmp_path / "mixed.csv"
    mixed.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,100,200,300,CP START,CP START,PNTS,No,,Other,\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,100,200,300,CP START,CP START,PNTS,No,,Other,\n"
        "END OF EXPORT\n",
        encoding="utf-8",
    )

    result = parse_csv_file(mixed, "mixed_csv", config, "run-test")
    assert len(result.points) == 1
    assert len(result.field_code_rules) == 1
    assert len(result.quarantined_rows) == 0
    assert any(finding.code == "auto_remediated_duplicate_tail_block" for finding in result.findings)
    assert any(finding.code == "auto_remediated_malformed_footer_row" for finding in result.findings)
