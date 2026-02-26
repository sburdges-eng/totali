from copy import deepcopy

from survey_automation.config import DEFAULT_CONFIG
from survey_automation.models import FieldCodeRule, PointRecord
from survey_automation.qc import run_qc


def _point(point_id: str, source_line: int) -> PointRecord:
    return PointRecord(
        point_id=point_id,
        northing=1.0,
        easting=2.0,
        elevation=3.0,
        description="CP TEST",
        dwg_description="CP TEST",
        dwg_layer="PNTS",
        locked="No",
        group_name="",
        category="Control",
        ls_number="",
        source_file="points.csv",
        source_line=source_line,
    )


def test_duplicate_point_id_reports_all_occurrences() -> None:
    config = deepcopy(DEFAULT_CONFIG)
    findings = run_qc(
        points=[_point("1001", 2), _point("1001", 5), _point("2001", 8)],
        field_code_rules=[
            FieldCodeRule(
                field_code="CP",
                layer="PNTS",
                symbol="DOT1",
                linework="YES",
                source_file="field_codes.csv",
                source_line=2,
            )
        ],
        config=config,
        run_id="run-test",
    )
    duplicate_findings = [finding for finding in findings if finding.code == "duplicate_point_id"]
    assert len(duplicate_findings) == 2
    assert {finding.row_number for finding in duplicate_findings} == {2, 5}


def test_duplicate_point_id_can_report_once_per_id() -> None:
    config = deepcopy(DEFAULT_CONFIG)
    config["validation"]["duplicate_point_id_mode"] = "per_point_id"
    findings = run_qc(
        points=[_point("1001", 2), _point("1001", 5), _point("2001", 8)],
        field_code_rules=[
            FieldCodeRule(
                field_code="CP",
                layer="PNTS",
                symbol="DOT1",
                linework="YES",
                source_file="field_codes.csv",
                source_line=2,
            )
        ],
        config=config,
        run_id="run-test",
    )
    duplicate_findings = [finding for finding in findings if finding.code == "duplicate_point_id"]
    assert len(duplicate_findings) == 1
    assert duplicate_findings[0].row_number == 2


def test_unmapped_description_skip_categories() -> None:
    config = deepcopy(DEFAULT_CONFIG)
    config["validation"]["unmapped_description_skip_categories"] = ["converted"]
    findings = run_qc(
        points=[
            PointRecord(
                point_id="1001",
                northing=1.0,
                easting=2.0,
                elevation=3.0,
                description="UNKNOWNCODE detail",
                dwg_description="",
                dwg_layer="",
                locked="",
                group_name="",
                category="Converted",
                ls_number="",
                source_file="converted.csv",
                source_line=2,
            ),
            PointRecord(
                point_id="1002",
                northing=1.0,
                easting=2.0,
                elevation=3.0,
                description="UNKNOWNCODE detail",
                dwg_description="",
                dwg_layer="",
                locked="",
                group_name="",
                category="Control",
                ls_number="",
                source_file="points.csv",
                source_line=3,
            ),
        ],
        field_code_rules=[
            FieldCodeRule(
                field_code="CP",
                layer="PNTS",
                symbol="DOT1",
                linework="YES",
                source_file="field_codes.csv",
                source_line=2,
            )
        ],
        config=config,
        run_id="run-test",
    )
    unmapped_findings = [finding for finding in findings if finding.code == "description_code_unmapped"]
    assert len(unmapped_findings) == 1
    assert unmapped_findings[0].file_path == "points.csv"


def test_duplicate_point_id_within_file_mode_ignores_cross_file_collisions() -> None:
    config = deepcopy(DEFAULT_CONFIG)
    config["validation"]["duplicate_point_id_mode"] = "within_file"
    findings = run_qc(
        points=[
            PointRecord(
                point_id="1001",
                northing=1.0,
                easting=2.0,
                elevation=3.0,
                description="CP TEST",
                dwg_description="",
                dwg_layer="",
                locked="",
                group_name="",
                category="Control",
                ls_number="",
                source_file="a.csv",
                source_line=2,
            ),
            PointRecord(
                point_id="1001",
                northing=1.0,
                easting=2.0,
                elevation=3.0,
                description="CP TEST",
                dwg_description="",
                dwg_layer="",
                locked="",
                group_name="",
                category="Control",
                ls_number="",
                source_file="b.csv",
                source_line=2,
            ),
            PointRecord(
                point_id="2001",
                northing=1.0,
                easting=2.0,
                elevation=3.0,
                description="CP TEST",
                dwg_description="",
                dwg_layer="",
                locked="",
                group_name="",
                category="Control",
                ls_number="",
                source_file="b.csv",
                source_line=3,
            ),
            PointRecord(
                point_id="2001",
                northing=1.0,
                easting=2.0,
                elevation=3.0,
                description="CP TEST",
                dwg_description="",
                dwg_layer="",
                locked="",
                group_name="",
                category="Control",
                ls_number="",
                source_file="b.csv",
                source_line=4,
            ),
        ],
        field_code_rules=[
            FieldCodeRule(
                field_code="CP",
                layer="PNTS",
                symbol="DOT1",
                linework="YES",
                source_file="field_codes.csv",
                source_line=2,
            )
        ],
        config=config,
        run_id="run-test",
    )
    duplicate_findings = [finding for finding in findings if finding.code == "duplicate_point_id"]
    assert len(duplicate_findings) == 1
    assert duplicate_findings[0].file_path == "b.csv"
    assert duplicate_findings[0].row_number == 3
