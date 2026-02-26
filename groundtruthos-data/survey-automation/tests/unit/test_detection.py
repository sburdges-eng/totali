from pathlib import Path

from survey_automation.detection import classify_csv, detect_file_type, discover_files


def test_classify_csv_variants(tmp_path: Path) -> None:
    point_file = tmp_path / "points.csv"
    point_file.write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1,2,3,CP,CP,PNTS,No,,Other,\n",
        encoding="utf-8",
    )

    field_file = tmp_path / "field.csv"
    field_file.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n",
        encoding="utf-8",
    )

    mixed_file = tmp_path / "mixed.csv"
    mixed_file.write_text(
        "Field Code,Layer,Symbol,Linework\n"
        "CP,PNTS,DOT1,YES\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1,2,3,CP,CP,PNTS,No,,Other,\n",
        encoding="utf-8",
    )

    assert classify_csv(point_file)[0] == "point_csv"
    assert classify_csv(field_file)[0] == "field_code_csv"
    assert classify_csv(mixed_file)[0] == "mixed_csv"


def test_detect_binary_crd_signature(tmp_path: Path) -> None:
    crd_file = tmp_path / "sample.crd"
    crd_file.write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    detected, message = detect_file_type(crd_file)
    assert detected == "crd_binary"
    assert "crd" in message


def test_detect_text_points_file_as_crd_text(tmp_path: Path) -> None:
    points_file = tmp_path / "points.pts"
    points_file.write_text(
        "1001 12345.6 54321.1 150.2 CP START\n"
        "1002 12346.0 54322.0 149.9 EP END\n",
        encoding="utf-8",
    )

    detected, message = detect_file_type(points_file)
    assert detected == "crd_text"
    assert message == "text_points_detected"


def test_detect_unknown_text_points_file_as_unsupported(tmp_path: Path) -> None:
    notes_file = tmp_path / "notes.txt"
    notes_file.write_text(
        "This is a general notes file.\n"
        "No survey coordinates here.\n",
        encoding="utf-8",
    )

    detected, message = detect_file_type(notes_file)
    assert detected == "unsupported"
    assert message == "text_points_unknown_schema"


def test_discover_files_deduplicates_symlink_and_realpath(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    target_file = source_root / "points.csv"
    target_file.write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1,2,3,CP,CP,PNTS,No,,Other,\n",
        encoding="utf-8",
    )

    totali_link = tmp_path / "TOTaLi"
    local_root = tmp_path / ".local-datasets" / "TOTaLi"
    local_root.parent.mkdir(parents=True, exist_ok=True)
    totali_link.symlink_to(source_root, target_is_directory=True)
    local_root.symlink_to(source_root, target_is_directory=True)

    files = discover_files(
        input_dir=tmp_path,
        include_globs=["TOTaLi/**/*", ".local-datasets/TOTaLi/**/*"],
        exclude_globs=[],
    )
    assert len(files) == 1
    assert files[0].suffix == ".csv"
