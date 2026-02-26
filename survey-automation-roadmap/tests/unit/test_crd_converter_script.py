import csv
import struct
import subprocess
import sys
from pathlib import Path


def _pack_record(point_id: str, northing: float, easting: float, elevation: float, description: str) -> bytes:
    record = bytearray(66)
    pid = point_id.encode("ascii")[:10]
    record[0 : len(pid)] = pid
    record[10:34] = struct.pack("<ddd", northing, easting, elevation)
    desc = description.encode("utf-8")[:32]
    record[34 : 34 + len(desc)] = desc
    return bytes(record)


def test_converter_parses_binary_crd_records(repo_root: Path, tmp_path: Path) -> None:
    input_path = tmp_path / "sample_binary.crd"
    output_path = tmp_path / "converted.csv"

    payload = bytearray(72)
    payload.extend(b"New CRD Format2")
    payload.extend(b"\x00" * 7)
    payload.extend(_pack_record("1001", 2822962.32096, 11059.2, 0.124, "SUMMIT FS"))
    payload.extend(_pack_record("1002", 2822963.1, 11060.3, 0.2, "CP"))
    payload.extend(b"\x00" * 10)
    input_path.write_bytes(bytes(payload))

    run = subprocess.run(
        [sys.executable, str(repo_root / "scripts/converter"), "--input", str(input_path), "--output", str(output_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stdout + run.stderr

    with output_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0][:4] == ["Point#", "Northing", "Easting", "Elevation"]
    assert rows[1][0] == "1001"
    assert rows[1][1] == "2822962.32096"
    assert rows[1][2] == "11059.2"
    assert rows[1][3] == "0.124"
    assert rows[1][4] == "SUMMIT FS"
    assert rows[2][0] == "1002"


def test_converter_supports_positional_args(repo_root: Path, tmp_path: Path) -> None:
    input_path = tmp_path / "sample_text.crd"
    output_path = tmp_path / "converted.csv"
    input_path.write_text("P1 100 200 300 DESC\n", encoding="utf-8")

    run = subprocess.run(
        [sys.executable, str(repo_root / "scripts/converter"), str(input_path), str(output_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    assert output_path.exists()


def test_converter_fails_for_invalid_content(repo_root: Path, tmp_path: Path) -> None:
    input_path = tmp_path / "invalid.crd"
    output_path = tmp_path / "converted.csv"
    input_path.write_text("not a parseable crd\n", encoding="utf-8")

    run = subprocess.run(
        [sys.executable, str(repo_root / "scripts/converter"), "--input", str(input_path), "--output", str(output_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode != 0
    assert "no point records parsed" in run.stderr.lower()
