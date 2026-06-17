"""Tests for the labeled-point dataset builder (U1 flywheel seed).

Synthetic fixtures only — no confidential client exports — so CI stays mocked.
"""

from __future__ import annotations

import pytest

from totali.fieldcodes import load_field_codes
from totali.segmentation.dataset import (
    LabeledPoint,
    build_labeled_dataset,
    parse_asc,
)

SAMPLE_FLD = """#2010V# Code|Description|Symbol|Symbol Size|Layer
FIELD CODE|Layer|Symbol|0.0000|none
TOPO|TOPO|DOT1|0.0000|none
CP|CONTROL_POINT|CTRLPT|0.0000|none
FND|TOPO|DOT1|0.0000|none
WL|WATERLINE|WATER|0.0000|none
"""

# PtNo,N,E,Z,Description — note: code is the first token; a row with an unknown
# code (ZZZ), a quoted comma-containing description, and a junk/header row.
SAMPLE_ASC = (
    "1,1372340.54,2818546.82,8010.78,TOPO shot grade\n"
    "2,1372341.10,2818547.00,8010.90,CP control\n"
    '3,1372342.00,2818548.00,8011.00,"""FND""","31544"\n'
    "4,1372343.00,2818549.00,8012.00,WL waterline\n"
    "5,1372344.00,2818550.00,8013.00,TOPO\n"
    "6,1372345.00,2818551.00,8014.00,ZZZ mystery code\n"
    "PtNo,Northing,Easting,Elev,Desc\n"  # header-ish junk row -> skipped
    "\n"
)


@pytest.fixture
def table(tmp_path):
    p = tmp_path / "codes.fld"
    p.write_text(SAMPLE_FLD, encoding="utf-8")
    return load_field_codes(p)


@pytest.fixture
def asc(tmp_path):
    p = tmp_path / "job.asc"
    p.write_text(SAMPLE_ASC, encoding="utf-8")
    return p


class TestParse:
    def test_parses_coordinates_and_codes(self, asc, table):
        pts = parse_asc(asc, table)
        # 6 data rows parse; header + blank skipped.
        assert len(pts) == 6
        first = pts[0]
        assert isinstance(first, LabeledPoint)
        assert first.field_code == "TOPO"
        assert (first.northing, first.easting, first.elevation) == (
            1372340.54, 2818546.82, 8010.78,
        )

    def test_first_token_is_field_code(self, asc, table):
        pts = {p.point_id: p for p in parse_asc(asc, table)}
        assert pts["1"].field_code == "TOPO"
        assert pts["2"].field_code == "CP"
        assert pts["3"].field_code == "FND"  # from '"""FND"""'
        assert pts["4"].field_code == "WL"

    def test_label_resolved_via_taxonomy(self, asc, table):
        pts = {p.point_id: p for p in parse_asc(asc, table)}
        assert pts["1"].layer == "TOPO"
        assert pts["2"].layer == "CONTROL_POINT"
        assert pts["3"].layer == "TOPO"  # FND -> TOPO
        assert pts["4"].layer == "WATERLINE"

    def test_unknown_code_is_unlabeled(self, asc, table):
        pts = {p.point_id: p for p in parse_asc(asc, table)}
        assert pts["6"].field_code == "ZZZ"
        assert pts["6"].layer is None
        assert pts["6"].is_labeled is False


class TestCodeExtraction:
    def test_leading_stake_number_skipped_for_recognized_code(self, tmp_path, table):
        # FND is in the taxonomy; STK999 (stake number) is not -> code is FND.
        asc = tmp_path / "stake.asc"
        asc.write_text(
            '10,1372350.0,2818560.0,8020.0,STK999 FND 5/8x1" ls1776\n', encoding="utf-8"
        )
        pts = parse_asc(asc, table)
        assert len(pts) == 1
        assert pts[0].field_code == "FND"
        assert pts[0].layer == "TOPO"

    def test_falls_back_to_first_token_when_none_recognized(self, tmp_path, table):
        asc = tmp_path / "unk.asc"
        asc.write_text("11,1.0,2.0,3.0,STK999 ZZZ notes\n", encoding="utf-8")
        pts = parse_asc(asc, table)
        assert pts[0].field_code == "STK999"
        assert pts[0].layer is None


class TestDataset:
    def test_labeled_unknown_partition(self, asc, table):
        ds = build_labeled_dataset([asc], table)
        assert len(ds) == 6
        assert len(ds.labeled()) == 5
        assert ds.unknown_codes() == {"ZZZ"}

    def test_class_counts(self, asc, table):
        ds = build_labeled_dataset([asc], table)
        assert ds.class_counts() == {"TOPO": 3, "CONTROL_POINT": 1, "WATERLINE": 1}
        assert ds.classes() == ("CONTROL_POINT", "TOPO", "WATERLINE")

    def test_combines_multiple_files(self, asc, table):
        ds = build_labeled_dataset([asc, asc], table)
        assert len(ds) == 12


class TestSplit:
    def test_split_is_deterministic(self, asc, table):
        ds = build_labeled_dataset([asc], table)
        tr1, ev1 = ds.split(eval_frac=0.5)
        tr2, ev2 = ds.split(eval_frac=0.5)
        ids = lambda d: sorted(p.point_id for p in d.points)  # noqa: E731
        assert ids(tr1) == ids(tr2)
        assert ids(ev1) == ids(ev2)

    def test_split_partitions_all_labeled(self, asc, table):
        ds = build_labeled_dataset([asc], table)
        tr, ev = ds.split(eval_frac=0.5)
        assert len(tr) + len(ev) == len(ds.labeled())
        assert {p.point_id for p in tr.points} & {p.point_id for p in ev.points} == set()

    def test_invalid_eval_frac_raises(self, asc, table):
        ds = build_labeled_dataset([asc], table)
        with pytest.raises(ValueError):
            ds.split(eval_frac=1.0)
