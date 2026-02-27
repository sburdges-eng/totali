"""Tests for totali.geodetic.gatekeeper"""
import json
import numpy as np
import laspy
import pytest
from pathlib import Path
from pyproj import CRS

from totali.geodetic.gatekeeper import GeodeticGatekeeper


def test_gatekeeper_passes_valid_las(config, audit, output_dir, synthetic_las_path):
    gk = GeodeticGatekeeper(config["geodetic"], audit)
    context = {"input_path": synthetic_las_path, "output_dir": output_dir}
    result = gk.run(context)
    assert result.success is True
    assert result.phase == "geodetic"
    assert "points_xyz" in result.data
    assert result.data["crs"].epsg_code == 2232


def test_gatekeeper_outputs_las(config, audit, output_dir, synthetic_las_path):
    gk = GeodeticGatekeeper(config["geodetic"], audit)
    context = {"input_path": synthetic_las_path, "output_dir": output_dir}
    result = gk.run(context)
    out_files = [str(f) for f in result.output_files]
    assert any("_gated.las" in f for f in out_files)
    assert any("_geodetic_report.json" in f for f in out_files)


def test_gatekeeper_report_content(config, audit, output_dir, synthetic_las_path):
    gk = GeodeticGatekeeper(config["geodetic"], audit)
    context = {"input_path": synthetic_las_path, "output_dir": output_dir}
    result = gk.run(context)
    report_path = [f for f in result.output_files if str(f).endswith("_geodetic_report.json")][0]
    with open(report_path) as f:
        report = json.load(f)
    assert report["crs"]["epsg"] == 2232
    assert report["validation_passed"] is True
    assert report["point_count"] == 3030


def test_gatekeeper_hash_deterministic(config, audit, output_dir, synthetic_las_path):
    gk = GeodeticGatekeeper(config["geodetic"], audit)
    context = {"input_path": synthetic_las_path, "output_dir": output_dir}
    result = gk.run(context)
    hash1 = result.data["input_hash"]
    assert len(hash1) == 64  # SHA-256 hex


def test_gatekeeper_rejects_missing_crs(config, audit, output_dir, tmp_path):
    # Create LAS without CRS VLR
    header = laspy.LasHeader(point_format=6, version="1.4")
    header.offsets = [0, 0, 0]
    header.scales = [0.001, 0.001, 0.001]
    las = laspy.LasData(header)
    las.x = np.array([1.0, 2.0, 3.0])
    las.y = np.array([1.0, 2.0, 3.0])
    las.z = np.array([1.0, 2.0, 3.0])
    no_crs_path = tmp_path / "no_crs.las"
    las.write(str(no_crs_path))

    cfg = dict(config["geodetic"])
    cfg["reject_on_missing_crs"] = True
    gk = GeodeticGatekeeper(cfg, audit)
    context = {"input_path": str(no_crs_path), "output_dir": output_dir}
    result = gk.run(context)
    assert result.success is False
    assert "CRS" in result.message


def test_gatekeeper_no_transform_when_matching(config, audit, output_dir, synthetic_las_path):
    gk = GeodeticGatekeeper(config["geodetic"], audit)
    context = {"input_path": synthetic_las_path, "output_dir": output_dir}
    result = gk.run(context)
    # CRS matches allowed, so no transform
    report_path = [f for f in result.output_files if str(f).endswith("_geodetic_report.json")][0]
    with open(report_path) as f:
        report = json.load(f)
    assert report["transform_applied"] is False


def test_gatekeeper_point_count(config, audit, output_dir, synthetic_las_path):
    gk = GeodeticGatekeeper(config["geodetic"], audit)
    context = {"input_path": synthetic_las_path, "output_dir": output_dir}
    result = gk.run(context)
    assert len(result.data["points_xyz"]) == 3030
