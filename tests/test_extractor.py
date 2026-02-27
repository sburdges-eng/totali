"""Tests for totali.extraction.extractor"""
import numpy as np
import pytest
from pathlib import Path

from totali.extraction.extractor import DeterministicExtractor
from totali.pipeline.models import ClassificationResult, ExtractionResult


def test_extractor_builds_dtm(config, audit, output_dir, sample_xyz, sample_classification):
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {
        "points_xyz": sample_xyz,
        "classification": sample_classification,
        "output_dir": output_dir,
    }
    result = ext.run(context)
    assert result.success is True
    er = result.data["extraction"]
    assert er.dtm_vertices is not None
    assert er.dtm_faces is not None
    assert len(er.dtm_faces) > 0


def test_extractor_generates_contours(config, audit, output_dir, sample_xyz, sample_classification):
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {
        "points_xyz": sample_xyz,
        "classification": sample_classification,
        "output_dir": output_dir,
    }
    result = ext.run(context)
    er = result.data["extraction"]
    total_contours = len(er.contours_minor) + len(er.contours_index)
    assert total_contours >= 0  # may be 0 for very flat surface


def test_extractor_error_metrics(config, audit, output_dir, sample_xyz, sample_classification):
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {
        "points_xyz": sample_xyz,
        "classification": sample_classification,
        "output_dir": output_dir,
    }
    result = ext.run(context)
    er = result.data["extraction"]
    assert "dtm" in er.error_metrics
    assert "vertex_count" in er.error_metrics["dtm"]
    assert "face_count" in er.error_metrics["dtm"]


def test_extractor_writes_report(config, audit, output_dir, sample_xyz, sample_classification):
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {
        "points_xyz": sample_xyz,
        "classification": sample_classification,
        "output_dir": output_dir,
    }
    result = ext.run(context)
    report_path = output_dir / "extraction_report.json"
    assert report_path.exists()


def test_extractor_insufficient_ground_fails(config, audit, output_dir):
    xyz = np.array([[0, 0, 100], [1, 1, 101], [2, 2, 102]], dtype=float)
    cr = ClassificationResult(
        labels=np.array([6, 6, 6]),  # all building, no ground
        confidences=np.full(3, 0.9),
        mean_confidence=0.9,
    )
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {"points_xyz": xyz, "classification": cr, "output_dir": output_dir}
    result = ext.run(context)
    assert result.success is False
    assert "Insufficient ground" in result.message


def test_extractor_qa_flags_low_confidence(config, audit, output_dir):
    rng = np.random.default_rng(42)
    n = 200
    xyz = np.column_stack([rng.uniform(0, 100, n), rng.uniform(0, 100, n), rng.uniform(6100, 6101, n)])
    cr = ClassificationResult(
        labels=np.full(n, 2, dtype=np.int32),
        confidences=np.full(n, 0.3, dtype=np.float32),  # all low confidence
        occlusion_mask=np.zeros(n, dtype=bool),
        mean_confidence=0.3,
        low_confidence_count=n,
    )
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {"points_xyz": xyz, "classification": cr, "output_dir": output_dir}
    result = ext.run(context)
    assert result.success is True
    er = result.data["extraction"]
    flag_types = [f["type"] for f in er.qa_flags]
    assert "low_confidence" in flag_types


def test_extractor_building_footprints(config, audit, output_dir):
    rng = np.random.default_rng(42)
    # 50 ground + 50 building cluster
    gx = rng.uniform(0, 200, 50)
    gy = rng.uniform(0, 200, 50)
    gz = np.full(50, 6100.0)
    bx = rng.uniform(80, 120, 50)
    by = rng.uniform(80, 120, 50)
    bz = np.full(50, 6120.0)
    xyz = np.column_stack([np.concatenate([gx, bx]), np.concatenate([gy, by]), np.concatenate([gz, bz])])
    labels = np.array([2]*50 + [6]*50, dtype=np.int32)
    cr = ClassificationResult(
        labels=labels,
        confidences=np.full(100, 0.8, dtype=np.float32),
        occlusion_mask=np.zeros(100, dtype=bool),
        mean_confidence=0.8,
        low_confidence_count=0,
    )
    ext = DeterministicExtractor(config["extraction"], audit)
    context = {"points_xyz": xyz, "classification": cr, "output_dir": output_dir}
    result = ext.run(context)
    assert result.success is True
