"""Tests for totali.segmentation.classifier"""
import numpy as np
import pytest
from unittest.mock import MagicMock

from totali.segmentation.classifier import PointCloudClassifier
from totali.pipeline.models import ClassificationResult


def test_classifier_rule_based_fallback(config, audit):
    clf = PointCloudClassifier(config["segmentation"], audit)
    rng = np.random.default_rng(42)
    xyz = np.column_stack([
        rng.uniform(0, 100, 500),
        rng.uniform(0, 100, 500),
        rng.uniform(6100, 6140, 500),
    ])
    las = MagicMock()
    las.classification = np.zeros(500, dtype=np.uint8)
    del las.intensity
    del las.return_number
    del las.number_of_returns

    context = {"points_xyz": xyz, "las": las}
    result = clf.run(context)

    assert result.success is True
    assert "classification" in result.data
    cr = result.data["classification"]
    assert isinstance(cr, ClassificationResult)
    assert len(cr.labels) == 500
    assert len(cr.confidences) == 500
    assert cr.mean_confidence > 0


def test_classifier_uses_existing_classification(config, audit):
    clf = PointCloudClassifier(config["segmentation"], audit)
    xyz = np.column_stack([
        np.arange(100, dtype=float),
        np.arange(100, dtype=float),
        np.full(100, 6100.0),
    ])
    las = MagicMock()
    las.classification = np.full(100, 2, dtype=np.uint8)  # all ground
    del las.intensity
    del las.return_number
    del las.number_of_returns

    context = {"points_xyz": xyz, "las": las}
    result = clf.run(context)

    cr = result.data["classification"]
    # Existing classification should be trusted (confidence 0.85)
    assert np.all(cr.labels == 2)
    assert cr.mean_confidence >= 0.80


def test_classifier_detects_occlusions(config, audit):
    clf = PointCloudClassifier(config["segmentation"], audit)
    rng = np.random.default_rng(42)
    xyz = np.column_stack([
        rng.uniform(0, 100, 200),
        rng.uniform(0, 100, 200),
        rng.uniform(6100, 6150, 200),
    ])
    las = MagicMock()
    las.classification = np.zeros(200, dtype=np.uint8)
    del las.intensity
    del las.return_number
    del las.number_of_returns

    context = {"points_xyz": xyz, "las": las}
    result = clf.run(context)
    cr = result.data["classification"]
    assert cr.occlusion_mask is not None
    assert cr.occlusion_mask.dtype == bool


def test_classifier_no_points_fails(config, audit):
    clf = PointCloudClassifier(config["segmentation"], audit)
    context = {"points_xyz": None, "las": None}
    result = clf.run(context)
    assert result.success is False


def test_classifier_class_counts(config, audit):
    clf = PointCloudClassifier(config["segmentation"], audit)
    xyz = np.column_stack([
        np.arange(50, dtype=float),
        np.zeros(50),
        np.full(50, 6100.0),
    ])
    las = MagicMock()
    las.classification = np.array([2]*30 + [6]*20, dtype=np.uint8)
    del las.intensity
    del las.return_number
    del las.number_of_returns

    context = {"points_xyz": xyz, "las": las}
    result = clf.run(context)
    cr = result.data["classification"]
    assert len(cr.class_counts) > 0
