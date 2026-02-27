"""Tests for totali.cad_shielding.shield"""
import json
import numpy as np
import pytest
from pathlib import Path

from totali.cad_shielding.shield import CADShield
from totali.pipeline.models import ExtractionResult, GeometryStatus


def test_shield_writes_dxf(config, audit, output_dir, sample_extraction):
    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": sample_extraction, "output_dir": output_dir}
    result = shield.run(context)
    assert result.success is True
    dxf_path = output_dir / "totali_draft_output.dxf"
    assert dxf_path.exists()
    assert dxf_path.stat().st_size > 0


def test_shield_writes_manifest(config, audit, output_dir, sample_extraction):
    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": sample_extraction, "output_dir": output_dir}
    result = shield.run(context)
    manifest_path = output_dir / "entity_manifest.json"
    assert manifest_path.exists()
    with open(manifest_path) as f:
        manifest = json.load(f)
    assert "entities" in manifest
    assert manifest["entity_count"] > 0


def test_shield_all_entities_draft(config, audit, output_dir, sample_extraction):
    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": sample_extraction, "output_dir": output_dir}
    result = shield.run(context)
    manifest = result.data["manifest"]
    for entity in manifest["entities"]:
        assert entity["status"] == GeometryStatus.DRAFT.value


def test_shield_entity_has_hash(config, audit, output_dir, sample_extraction):
    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": sample_extraction, "output_dir": output_dir}
    result = shield.run(context)
    manifest = result.data["manifest"]
    for entity in manifest["entities"]:
        assert len(entity["source_hash"]) == 16


def test_shield_healing_report(config, audit, output_dir, sample_extraction):
    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": sample_extraction, "output_dir": output_dir}
    result = shield.run(context)
    healing = result.data["healing"]
    assert healing.input_entity_count > 0
    assert healing.quarantined_count >= 0


def test_shield_no_extraction_fails(config, audit, output_dir):
    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": None, "output_dir": output_dir}
    result = shield.run(context)
    assert result.success is False


def test_shield_degenerate_face_quarantined(config, audit, output_dir):
    """A DTM with a zero-area face should be quarantined."""
    er = ExtractionResult()
    er.dtm_vertices = np.array([
        [0, 0, 0], [1, 0, 0], [0, 1, 0],  # valid triangle
        [5, 5, 5], [5, 5, 5], [5, 5, 5],  # degenerate (zero area)
    ], dtype=float)
    er.dtm_faces = np.array([[0, 1, 2], [3, 4, 5]])

    shield = CADShield(config["cad_shielding"], audit)
    context = {"extraction": er, "output_dir": output_dir}
    result = shield.run(context)
    assert result.success is True
    healing = result.data["healing"]
    assert healing.quarantined_count >= 1
