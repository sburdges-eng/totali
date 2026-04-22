"""Cross-cutting invariant guards on config/pipeline.yaml.

Asserts the TOTaLi invariants (§1 of AGENTIC_COMPLETION_PLAN.md) that live in
config: auto_promote False, require_pls_signature True, CRS allowlist, layer
naming, audit append-only + SHA-256, deterministic-output defaults.

These tests fail fast on any config regression.
"""

import re
from pathlib import Path

import pytest
import yaml


LAYER_RE = re.compile(
    r"^TOTaLi-[A-Z0-9]+(?:-[A-Z0-9_]+)+-DRAFT$|^TOTaLi-QA-[A-Z0-9_-]+$"
)
ALLOWED_CRS = {"EPSG:2231", "EPSG:2232", "EPSG:2233", "EPSG:6428", "EPSG:6430", "EPSG:6432"}


@pytest.fixture(scope="module")
def cfg():
    path = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


class TestLintingInvariants:
    def test_auto_promote_false(self, cfg):
        assert cfg["linting"]["auto_promote"] is False

    def test_require_pls_signature_true(self, cfg):
        assert cfg["linting"]["require_pls_signature"] is True


class TestLayerNamingInvariants:
    def test_all_layer_names_conform(self, cfg):
        mapping = cfg["cad_shielding"]["layer_mapping"]
        bad = [v for v in mapping.values() if not LAYER_RE.match(v)]
        assert not bad, f"Non-conforming layer names: {bad}"

    def test_certified_suffix_empty(self, cfg):
        assert cfg["cad_shielding"]["certified_layer_suffix"] == ""


class TestCRSInvariants:
    def test_allowed_crs_subset(self, cfg):
        allowed = set(cfg["geodetic"]["allowed_crs"])
        assert allowed.issubset(ALLOWED_CRS), (
            f"Disallowed CRS in config: {allowed - ALLOWED_CRS}"
        )

    def test_required_epoch_present(self, cfg):
        assert "required_epoch" in cfg["geodetic"]
        assert cfg["geodetic"]["required_epoch"]

    def test_geoid_model_is_geoid18(self, cfg):
        assert cfg["geodetic"]["geoid_model"] == "GEOID18"

    def test_elevation_unit_us_survey_foot(self, cfg):
        assert cfg["geodetic"]["elevation_unit"] == "US_survey_foot"

    def test_reject_on_mixed_datum_true(self, cfg):
        assert cfg["geodetic"]["reject_on_mixed_datum"] is True


class TestAuditInvariants:
    def test_hash_algorithm_sha256(self, cfg):
        assert cfg["audit"]["hash_algorithm"] == "sha256"

    def test_log_format_jsonl(self, cfg):
        assert cfg["audit"]["log_format"] == "jsonl"

    def test_log_events_is_allowlist(self, cfg):
        events = cfg["audit"].get("log_events")
        assert isinstance(events, list) and events, (
            "audit.log_events must be a non-empty allowlist"
        )
