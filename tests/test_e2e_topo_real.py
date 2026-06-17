"""U4 (scaffold): end-to-end topo → defensible DXF on real partner data.

The full real-LAS run with geodetic gates ACTIVE (not the stub-relaxed E2E in
``test_pipeline_e2e.py``) is the one piece of this plan that genuinely needs a
dataset we do not have. So this file is split:

- ``TestRealPartnerLasE2E`` — the real run. SKIPPED unless ``TOTALI_PARTNER_LAS``
  points at a real partner LAS sample. Its body is the executable *contract* for
  when that dataset lands: full PHASE_ORDER, gates active, stampable DXF carrying
  an audit reference, audit chain verifies end-to-end. **TODO(partner-data):**
  obtain the partner LAS sample + agreed DXF deliverable spec, then this runs.
- ``TestDwgLoudStub`` — runs always (no data needed): a DWG export request must
  fail loudly (KTD2). This is the U4 "DWG export request fails loudly" scenario.

Nothing here fabricates partner data: the real test is inert until a real file
is provided out-of-band via the environment.
"""

from __future__ import annotations

import json
import os

import pytest

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log
from totali.cad_shielding.shield import CADShield, UnsupportedCADFormat
from totali.pipeline.orchestrator import PHASE_ORDER, PipelineOrchestrator

_PARTNER_LAS = os.environ.get("TOTALI_PARTNER_LAS")


@pytest.mark.skipif(
    not _PARTNER_LAS,
    reason=(
        "U4 real-LAS E2E needs a partner dataset (set TOTALI_PARTNER_LAS to a real "
        "topo LAS). TODO(partner-data): wire the agreed DXF deliverable spec + zone "
        "envelope and run with geodetic gates active."
    ),
)
class TestRealPartnerLasE2E:
    """Contract for the un-relaxed, real-data run. Inert until the dataset lands."""

    def _gated_config(self, sample_config: dict) -> dict:
        cfg = dict(sample_config)
        cfg["geodetic"] = dict(sample_config["geodetic"])
        # Gates ACTIVE — the whole point vs. the stub-relaxed E2E. The partner's
        # real LAS must declare (or allow inference of) a CRS in the allowlist.
        cfg["geodetic"]["reject_on_missing_crs"] = True
        cfg["geodetic"]["reject_on_mixed_datum"] = True
        # TODO(partner-data): populate jurisdiction_zones with the partner zone
        # envelope and set crs_inference_enabled if the LAS lacks a CRS VLR.
        return cfg

    def test_real_las_flows_all_phases_to_stampable_dxf(self, sample_config, tmp_path):
        cfg = self._gated_config(sample_config)
        out = tmp_path / "out"
        out.mkdir()
        audit = AuditLogger(log_dir=str(tmp_path / "audit"), project_id="u4_real")
        orch = PipelineOrchestrator(cfg, audit, out)

        result = orch.run(_PARTNER_LAS, phase="all")

        # All five phases ran with gates active.
        assert [p.phase for p in result.phases] == PHASE_ORDER
        assert result.success is True, [
            (p.phase, p.success, p.message) for p in result.phases
        ]

        # A DXF was produced and carries an audit reference.
        dxf = out / "totali_draft_output.dxf"
        assert dxf.exists()
        manifest = json.loads((out / "entity_manifest.json").read_text())
        assert manifest["audit_reference"]["input_hash"]

        # The audit chain verifies end-to-end (SC1).
        ok, errors = verify_log(audit.log_path)
        assert ok is True, errors


class TestDwgLoudStub:
    """KTD2: DWG export must fail loudly — no silent half-support. Runs always."""

    def _cfg(self, fmt: str) -> dict:
        return {
            "format": fmt,
            "geometry_healing": {"close_tolerance": 0.001},
            "layer_mapping": {"ground_surface": "TOTaLi-SURV-DTM-DRAFT"},
        }

    def test_dwg_request_fails_loudly(self, audit_logger):
        with pytest.raises(UnsupportedCADFormat) as exc:
            CADShield(self._cfg("dwg"), audit_logger)
        assert "dwg" in str(exc.value).lower()

    def test_dxf_is_accepted(self, audit_logger):
        shield = CADShield(self._cfg("dxf"), audit_logger)
        assert shield.format == "dxf"
