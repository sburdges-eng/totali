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
import subprocess
import sys
from pathlib import Path

import pytest

from totali.cad_shielding.shield import CADShield, UnsupportedCADFormat

_PARTNER_LAS = os.environ.get("TOTALI_PARTNER_LAS")
_REPO_ROOT = Path(__file__).resolve().parents[1]
_RUNNER = _REPO_ROOT / "tools" / "run_partner_las_e2e.py"


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

    def test_real_las_flows_all_phases_to_stampable_dxf(self, tmp_path):
        """Subprocess runner — conftest stubs laspy; real LAS needs real laspy."""
        pytest.importorskip("laspy")
        pytest.importorskip("pyproj")
        assert _RUNNER.is_file(), f"missing runner: {_RUNNER}"

        out = tmp_path / "out"
        audit = tmp_path / "audit"
        result = subprocess.run(
            [
                sys.executable,
                str(_RUNNER),
                "--output-dir",
                str(out),
                "--audit-dir",
                str(audit),
                "--project-id",
                "u4_real",
            ],
            cwd=str(_REPO_ROOT),
            env={**os.environ, "TOTALI_PARTNER_LAS": _PARTNER_LAS},
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            f"partner LAS E2E exited {result.returncode}; "
            f"stderr:\n{result.stderr}\nstdout:\n{result.stdout}"
        )

        summary = json.loads(result.stdout)
        assert summary["success"] is True
        assert summary["phase_order_ok"] is True
        assert summary["dxf_exists"] is True
        assert summary["audit_verify_ok"] is True
        assert summary["input_hash"]


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
