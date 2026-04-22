"""Q-4: /api/resolve emits crs_resolved / crs_rejected_by_operator audit events
when an AuditLogger is injected via set_audit_logger().
"""

from types import SimpleNamespace

import pytest

flask = pytest.importorskip("flask")

from totali.audit.logger import AuditLogger  # noqa: E402
from totali.quarantine_ui.app import (  # noqa: E402
    QUARANTINE_QUEUE,
    add_to_quarantine,
    app,
    set_audit_logger,
)


@pytest.fixture
def client_and_audit(tmp_path):
    QUARANTINE_QUEUE.clear()
    audit = AuditLogger(log_dir=str(tmp_path / "audit"), project_id="qui")
    set_audit_logger(audit)
    cand = SimpleNamespace(epsg=2231, name="NAD83 CO N", confidence=0.92)
    add_to_quarantine(
        item_id="q1",
        filename="sample.las",
        point_count=100,
        bounds_min=[0.0, 0.0, 0.0],
        bounds_max=[100.0, 100.0, 100.0],
        candidates=[cand],
        output_dir=str(tmp_path),
    )
    with app.test_client() as c:
        yield c, audit
    QUARANTINE_QUEUE.clear()
    set_audit_logger(None)


class TestConfirmEmitsEvent:
    def test_confirm_emits_crs_resolved(self, client_and_audit):
        client, audit = client_and_audit
        resp = client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "confirm", "epsg": 2231, "operator": "sean-pls"},
        )
        assert resp.get_json()["success"] is True
        events = audit.get_events("crs_resolved")
        assert len(events) == 1
        payload = events[0]["data"]
        assert payload["item_id"] == "q1"
        assert payload["resolved_epsg"] == 2231
        assert payload["operator"] == "sean-pls"
        assert payload["filename"] == "sample.las"

    def test_confirm_without_operator_records_unknown(self, client_and_audit):
        client, audit = client_and_audit
        client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "confirm", "epsg": 2231},
        )
        assert audit.get_events("crs_resolved")[0]["data"]["operator"] == "unknown"


class TestRejectEmitsEvent:
    def test_reject_emits_crs_rejected_by_operator(self, client_and_audit):
        client, audit = client_and_audit
        resp = client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "reject", "operator": "sean-pls"},
        )
        assert resp.get_json()["success"] is True
        events = audit.get_events("crs_rejected_by_operator")
        assert len(events) == 1
        assert events[0]["data"]["item_id"] == "q1"
        assert events[0]["data"]["operator"] == "sean-pls"


class TestNoLoggerInjected:
    def test_resolve_works_without_logger(self, tmp_path):
        """When no logger is injected, resolve still succeeds (dev bring-up)."""
        QUARANTINE_QUEUE.clear()
        set_audit_logger(None)
        cand = SimpleNamespace(epsg=2231, name="x", confidence=0.9)
        add_to_quarantine(
            item_id="nolog",
            filename="f.las",
            point_count=1,
            bounds_min=[0.0, 0.0, 0.0],
            bounds_max=[1.0, 1.0, 1.0],
            candidates=[cand],
            output_dir=str(tmp_path),
        )
        with app.test_client() as c:
            resp = c.post(
                "/api/resolve",
                json={"item_id": "nolog", "action": "confirm", "epsg": 2231},
            )
        assert resp.get_json()["success"] is True
        QUARANTINE_QUEUE.clear()


class TestChainIntact:
    def test_chain_valid_after_resolve(self, client_and_audit):
        client, audit = client_and_audit
        client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "confirm", "epsg": 2231, "operator": "op"},
        )
        ok, errors = audit.verify_chain()
        assert ok is True, errors
