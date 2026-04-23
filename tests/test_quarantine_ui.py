"""Quarantine UI tests — routes, Q-4 audit emission, Q-5 idempotency.

Flask is an optional UI dependency: if it isn't installed, the whole module
skips rather than failing collection. In the CI environment Flask is present.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

pytest.importorskip("flask")

from totali.quarantine_ui.app import (  # noqa: E402
    QUARANTINE_QUEUE,
    add_to_quarantine,
    app,
    set_audit_logger,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_state():
    """Wipe module state before and after every test."""
    QUARANTINE_QUEUE.clear()
    set_audit_logger(None)
    yield
    QUARANTINE_QUEUE.clear()
    set_audit_logger(None)


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _seed_item(tmp_path, item_id="item-1", filename="survey.las"):
    add_to_quarantine(
        item_id=item_id,
        filename=filename,
        point_count=12345,
        bounds_min=(0.0, 0.0, 0.0),
        bounds_max=(100.0, 100.0, 10.0),
        candidates=[
            SimpleNamespace(epsg=26913, name="NAD83 / UTM 13N", confidence=0.71),
            SimpleNamespace(epsg=32113, name="NAD83 / New Mexico Central", confidence=0.58),
        ],
        output_dir=str(tmp_path),
    )


class _RecordingAuditLogger:
    """Minimal AuditLogger stand-in: appends (event, payload) tuples."""

    def __init__(self):
        self.events: list[tuple[str, dict]] = []

    def log(self, event_type: str, data: dict) -> None:
        self.events.append((event_type, dict(data)))


# ---------------------------------------------------------------------------
# Q-1 / queue management
# ---------------------------------------------------------------------------


class TestQueueManagement:
    def test_empty_queue_renders_no_items(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"No items" in resp.data

    def test_add_then_render_first_item(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"item-1" in resp.data
        assert b"Quarantined item" in resp.data

    def test_item_detail_404_when_missing(self, client):
        resp = client.get("/item/does-not-exist")
        assert resp.status_code == 404

    def test_item_detail_200_when_present(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.get("/item/item-1")
        assert resp.status_code == 200
        assert b"item-1" in resp.data

    def test_dict_candidates_accepted(self, client, tmp_path):
        """Dicts should be a valid candidate shape alongside SimpleNamespace."""
        add_to_quarantine(
            item_id="item-dict",
            filename="x.las",
            point_count=10,
            bounds_min=(0, 0, 0),
            bounds_max=(1, 1, 1),
            candidates=[{"epsg": 4326, "name": "WGS84", "confidence": 0.9}],
            output_dir=str(tmp_path),
        )
        resp = client.get("/item/item-dict")
        assert resp.status_code == 200
        assert b"4326" in resp.data


# ---------------------------------------------------------------------------
# Confirm / Reject happy paths
# ---------------------------------------------------------------------------


class TestConfirmFlow:
    def test_confirm_succeeds_once(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        assert body["epsg"] == 26913
        assert "item-1" not in QUARANTINE_QUEUE

    def test_confirm_writes_resolution_file(self, client, tmp_path):
        _seed_item(tmp_path)
        client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        resolution = tmp_path / "item-1_crs_resolution.json"
        assert resolution.exists()
        data = json.loads(resolution.read_text())
        assert data["action"] == "confirm"
        assert data["epsg"] == 26913
        assert data["operator"] == "alice"

    def test_confirm_missing_epsg_rejected(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "operator": "alice"},
        )
        assert resp.status_code == 400
        # Item must survive a malformed request so the operator can retry.
        assert "item-1" in QUARANTINE_QUEUE


class TestRejectFlow:
    def test_reject_succeeds_once(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "reject", "operator": "bob"},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        assert body["action"] == "reject"
        assert "item-1" not in QUARANTINE_QUEUE


# ---------------------------------------------------------------------------
# Q-4 audit emission
# ---------------------------------------------------------------------------


class TestQ4AuditEmission:
    def test_confirm_emits_crs_resolved_with_operator(self, client, tmp_path):
        audit = _RecordingAuditLogger()
        set_audit_logger(audit)
        _seed_item(tmp_path)
        client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        assert len(audit.events) == 1
        event, payload = audit.events[0]
        assert event == "crs_resolved"
        assert payload["operator"] == "alice"
        assert payload["epsg"] == 26913
        assert payload["item_id"] == "item-1"

    def test_reject_emits_crs_rejected_by_operator_with_operator(self, client, tmp_path):
        audit = _RecordingAuditLogger()
        set_audit_logger(audit)
        _seed_item(tmp_path)
        client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "reject", "operator": "bob", "reason": "bad CRS"},
        )
        assert len(audit.events) == 1
        event, payload = audit.events[0]
        assert event == "crs_rejected_by_operator"
        assert payload["operator"] == "bob"
        assert payload["item_id"] == "item-1"
        assert payload["reason"] == "bad CRS"

    def test_confirm_without_logger_still_succeeds(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        assert resp.status_code == 200
        assert resp.get_json()["success"] is True

    def test_reject_without_logger_still_succeeds(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "reject", "operator": "bob"},
        )
        assert resp.status_code == 200
        assert resp.get_json()["success"] is True


# ---------------------------------------------------------------------------
# Q-5 idempotency
# ---------------------------------------------------------------------------


class TestQ5Idempotency:
    def test_second_confirm_returns_not_found(self, client, tmp_path):
        _seed_item(tmp_path)
        first = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        assert first.get_json()["success"] is True

        second = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        assert second.status_code == 200
        body = second.get_json()
        assert body == {"success": False, "error": "Item not found"}

    def test_second_reject_returns_not_found(self, client, tmp_path):
        _seed_item(tmp_path)
        client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "reject", "operator": "bob"},
        )
        second = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "reject", "operator": "bob"},
        )
        assert second.status_code == 200
        assert second.get_json() == {"success": False, "error": "Item not found"}

    def test_second_resolve_emits_no_audit_event(self, client, tmp_path):
        audit = _RecordingAuditLogger()
        set_audit_logger(audit)
        _seed_item(tmp_path)
        client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "confirm", "epsg": 26913, "operator": "alice"},
        )
        # Only the first call crosses the audit chain.
        assert len(audit.events) == 1


# ---------------------------------------------------------------------------
# Health + unknown actions
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_health_returns_ok_and_queue_length(self, client, tmp_path):
        resp = client.get("/health")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["status"] == "ok"
        assert body["queue_length"] == 0

        _seed_item(tmp_path, item_id="a")
        _seed_item(tmp_path, item_id="b")
        body = client.get("/health").get_json()
        assert body["queue_length"] == 2


class TestUnknownAction:
    def test_unknown_action_returns_success_false(self, client, tmp_path):
        _seed_item(tmp_path)
        resp = client.post(
            "/api/resolve",
            json={"item_id": "item-1", "action": "mutate", "operator": "mallory"},
        )
        body = resp.get_json()
        assert body["success"] is False
        # And the item is still in the queue — unknown actions are not terminal.
        assert "item-1" in QUARANTINE_QUEUE

    def test_missing_item_id_returns_400(self, client):
        resp = client.post("/api/resolve", json={"action": "confirm"})
        assert resp.status_code == 400
        assert resp.get_json()["success"] is False
