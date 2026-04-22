"""Q-5: a resolved quarantine item cannot be re-resolved.

Current behavior: /api/resolve pops the item from the queue after processing.
A second POST with the same item_id returns success=False with "Item not
found". This is idempotent-by-removal — the side effect (resolution file
write) cannot be doubled.

Future hardening per AGENTIC.md Q-5 should return HTTP 409 with the existing
decision; until that lands, this test pins the current "no duplicate side
effect" contract.
"""

from types import SimpleNamespace

import pytest

flask = pytest.importorskip("flask")

from totali.quarantine_ui.app import app, QUARANTINE_QUEUE, add_to_quarantine  # noqa: E402


@pytest.fixture
def client(tmp_path):
    QUARANTINE_QUEUE.clear()
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
        yield c
    QUARANTINE_QUEUE.clear()


class TestResolveIdempotency:
    def test_confirm_succeeds_once(self, client):
        resp = client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "confirm", "epsg": 2231},
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["success"] is True
        assert payload["epsg"] == 2231

    def test_second_confirm_reports_not_found(self, client):
        client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "confirm", "epsg": 2231},
        )
        resp2 = client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "confirm", "epsg": 2231},
        )
        payload = resp2.get_json()
        assert payload["success"] is False
        assert "not found" in payload["error"].lower()

    def test_reject_succeeds_once(self, client):
        resp = client.post(
            "/api/resolve", json={"item_id": "q1", "action": "reject"}
        )
        assert resp.get_json()["success"] is True

    def test_second_reject_reports_not_found(self, client):
        client.post("/api/resolve", json={"item_id": "q1", "action": "reject"})
        resp2 = client.post(
            "/api/resolve", json={"item_id": "q1", "action": "reject"}
        )
        assert resp2.get_json()["success"] is False


class TestUnknownAction:
    def test_unknown_action_returns_error(self, client):
        resp = client.post(
            "/api/resolve",
            json={"item_id": "q1", "action": "mutate-everything"},
        )
        payload = resp.get_json()
        assert payload["success"] is False

    def test_missing_item_id(self, client):
        resp = client.post("/api/resolve", json={"action": "confirm", "epsg": 2231})
        assert resp.get_json()["success"] is False
