"""Tests for HITL quarantine UI: add_to_quarantine and resolve API."""

import json
from pathlib import Path

import pytest

from totali.geodetic.crs_inference import EPSGCandidate
from totali.quarantine_ui.app import (
    QUARANTINE_QUEUE,
    add_to_quarantine,
    app,
    _epsg_to_geo_bounds,
)


@pytest.fixture(autouse=True)
def clear_quarantine_queue():
    """Clear in-memory queue before and after each test."""
    QUARANTINE_QUEUE.clear()
    yield
    QUARANTINE_QUEUE.clear()


@pytest.fixture
def client():
    """Flask test client."""
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def sample_candidates():
    """EPSG candidates as returned by CRSInferenceEngine."""
    return [
        EPSGCandidate(2231, "NAD83 CO North (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot", 0.75),
        EPSGCandidate(2232, "NAD83 CO Central (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot", 0.65),
    ]


class TestAddToQuarantine:
    def test_add_to_quarantine_populates_queue(self, sample_candidates, tmp_path):
        add_to_quarantine(
            item_id="abc12345",
            filename="survey.las",
            point_count=1000,
            bounds_min=[200000.0, 50000.0, 0.0],
            bounds_max=[250000.0, 100000.0, 10.0],
            candidates=sample_candidates,
            output_dir=str(tmp_path),
        )
        assert "abc12345" in QUARANTINE_QUEUE
        item = QUARANTINE_QUEUE["abc12345"]
        assert item["filename"] == "survey.las"
        assert item["point_count"] == 1000
        assert len(item["candidates"]) == 2
        assert item["candidates"][0]["epsg"] == 2231
        assert item["candidates"][0]["name"] == "NAD83 CO North (ftUS)"
        assert item["candidates"][0]["confidence"] == 0.75
        assert item["output_dir"] == str(tmp_path)

    def test_candidates_serialized_from_dataclass(self, sample_candidates):
        add_to_quarantine(
            item_id="id1",
            filename="f.las",
            point_count=5,
            bounds_min=[0, 0, 0],
            bounds_max=[1, 1, 1],
            candidates=sample_candidates,
            output_dir="/tmp",
        )
        for c in QUARANTINE_QUEUE["id1"]["candidates"]:
            assert "epsg" in c
            assert "name" in c
            assert "confidence" in c


class TestIndexAndRender:
    def test_index_empty_queue_returns_message(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "No items" in r.get_data(as_text=True)

    def test_index_redirects_to_first_item(self, client, sample_candidates, tmp_path):
        add_to_quarantine(
            item_id="first",
            filename="a.las",
            point_count=10,
            bounds_min=[0, 0, 0],
            bounds_max=[1, 1, 1],
            candidates=sample_candidates,
            output_dir=str(tmp_path),
        )
        r = client.get("/")
        assert r.status_code == 200
        assert "a.las" in r.get_data(as_text=True)
        assert "EPSG:2231" in r.get_data(as_text=True)

    def test_render_item_404_when_missing(self, client):
        r = client.get("/item/nonexistent")
        assert r.status_code == 404

    def test_render_item_200_with_candidates(self, client, sample_candidates, tmp_path):
        add_to_quarantine(
            item_id="item99",
            filename="data.las",
            point_count=500,
            bounds_min=[100.0, 200.0, 0.0],
            bounds_max=[300.0, 400.0, 10.0],
            candidates=sample_candidates,
            output_dir=str(tmp_path),
        )
        r = client.get("/item/item99")
        assert r.status_code == 200
        html = r.get_data(as_text=True)
        assert "data.las" in html
        assert "2231" in html
        assert "2232" in html


class TestResolveAPI:
    def test_resolve_confirm_writes_resolution_json(self, client, sample_candidates, tmp_path):
        add_to_quarantine(
            item_id="resolve_me",
            filename="survey.las",
            point_count=100,
            bounds_min=[0, 0, 0],
            bounds_max=[1, 1, 1],
            candidates=sample_candidates,
            output_dir=str(tmp_path),
        )
        r = client.post(
            "/api/resolve",
            json={"item_id": "resolve_me", "action": "confirm", "epsg": 2232},
            content_type="application/json",
        )
        data = r.get_json()
        assert data["success"] is True
        assert data["epsg"] == 2232
        assert "resolve_me" not in QUARANTINE_QUEUE

        resolution_file = tmp_path / "resolve_me_crs_resolution.json"
        assert resolution_file.exists()
        with open(resolution_file) as f:
            resolution = json.load(f)
        assert resolution["item_id"] == "resolve_me"
        assert resolution["resolved_epsg"] == 2232
        assert resolution["action"] == "confirmed"
        assert resolution["source"] == "human_review"

    def test_resolve_reject_removes_item_no_file(self, client, sample_candidates, tmp_path):
        add_to_quarantine(
            item_id="reject_me",
            filename="bad.las",
            point_count=1,
            bounds_min=[0, 0, 0],
            bounds_max=[1, 1, 1],
            candidates=sample_candidates,
            output_dir=str(tmp_path),
        )
        r = client.post(
            "/api/resolve",
            json={"item_id": "reject_me", "action": "reject"},
            content_type="application/json",
        )
        data = r.get_json()
        assert data["success"] is True
        assert data["action"] == "rejected"
        assert "reject_me" not in QUARANTINE_QUEUE
        assert not (tmp_path / "reject_me_crs_resolution.json").exists()

    def test_resolve_item_not_found(self, client):
        r = client.post(
            "/api/resolve",
            json={"item_id": "nonexistent", "action": "confirm", "epsg": 2231},
            content_type="application/json",
        )
        data = r.get_json()
        assert data["success"] is False
        assert "error" in data

    def test_resolve_unknown_action(self, client, sample_candidates, tmp_path):
        add_to_quarantine(
            item_id="unk",
            filename="f.las",
            point_count=1,
            bounds_min=[0, 0, 0],
            bounds_max=[1, 1, 1],
            candidates=sample_candidates,
            output_dir=str(tmp_path),
        )
        r = client.post(
            "/api/resolve",
            json={"item_id": "unk", "action": "invalid_action"},
            content_type="application/json",
        )
        data = r.get_json()
        assert data["success"] is False


class TestEpsgToGeoBounds:
    def test_known_epsg_returns_bounds(self):
        b = _epsg_to_geo_bounds(2231)
        assert len(b) == 4
        assert b[0] == 40.0 and b[1] == 41.0
        assert b[2] == -109.0 and b[3] == -102.0

    def test_unknown_epsg_returns_default_colorado(self):
        b = _epsg_to_geo_bounds(99999)
        assert len(b) == 4
        assert 37.0 <= b[0] <= 41.0 and -109.0 <= b[2] <= -102.0
