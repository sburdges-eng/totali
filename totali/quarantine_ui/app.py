"""
HITL quarantine queue and lightweight Flask resolver UI.
"""

from __future__ import annotations

import json
from pathlib import Path

from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

# In-memory queue for local review workflows.
QUARANTINE_QUEUE: dict[str, dict] = {}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <title>TOTaLi CRS Resolution</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    .container { display: flex; gap: 20px; }
    .map-container { flex: 2; }
    .panel { flex: 1; padding: 15px; background: #f5f5f5; border-radius: 8px; }
    #map { height: 500px; border-radius: 8px; }
    .candidate { padding: 10px; margin: 10px 0; background: white; border: 2px solid #ddd; border-radius: 4px; cursor: pointer; }
    .candidate:hover { border-color: #007bff; }
    .candidate.selected { border-color: #28a745; background: #e8f5e9; }
    .btn { padding: 12px 24px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; margin-top: 15px; }
    .btn-primary { background: #007bff; color: white; }
    .btn-danger { background: #dc3545; color: white; margin-left: 8px; }
    .stats { font-size: 12px; color: #666; }
  </style>
</head>
<body>
  <h1>CRS Resolution Required</h1>
  <p><strong>File:</strong> {{ item.filename }}</p>
  <p class="stats">Points: {{ item.point_count }} | Bounds: {{ item.bounds_str }}</p>

  <div class="container">
    <div class="map-container"><div id="map"></div></div>
    <div class="panel">
      <h3>Select Correct CRS</h3>
      {% for cand in item.candidates %}
      <div class="candidate" onclick="selectCandidate(this, {{ cand.epsg }})">
        <strong>EPSG:{{ cand.epsg }}</strong><br/>
        {{ cand.name }}<br/>
        <span class="stats">Confidence: {{ "%.0f"|format(cand.confidence * 100) }}%</span>
      </div>
      {% endfor %}
      <button class="btn btn-primary" onclick="confirmSelection()">Confirm Selection</button>
      <button class="btn btn-danger" onclick="rejectItem()">Reject File</button>
    </div>
  </div>

  <script>
    var map = L.map('map').setView([39.0, -105.5], 7);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '© OpenStreetMap' }).addTo(map);
    var layers = {};
    var candidates = {{ candidates_json|safe }};
    var selectedEpsg = null;

    candidates.forEach(function(c) {
      var bounds = [[c.geo_bounds[0], c.geo_bounds[2]], [c.geo_bounds[1], c.geo_bounds[3]]];
      layers[c.epsg] = L.rectangle(bounds, { color: '#007bff', weight: 2, fillOpacity: 0.2 });
    });

    function selectCandidate(el, epsg) {
      document.querySelectorAll('.candidate').forEach(function(c) { c.classList.remove('selected'); });
      el.classList.add('selected');
      selectedEpsg = epsg;
      Object.keys(layers).forEach(function(k) { if (map.hasLayer(layers[k])) { map.removeLayer(layers[k]); } });
      layers[epsg].addTo(map);
      map.fitBounds(layers[epsg].getBounds());
    }

    function confirmSelection() {
      if (!selectedEpsg) { alert('Please select a CRS first.'); return; }
      fetch('/api/resolve', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ item_id: '{{ item.id }}', epsg: selectedEpsg, action: 'confirm' })
      }).then(function(r) { return r.json(); }).then(function() { window.location.reload(); });
    }

    function rejectItem() {
      fetch('/api/resolve', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ item_id: '{{ item.id }}', action: 'reject' })
      }).then(function(r) { return r.json(); }).then(function() { window.location.reload(); });
    }
  </script>
</body>
</html>
"""


@app.route("/")
def index():
    if not QUARANTINE_QUEUE:
        return "<h1>No items in quarantine queue</h1>"
    item_id = next(iter(QUARANTINE_QUEUE.keys()))
    return render_item(item_id)


@app.route("/item/<item_id>")
def render_item(item_id: str):
    item = QUARANTINE_QUEUE.get(item_id)
    if not item:
        return "Item not found", 404

    candidates_with_geo = []
    for candidate in item["candidates"]:
        candidates_with_geo.append(
            {
                "epsg": candidate["epsg"],
                "name": candidate["name"],
                "confidence": candidate["confidence"],
                "geo_bounds": _epsg_to_geo_bounds(candidate["epsg"]),
            }
        )

    return render_template_string(
        HTML_TEMPLATE,
        item={
            "id": item_id,
            "filename": item["filename"],
            "point_count": item["point_count"],
            "bounds_str": f"({item['bounds_min']}) to ({item['bounds_max']})",
            "candidates": item["candidates"],
        },
        candidates_json=json.dumps(candidates_with_geo),
    )


@app.route("/api/resolve", methods=["POST"])
def resolve():
    data = request.json or {}
    item_id = data.get("item_id")
    action = data.get("action")
    if item_id not in QUARANTINE_QUEUE:
        return jsonify({"success": False, "error": "Item not found"})

    item = QUARANTINE_QUEUE.pop(item_id)
    if action == "confirm":
        epsg = int(data.get("epsg"))
        resolution_path = Path(item["output_dir"]) / f"{item_id}_crs_resolution.json"
        with open(resolution_path, "w") as handle:
            json.dump(
                {
                    "item_id": item_id,
                    "resolved_epsg": epsg,
                    "action": "confirmed",
                    "source": "human_review",
                },
                handle,
                indent=2,
            )
        return jsonify({"success": True, "epsg": epsg})
    if action == "reject":
        return jsonify({"success": True, "action": "rejected"})
    return jsonify({"success": False, "error": "Unknown action"})


def add_to_quarantine(
    item_id: str,
    filename: str,
    point_count: int,
    bounds_min: list,
    bounds_max: list,
    candidates: list,
    output_dir: str,
):
    QUARANTINE_QUEUE[item_id] = {
        "filename": filename,
        "point_count": point_count,
        "bounds_min": bounds_min,
        "bounds_max": bounds_max,
        "candidates": [
            {"epsg": c.epsg, "name": c.name, "confidence": c.confidence} for c in candidates
        ],
        "output_dir": output_dir,
    }


def _epsg_to_geo_bounds(epsg: int) -> list[float]:
    bounds_map = {
        2231: [40.0, 41.0, -109.0, -102.0],
        2232: [38.5, 40.0, -109.0, -102.0],
        2233: [37.0, 38.5, -109.0, -102.0],
        6428: [40.0, 41.0, -109.0, -102.0],
        6430: [38.5, 40.0, -109.0, -102.0],
        6432: [37.0, 38.5, -109.0, -102.0],
    }
    return bounds_map.get(epsg, [37.0, 41.0, -109.0, -102.0])


if __name__ == "__main__":
    app.run(debug=True, port=5050)
