"""Quarantine UI Flask application.

The pipeline pauses on low-confidence CRS inference and requires an operator
to resolve (`confirm`) or reject the item before work may proceed. This module
exposes the Flask `app`, the in-memory `QUARANTINE_QUEUE`, and hooks for
injecting an :class:`~totali.audit.logger.AuditLogger` so every resolution
emits a `crs_resolved` / `crs_rejected_by_operator` audit record (Q-4).

Idempotency (Q-5): the queue is drained by resolution — a second POST against
the same `item_id` returns ``{"success": False, "error": "Item not found"}``
with HTTP 200, because the first call already committed the decision.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

# Module-level state. The UI is a single-process gate; a dict is sufficient
# and keeps resolution semantics trivial for tests (Q-5 idempotency falls out
# naturally from ``pop``). Production operation is one pipeline run per UI.
QUARANTINE_QUEUE: dict[str, dict] = {}

# Audit sink. ``None`` means "no-op" — the UI still functions without an audit
# chain wired up (useful for local/dev usage), but production callers must
# inject a real AuditLogger via :func:`set_audit_logger`.
_AUDIT_LOGGER: Any = None


def set_audit_logger(logger: Any) -> None:
    """Inject (or clear, with ``None``) the audit logger used for Q-4 events."""
    global _AUDIT_LOGGER
    _AUDIT_LOGGER = logger


def _emit_audit(event: str, payload: dict) -> None:
    """Best-effort audit emission. No-op when no logger is wired up."""
    if _AUDIT_LOGGER is None:
        return
    _AUDIT_LOGGER.log(event, payload)


def add_to_quarantine(
    item_id: str,
    filename: str,
    point_count: int,
    bounds_min: Any,
    bounds_max: Any,
    candidates: list,
    output_dir: str,
) -> None:
    """Register an item awaiting operator CRS resolution.

    ``candidates`` entries may be dataclasses, ``SimpleNamespace`` objects, or
    plain dicts — we read ``epsg`` / ``name`` / ``confidence`` via ``getattr``
    with a dict fallback so tests and production callers can share a shape.
    """
    normalized_candidates = []
    for c in candidates:
        if isinstance(c, dict):
            normalized_candidates.append(
                {
                    "epsg": c.get("epsg"),
                    "name": c.get("name"),
                    "confidence": c.get("confidence"),
                }
            )
        else:
            normalized_candidates.append(
                {
                    "epsg": getattr(c, "epsg", None),
                    "name": getattr(c, "name", None),
                    "confidence": getattr(c, "confidence", None),
                }
            )

    QUARANTINE_QUEUE[item_id] = {
        "item_id": item_id,
        "filename": filename,
        "point_count": point_count,
        "bounds_min": list(bounds_min) if bounds_min is not None else None,
        "bounds_max": list(bounds_max) if bounds_max is not None else None,
        "candidates": normalized_candidates,
        "output_dir": output_dir,
        "added_at": datetime.now(timezone.utc).isoformat(),
    }


def _render_item(item: dict) -> str:
    """Render a quarantined item as a minimal HTML-escaped page.

    Deliberately avoids Jinja templates — we want zero filesystem-lookup
    surprises when the module is imported in isolation under test.
    """
    lines = [
        "<!doctype html>",
        "<html><head><title>Quarantine · {}</title></head><body>".format(
            escape(item["item_id"])
        ),
        "<h1>Quarantined item</h1>",
        "<pre>" + escape(json.dumps(item, indent=2, default=str)) + "</pre>",
        '<form method="post" action="/api/resolve">',
        '  <input type="hidden" name="item_id" value="{}">'.format(escape(item["item_id"])),
        "</form>",
        "</body></html>",
    ]
    return "\n".join(lines)


@app.route("/", methods=["GET"])
def index() -> Any:
    """Landing page: render the first pending item, or a friendly empty state."""
    if not QUARANTINE_QUEUE:
        return "No items"
    # Deterministic: first-inserted wins (dict preserves insertion order).
    first_id = next(iter(QUARANTINE_QUEUE))
    return _render_item(QUARANTINE_QUEUE[first_id])


@app.route("/item/<item_id>", methods=["GET"])
def item_detail(item_id: str) -> Any:
    """Dedicated per-item view."""
    item = QUARANTINE_QUEUE.get(item_id)
    if item is None:
        return ("Not found", 404)
    return _render_item(item)


@app.route("/health", methods=["GET"])
def health() -> Any:
    """Liveness probe. Reports queue depth for ops dashboards."""
    return jsonify({"status": "ok", "queue_length": len(QUARANTINE_QUEUE)})


@app.route("/api/resolve", methods=["POST"])
def resolve() -> Any:
    """Operator resolves one quarantined item.

    Payload:
        {
          "item_id": "<id>",
          "action": "confirm" | "reject",
          "epsg": int,          # required for confirm
          "operator": "<id>"    # carried into the audit record (Q-4)
        }

    On ``confirm`` we write ``<item_id>_crs_resolution.json`` into the item's
    ``output_dir`` so the downstream pipeline can read the decision and
    release its wait-gate. On ``reject`` we emit the audit event and drop
    the item; the pipeline halts.

    Q-5 idempotency: a second call against the same ``item_id`` returns
    ``{"success": False, "error": "Item not found"}`` (HTTP 200).
    """
    payload = request.get_json(silent=True) or {}
    item_id = payload.get("item_id")
    action = payload.get("action")
    operator = payload.get("operator", "unknown")

    if not item_id:
        return jsonify({"success": False, "error": "Missing item_id"}), 400

    item = QUARANTINE_QUEUE.pop(item_id, None)
    if item is None:
        # Q-5: second resolve on the same item. Intentionally HTTP 200 — the
        # decision is final, so the caller's retry sees a well-formed "no-op"
        # body rather than a 409 they'd have to special-case.
        return jsonify({"success": False, "error": "Item not found"})

    if action == "confirm":
        epsg = payload.get("epsg")
        if epsg is None:
            # Put the item back so the operator can retry. The audit chain is
            # deliberately NOT touched for malformed requests.
            QUARANTINE_QUEUE[item_id] = item
            return jsonify({"success": False, "error": "Missing epsg"}), 400

        output_dir = Path(item.get("output_dir", "."))
        output_dir.mkdir(parents=True, exist_ok=True)
        resolution_path = output_dir / f"{item_id}_crs_resolution.json"
        resolution = {
            "item_id": item_id,
            "action": "confirm",
            "epsg": epsg,
            "operator": operator,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        resolution_path.write_text(json.dumps(resolution, indent=2))

        _emit_audit(
            "crs_resolved",
            {
                "item_id": item_id,
                "epsg": epsg,
                "operator": operator,
                "filename": item.get("filename"),
            },
        )
        return jsonify({"success": True, "item_id": item_id, "epsg": epsg})

    if action == "reject":
        _emit_audit(
            "crs_rejected_by_operator",
            {
                "item_id": item_id,
                "operator": operator,
                "filename": item.get("filename"),
                "reason": payload.get("reason"),
            },
        )
        return jsonify({"success": True, "item_id": item_id, "action": "reject"})

    # Unknown action — we already popped the item above, so restore it to keep
    # the queue consistent for a corrected retry. No audit event for garbage.
    QUARANTINE_QUEUE[item_id] = item
    return jsonify({"success": False, "error": f"Unknown action: {action!r}"}), 400
