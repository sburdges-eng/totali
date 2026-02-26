"""
Telemetry zone normalization and registration.

Ingests raw machine telemetry records, normalizes them to a canonical
schema, and registers spatial zones in PostGIS.
"""
import logging

import psycopg2

logger = logging.getLogger(__name__)

# Canonical field mapping — maps common raw field names to standard names.
FIELD_ALIASES = {
    "blade_pressure": ["bladePressure", "blade_press", "bp"],
    "speed_m_s": ["speed", "ground_speed", "speed_ms", "velocity"],
    "pass_count": ["passCount", "pass_cnt", "passes"],
    "soil_resistance": ["soilResistance", "soil_resist", "resistance"],
    "rework_flag": ["reworkFlag", "rework", "is_rework"],
    "latitude": ["lat", "y"],
    "longitude": ["lon", "lng", "x"],
}


def _resolve_field(raw: dict, canonical: str) -> object | None:
    """Look up a canonical field name in a raw record, trying aliases."""
    if canonical in raw:
        return raw[canonical]
    for alias in FIELD_ALIASES.get(canonical, []):
        if alias in raw:
            return raw[alias]
    return None


def normalize_telemetry(raw_records: list[dict]) -> list[dict]:
    """Normalize raw telemetry records to canonical schema.

    Args:
        raw_records: List of dicts with varying field names.

    Returns:
        List of normalized dicts with standard field names.
    """
    normalized = []
    for raw in raw_records:
        record = {
            "blade_pressure": _resolve_field(raw, "blade_pressure"),
            "speed_m_s": _resolve_field(raw, "speed_m_s"),
            "pass_count": _resolve_field(raw, "pass_count"),
            "soil_resistance": _resolve_field(raw, "soil_resistance"),
            "rework_flag": bool(_resolve_field(raw, "rework_flag") or False),
            "latitude": _resolve_field(raw, "latitude"),
            "longitude": _resolve_field(raw, "longitude"),
        }

        # Convert numeric fields
        for field in ("blade_pressure", "speed_m_s", "soil_resistance"):
            if record[field] is not None:
                try:
                    record[field] = float(record[field])
                except (ValueError, TypeError):
                    record[field] = None

        if record["pass_count"] is not None:
            try:
                record["pass_count"] = int(record["pass_count"])
            except (ValueError, TypeError):
                record["pass_count"] = None

        normalized.append(record)

    logger.info("Normalized %d telemetry records", len(normalized))
    return normalized


def register_telemetry_zone(
    conn_string: str,
    geom_wkt: str,
    soil_resistance: float | None = None,
    rework_flag: bool = False,
    metadata: dict | None = None,
) -> str:
    """Register a telemetry zone polygon in PostGIS.

    Args:
        conn_string: PostgreSQL connection string.
        geom_wkt: WKT geometry string (POLYGON) in EPSG:4326.
        soil_resistance: Average soil resistance for the zone.
        rework_flag: Whether this zone required rework.
        metadata: Optional additional metadata.

    Returns:
        UUID of the inserted telemetry zone.
    """
    from psycopg2.extras import Json

    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO telemetry_zones
                    (geom, soil_resistance, rework_flag, metadata)
                VALUES
                    (ST_GeomFromText(%s, 4326), %s, %s, %s)
                RETURNING id
                """,
                (geom_wkt, soil_resistance, rework_flag, Json(metadata or {})),
            )
            zone_id = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Registered telemetry zone %s", zone_id)
    return str(zone_id)
