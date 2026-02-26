-- 003_knowledge_graph.sql
-- Knowledge graph links and lineage view for tile → surface → telemetry → outcome.

-- ============================================================
-- tile_surface_link — many-to-many: tiles → surfaces
-- ============================================================
CREATE TABLE IF NOT EXISTS tile_surface_link (
    tile_id     UUID NOT NULL REFERENCES lidar_tiles(id) ON DELETE CASCADE,
    surface_id  UUID NOT NULL REFERENCES surfaces(id) ON DELETE CASCADE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (tile_id, surface_id)
);

-- ============================================================
-- surface_telemetry_link — many-to-many: surfaces → telemetry zones
-- ============================================================
CREATE TABLE IF NOT EXISTS surface_telemetry_link (
    surface_id  UUID NOT NULL REFERENCES surfaces(id) ON DELETE CASCADE,
    zone_id     UUID NOT NULL REFERENCES telemetry_zones(id) ON DELETE CASCADE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (surface_id, zone_id)
);

-- ============================================================
-- outcome_variance — measured outcomes tied to telemetry zones
-- ============================================================
CREATE TABLE IF NOT EXISTS outcome_variance (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zone_id         UUID NOT NULL REFERENCES telemetry_zones(id) ON DELETE CASCADE,
    metric_name     VARCHAR(100) NOT NULL,
    predicted_value FLOAT,
    measured_value  FLOAT,
    variance        FLOAT,
    measured_at     TIMESTAMP WITH TIME ZONE,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outcome_variance_zone ON outcome_variance(zone_id);

-- ============================================================
-- lineage_view — full chain: dataset → tile → surface → telemetry → outcome
-- ============================================================
CREATE OR REPLACE VIEW lineage_view AS
SELECT
    d.id            AS dataset_id,
    d.source_name,
    t.id            AS tile_id,
    t.file_path     AS tile_path,
    t.density_pts_m2,
    s.id            AS surface_id,
    s.version_hash,
    s.rmse          AS surface_rmse,
    tz.id           AS zone_id,
    tz.soil_resistance,
    tz.rework_flag,
    ov.id           AS outcome_id,
    ov.metric_name,
    ov.predicted_value,
    ov.measured_value,
    ov.variance
FROM datasets d
JOIN lidar_tiles t          ON t.dataset_id  = d.id
LEFT JOIN tile_surface_link tsl ON tsl.tile_id    = t.id
LEFT JOIN surfaces s        ON s.id          = tsl.surface_id
LEFT JOIN surface_telemetry_link stl ON stl.surface_id = s.id
LEFT JOIN telemetry_zones tz ON tz.id         = stl.zone_id
LEFT JOIN outcome_variance ov ON ov.zone_id   = tz.id;
