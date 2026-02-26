-- 002_feature_store.sql
-- Feature tables for tiles, surfaces, telemetry, and decimation audit log.

-- ============================================================
-- tile_features — per-tile geomorphometric features
-- ============================================================
CREATE TABLE IF NOT EXISTS tile_features (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tile_id             UUID NOT NULL REFERENCES lidar_tiles(id) ON DELETE CASCADE,
    feature_version     VARCHAR(50) NOT NULL,
    z_mean              FLOAT,
    z_std               FLOAT,
    z_min               FLOAT,
    z_max               FLOAT,
    slope_mean          FLOAT,
    slope_max           FLOAT,
    curvature_mean      FLOAT,
    roughness           FLOAT,
    breakline_prob      FLOAT,
    feature_vector      JSONB NOT NULL DEFAULT '{}',
    computed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (tile_id, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_tile_features_tile ON tile_features(tile_id);
CREATE INDEX IF NOT EXISTS idx_tile_features_version ON tile_features(feature_version);

-- ============================================================
-- surface_features — per-surface quality and volumetric features
-- ============================================================
CREATE TABLE IF NOT EXISTS surface_features (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    surface_id          UUID NOT NULL REFERENCES surfaces(id) ON DELETE CASCADE,
    feature_version     VARCHAR(50) NOT NULL,
    rmse                FLOAT,
    cut_volume_m3       FLOAT,
    fill_volume_m3      FLOAT,
    slope_violation_pct FLOAT,
    metadata            JSONB NOT NULL DEFAULT '{}',
    computed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (surface_id, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_surface_features_surface ON surface_features(surface_id);
CREATE INDEX IF NOT EXISTS idx_surface_features_version ON surface_features(feature_version);

-- ============================================================
-- telemetry_features — per-zone machine telemetry features
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_features (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zone_id             UUID NOT NULL REFERENCES telemetry_zones(id) ON DELETE CASCADE,
    feature_version     VARCHAR(50) NOT NULL,
    blade_pressure      FLOAT,
    speed_m_s           FLOAT,
    pass_count          INTEGER,
    soil_resistance     FLOAT,
    rework_flag         BOOLEAN NOT NULL DEFAULT FALSE,
    metadata            JSONB NOT NULL DEFAULT '{}',
    computed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (zone_id, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_telemetry_features_zone ON telemetry_features(zone_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_features_version ON telemetry_features(feature_version);

-- ============================================================
-- decimation_log — audit trail for adaptive decimation runs
-- ============================================================
CREATE TABLE IF NOT EXISTS decimation_log (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tile_id             UUID NOT NULL REFERENCES lidar_tiles(id) ON DELETE CASCADE,
    original_density    FLOAT NOT NULL,
    new_density         FLOAT NOT NULL,
    voxel_size          FLOAT NOT NULL,
    preserved_ratio     FLOAT,
    breakline_protected BOOLEAN NOT NULL DEFAULT FALSE,
    input_path          TEXT,
    output_path         TEXT,
    executed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decimation_log_tile ON decimation_log(tile_id);
