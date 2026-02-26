-- 001_core_registry.sql
-- Core dataset registry, tile catalog, usage tracking, and surface modeling tables.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- datasets — top-level ingested dataset records
-- ============================================================
CREATE TABLE IF NOT EXISTS datasets (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name   VARCHAR(200) NOT NULL,
    source_url    TEXT,
    license_type  VARCHAR(100) NOT NULL,
    license_meta  JSONB NOT NULL DEFAULT '{}',
    crs           VARCHAR(200),
    checksum      CHAR(64),
    file_path     TEXT,
    file_format   VARCHAR(20),
    file_size     BIGINT,
    approval      VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (approval IN ('pending', 'approved', 'rejected')),
    acquired_at   TIMESTAMP WITH TIME ZONE,
    ingested_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata      JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_datasets_source ON datasets(source_name);
CREATE INDEX IF NOT EXISTS idx_datasets_approval ON datasets(approval);

-- ============================================================
-- lidar_tiles — individual tiles produced from a dataset
-- ============================================================
CREATE TABLE IF NOT EXISTS lidar_tiles (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id       UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    file_path        TEXT NOT NULL,
    geom             GEOMETRY(Polygon, 4326) NOT NULL,
    point_count      BIGINT,
    density_pts_m2   FLOAT,
    z_min            FLOAT,
    z_max            FLOAT,
    z_std            FLOAT,
    classification   JSONB NOT NULL DEFAULT '{}',
    crs              VARCHAR(200),
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lidar_tiles_geom ON lidar_tiles USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_lidar_tiles_dataset ON lidar_tiles(dataset_id);

-- ============================================================
-- tile_usage — tracks which model version used which tiles
-- ============================================================
CREATE TABLE IF NOT EXISTS tile_usage (
    tile_id         UUID NOT NULL REFERENCES lidar_tiles(id) ON DELETE CASCADE,
    model_version   VARCHAR(100) NOT NULL,
    split           VARCHAR(10) NOT NULL CHECK (split IN ('train', 'val', 'test')),
    assigned_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (tile_id, model_version)
);

-- ============================================================
-- surfaces — graded or designed surfaces derived from tiles
-- ============================================================
CREATE TABLE IF NOT EXISTS surfaces (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    version_hash      CHAR(64) NOT NULL,
    rmse              FLOAT,
    parent_surface_id UUID REFERENCES surfaces(id),
    metadata          JSONB NOT NULL DEFAULT '{}',
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_surfaces_parent ON surfaces(parent_surface_id);

-- ============================================================
-- telemetry_zones — spatial zones with machine telemetry data
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_zones (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    geom             GEOMETRY(Polygon, 4326) NOT NULL,
    soil_resistance  FLOAT,
    rework_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    metadata         JSONB NOT NULL DEFAULT '{}',
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_zones_geom ON telemetry_zones USING GIST(geom);
