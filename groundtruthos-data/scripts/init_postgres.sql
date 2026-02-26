-- GroundTruthOS supplemental PostGIS tables for the download pipeline.
-- Run AFTER the core schema (schema/001_core_registry.sql).
--
-- These tables supplement the core registry with download-specific
-- tracking: acquisition catalog, tile manifests, and compliance audit.
--
-- Usage:
--   psql -d groundtruth -f scripts/init_postgres.sql

-- Download-pipeline catalog (supplements core 'datasets' table)
-- Used by downloader/registry.py for dedup and spatial queries during acquisition.
CREATE TABLE IF NOT EXISTS dataset_catalog (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    tile_id VARCHAR(500) NOT NULL,
    local_path TEXT NOT NULL,
    format VARCHAR(20) NOT NULL,
    crs VARCHAR(200),
    acquisition_date DATE,
    download_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    file_size_bytes BIGINT,
    checksum_sha256 CHAR(64),
    point_count BIGINT,
    point_density_per_m2 FLOAT,
    z_min FLOAT,
    z_max FLOAT,
    vertical_datum VARCHAR(50),
    horizontal_datum VARCHAR(50),
    license JSONB NOT NULL,
    quality_status VARCHAR(20) DEFAULT 'pending'
        CHECK (quality_status IN ('pending', 'passed', 'flagged', 'rejected')),
    quality_notes TEXT,
    metadata JSONB,
    bbox GEOMETRY(Polygon, 4326),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(source, tile_id)
);

CREATE INDEX IF NOT EXISTS idx_catalog_bbox
    ON dataset_catalog USING GIST(bbox);
CREATE INDEX IF NOT EXISTS idx_catalog_source
    ON dataset_catalog(source);
CREATE INDEX IF NOT EXISTS idx_catalog_quality
    ON dataset_catalog(quality_status);
CREATE INDEX IF NOT EXISTS idx_catalog_format
    ON dataset_catalog(format);

-- Analysis-tile manifest (50m x 50m tiles for JEPA training)
CREATE TABLE IF NOT EXISTS tile_manifest (
    id SERIAL PRIMARY KEY,
    source_catalog_id INTEGER REFERENCES dataset_catalog(id),
    tile_path TEXT NOT NULL,
    grid_index_x INTEGER,
    grid_index_y INTEGER,
    point_count BIGINT,
    density_pts_m2 FLOAT,
    tile_size_m FLOAT DEFAULT 50.0,
    overlap_m FLOAT DEFAULT 5.0,
    height_map_path TEXT,
    height_map_resolution_m FLOAT,
    height_map_coverage_pct FLOAT,
    bbox GEOMETRY(Polygon),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tile_manifest_bbox
    ON tile_manifest USING GIST(bbox);

-- Compliance audit log (append-only, mirrors JSONL file)
CREATE TABLE IF NOT EXISTS compliance_log (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source VARCHAR(100),
    tile_id VARCHAR(500),
    details JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_compliance_source
    ON compliance_log(source);
CREATE INDEX IF NOT EXISTS idx_compliance_event
    ON compliance_log(event_type);
