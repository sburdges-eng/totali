-- Index patch envelopes for spatial filters and quick quality inspection.
-- Keep this script adjacent to the COPC/PostGIS PDAL pipeline template for reproducible loading.

CREATE INDEX IF NOT EXISTS {{schema_name}}.{{table_name}}_envelope_gist
ON {{schema_name}}.{{table_name}} USING GIST (PC_EnvelopeGeometry(patch));

-- Quick health checks after load.
SELECT
  COUNT(*) AS patches,
  AVG(PC_NumPoints(patch)) AS avg_points_per_patch,
  MIN(PC_NumPoints(patch)) AS min_points_per_patch,
  MAX(PC_NumPoints(patch)) AS max_points_per_patch
FROM {{schema_name}}.{{table_name}};
