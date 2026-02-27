import unittest
from unittest.mock import MagicMock
import sys
import os
from pathlib import Path

# Add the source directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../groundtruthos-data/pipeline')))

# Create mocks for external dependencies
mock_laspy = MagicMock()
mock_numpy = MagicMock()
mock_psycopg2 = MagicMock()
mock_psycopg2_extras = MagicMock()
mock_pyarrow = MagicMock()
mock_pyarrow_parquet = MagicMock()
mock_duckdb = MagicMock()

# Set up sys.modules BEFORE importing features
sys.modules['laspy'] = mock_laspy
sys.modules['numpy'] = mock_numpy
sys.modules['psycopg2'] = mock_psycopg2
sys.modules['psycopg2.extras'] = mock_psycopg2_extras

# Ensure that 'import pyarrow as pa' gets our mock
sys.modules['pyarrow'] = mock_pyarrow

# Ensure that 'import pyarrow.parquet as pq' gets our mock
# IMPORTANT: When  runs, it usually sets .
# If  does , it looks up 'pyarrow.parquet' in sys.modules.
sys.modules['pyarrow.parquet'] = mock_pyarrow_parquet

# Also, ensure accessing pyarrow.parquet works if it happens via the pyarrow module
mock_pyarrow.parquet = mock_pyarrow_parquet

sys.modules['duckdb'] = mock_duckdb

# Now import the module to test
import features

class TestTelemetryFeatures(unittest.TestCase):

    def test_compute_telemetry_features(self):
        # Mock psycopg2 connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock fetchone return value for reading telemetry_zones
        mock_cursor.fetchone.return_value = (0.5, True, {'avg_blade_pressure': 10.0, 'avg_speed_m_s': 5.0, 'max_pass_count': 3})

        zone_id = 'zone-123'
        feature_version = 'v1'
        conn_string = 'postgres://user:pass@localhost/db'

        result = features.compute_telemetry_features(zone_id, feature_version, conn_string)

        expected_result = {
            "zone_id": zone_id,
            "feature_version": feature_version,
            "soil_resistance": 0.5,
            "rework_flag": True,
            "blade_pressure": 10.0,
            "speed_m_s": 5.0,
            "pass_count": 3,
        }
        self.assertEqual(result, expected_result)

        mock_cursor.execute.assert_any_call(
            "SELECT soil_resistance, rework_flag, metadata FROM telemetry_zones WHERE id = %s",
            (zone_id,)
        )

        insert_call_args = mock_cursor.execute.call_args_list[1]
        sql_query = insert_call_args[0][0]
        self.assertIn("INSERT INTO telemetry_features", sql_query)
        self.assertIn("ON CONFLICT (zone_id, feature_version) DO UPDATE SET", sql_query)

    def test_export_telemetry_features_parquet(self):
        # Reset mocks
        mock_psycopg2.connect.reset_mock()
        mock_pyarrow.table.reset_mock()
        mock_pyarrow_parquet.write_table.reset_mock()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock fetchall return value
        mock_cursor.fetchall.return_value = [
            ('zone-1', 'v1', 0.5, False, 10.0, 5.0, 3),
            ('zone-2', 'v1', 0.6, True, 12.0, 4.0, 4)
        ]
        mock_cursor.description = [
            ('zone_id',), ('feature_version',), ('soil_resistance',), ('rework_flag',),
            ('blade_pressure',), ('speed_m_s',), ('pass_count',)
        ]

        zone_ids = ['zone-1', 'zone-2']
        feature_version = 'v1'
        output_path = Path('output.parquet')
        conn_string = 'postgres://fake'

        features.export_telemetry_features_parquet(zone_ids, feature_version, output_path, conn_string)

        mock_cursor.execute.assert_called()
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("SELECT zone_id, feature_version", sql)
        self.assertIn("FROM telemetry_features", sql)

        # In features.py: import pyarrow.parquet as pq; pq.write_table(...)
        # We mocked sys.modules['pyarrow.parquet'].
        # Let's verify that mock received the call.
        mock_pyarrow_parquet.write_table.assert_called()

        # Also check pyarrow.table
        mock_pyarrow.table.assert_called()

    def test_snapshot_telemetry_features(self):
        # Reset mocks
        mock_psycopg2.connect.reset_mock()
        mock_duckdb.connect.reset_mock()

        mock_pg_conn = MagicMock()
        mock_pg_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_pg_conn
        mock_pg_conn.cursor.return_value.__enter__.return_value = mock_pg_cursor

        mock_duck_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_duck_conn

        mock_pg_cursor.fetchall.return_value = [
            ('zone-1', 'v1', 0.5, False, 10.0, 5.0, 3)
        ]
        mock_pg_cursor.description = [
            ('zone_id',), ('feature_version',), ('soil_resistance',), ('rework_flag',),
            ('blade_pressure',), ('speed_m_s',), ('pass_count',)
        ]

        feature_version = 'v1'
        duckdb_path = Path('snapshot.duckdb')
        conn_string = 'postgres://fake'

        features.snapshot_telemetry_features(feature_version, duckdb_path, conn_string)

        mock_pg_cursor.execute.assert_called()
        sql = mock_pg_cursor.execute.call_args[0][0]
        self.assertIn("SELECT zone_id::text, feature_version", sql)

        mock_duck_conn.execute.assert_called()
        create_sql = mock_duck_conn.execute.call_args[0][0]
        self.assertIn("CREATE OR REPLACE TABLE telemetry_features", create_sql)

        mock_duck_conn.executemany.assert_called()
        insert_sql = mock_duck_conn.executemany.call_args[0][0]
        self.assertIn("INSERT INTO telemetry_features VALUES", insert_sql)

if __name__ == '__main__':
    unittest.main()
