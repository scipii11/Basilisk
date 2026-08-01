# -*- coding: utf-8 -*-
"""
Test suite for SQLite and DuckDB Time Series Database Managers.

Tests cover:
- Type inference from pandas DataFrames
- Type conversion storage in type_conversions table
- Database-agnostic type handling (SQLite, DuckDB, Pandas)
- Syncing between SQLite and DuckDB with consistent types
- Data integrity during transfers
"""

import os
import sys
import tempfile
import shutil
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Add parent directory to path to import Core module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Core import SQLiteTimeSeriesManager, DuckDBSyncManager


class TestTypeInference(unittest.TestCase):
    """Test automatic type inference from pandas DataFrames."""
    
    def setUp(self):
        """Create temporary directory for test databases."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def test_datetime_inference(self):
        """Test that datetime columns are inferred correctly."""
        db_path = os.path.join(self.temp_dir, 'test_datetime.db')
        
        # Create sample data with datetime column
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, tz='UTC'),
            'value': [1.0, 2.0, 3.0, 4.0, 5.0]
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['date'],
            sample_data=df
        )
        
        # Check that datetime column has correct types
        self.assertIn('date', manager.column_types)
        self.assertEqual(manager.column_types['date']['sqlite'], 'TEXT')
        self.assertEqual(manager.column_types['date']['duckdb'], 'TIMESTAMP')
        self.assertEqual(manager.column_types['date']['pandas'], 'datetime64[ns, UTC]')
        
        manager.conn.close()
    
    def test_float_inference(self):
        """Test that float columns are inferred correctly."""
        db_path = os.path.join(self.temp_dir, 'test_float.db')
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'metric': [1.5, 2.5, 3.5]
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['id'],
            sample_data=df
        )
        
        # Check float type inference
        self.assertIn('metric', manager.column_types)
        self.assertEqual(manager.column_types['metric']['sqlite'], 'REAL')
        self.assertEqual(manager.column_types['metric']['duckdb'], 'DOUBLE')
        self.assertEqual(manager.column_types['metric']['pandas'], 'float64')
        
        manager.conn.close()
    
    def test_integer_inference(self):
        """Test that integer columns are inferred correctly."""
        db_path = os.path.join(self.temp_dir, 'test_int.db')
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'count': [10, 20, 30]
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['id'],
            sample_data=df
        )
        
        # Check integer type inference
        self.assertIn('count', manager.column_types)
        self.assertEqual(manager.column_types['count']['sqlite'], 'INTEGER')
        self.assertEqual(manager.column_types['count']['duckdb'], 'BIGINT')
        self.assertEqual(manager.column_types['count']['pandas'], 'Int64')
        
        manager.conn.close()
    
    def test_string_inference(self):
        """Test that string columns are inferred correctly."""
        db_path = os.path.join(self.temp_dir, 'test_string.db')
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'category': ['A', 'B', 'C']
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['id'],
            sample_data=df
        )
        
        # Check string type inference
        self.assertIn('category', manager.column_types)
        self.assertEqual(manager.column_types['category']['sqlite'], 'TEXT')
        self.assertEqual(manager.column_types['category']['duckdb'], 'VARCHAR')
        self.assertEqual(manager.column_types['category']['pandas'], 'string')
        
        manager.conn.close()


class TestTypeConversionsTable(unittest.TestCase):
    """Test the type_conversions table stores all three type systems."""
    
    def setUp(self):
        """Create temporary directory for test databases."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def test_type_conversions_table_populated(self):
        """Test that type_conversions table contains sqlite, duckdb, and pandas types."""
        db_path = os.path.join(self.temp_dir, 'test_tc.db')
        
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=3, tz='UTC'),
            'value': [1.1, 2.2, 3.3],
            'category': ['X', 'Y', 'Z']
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['date'],
            sample_data=df
        )
        
        # Query type_conversions table
        cur = manager.conn.cursor()
        cur.execute("SELECT semantic_type, sqlite_type, duckdb_type, pandas_type FROM type_conversions")
        rows = cur.fetchall()
        
        # Should have entries for TIMESTAMP, METRIC, CATEGORY defaults
        self.assertGreater(len(rows), 0)
        
        # Verify each row has all three type fields
        for row in rows:
            semantic_type, sqlite_type, duckdb_type, pandas_type = row
            self.assertIsNotNone(sqlite_type)
            self.assertIsNotNone(duckdb_type)
            self.assertIsNotNone(pandas_type)
        
        manager.conn.close()
    
    def test_column_specific_types_stored(self):
        """Test that column-specific derived types are stored in type_conversions."""
        db_path = os.path.join(self.temp_dir, 'test_col_types.db')
        
        df = pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=3, tz='UTC'),
            'metric_value': [10.5, 20.5, 30.5],
            'category_name': ['A', 'B', 'C']
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['ts'],
            sample_data=df
        )
        
        # Verify column_types dictionary has all three type systems
        for col_name, types in manager.column_types.items():
            self.assertIn('sqlite', types)
            self.assertIn('duckdb', types)
            self.assertIn('pandas', types)
        
        manager.conn.close()


class TestSQLiteDuckDBSync(unittest.TestCase):
    """Test syncing between SQLite and DuckDB maintains type consistency."""
    
    def setUp(self):
        """Create temporary directory for test databases."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def test_sync_preserves_types(self):
        """Test that data synced to DuckDB maintains correct types."""
        sqlite_path = os.path.join(self.temp_dir, 'test_sync_source.db')
        duckdb_path = os.path.join(self.temp_dir, 'test_sync_target.duckdb')
        
        # Create sample data
        df = pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=5, tz='UTC'),
            'metric': [1.1, 2.2, 3.3, 4.4, 5.5],
            'category': ['A', 'B', 'C', 'D', 'E'],
            'id': [100, 200, 300, 400, 500]
        })
        
        # Initialize SQLite manager
        sqlite_mgr = SQLiteTimeSeriesManager(
            db_path=sqlite_path,
            table_keys=['ts'],
            sample_data=df
        )
        
        # Upload data to SQLite
        sqlite_mgr.upload_from_pandas(df, user='test_user')
        
        # Initialize DuckDB sync manager
        duckdb_mgr = DuckDBSyncManager(
            duckdb_path=duckdb_path,
            sqlite_manager=sqlite_mgr
        )
        
        # Sync to DuckDB
        duckdb_mgr.sync()
        
        # Query data from DuckDB
        duckdb_df = duckdb_mgr.conn.execute(
            "SELECT * FROM analytics_live_data ORDER BY timestamp"
        ).fetchdf()
        
        # Verify row count matches
        self.assertEqual(len(duckdb_df), len(df))
        
        # Verify data integrity
        pd.testing.assert_frame_equal(
            df.sort_values('timestamp').reset_index(drop=True),
            duckdb_df.drop(columns=['last_updated']).sort_values('timestamp').reset_index(drop=True),
            check_dtype=False
        )
        
        sqlite_mgr.conn.close()
        duckdb_mgr.conn.close()
    
    def test_sync_with_different_column_types(self):
        """Test sync handles mixed column types correctly."""
        sqlite_path = os.path.join(self.temp_dir, 'test_mixed_source.db')
        duckdb_path = os.path.join(self.temp_dir, 'test_mixed_target.duckdb')
        
        # Create data with various types
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=3, tz='UTC'),
            'value_float': [1.5, 2.5, 3.5],
            'value_int': [10, 20, 30],
            'text_col': ['alpha', 'beta', 'gamma'],
            'bool_col': [True, False, True]
        })
        
        sqlite_mgr = SQLiteTimeSeriesManager(
            db_path=sqlite_path,
            table_keys=['date'],
            sample_data=df
        )
        
        sqlite_mgr.upload_from_pandas(df, user='test_user')
        
        duckdb_mgr = DuckDBSyncManager(
            duckdb_path=duckdb_path,
            sqlite_manager=sqlite_mgr
        )
        
        duckdb_mgr.sync()
        
        # Verify DuckDB table was created with correct columns
        result = duckdb_mgr.conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'analytics_live_data'
            ORDER BY ordinal_position
        """).fetchall()
        
        column_names = [row[0] for row in result]
        self.assertIn('date', column_names)
        self.assertIn('value_float', column_names)
        self.assertIn('value_int', column_names)
        self.assertIn('text_col', column_names)
        self.assertIn('bool_col', column_names)
        
        sqlite_mgr.conn.close()
        duckdb_mgr.conn.close()


class TestPandasRoundTrip(unittest.TestCase):
    """Test pandas DataFrame round-trip through databases."""
    
    def setUp(self):
        """Create temporary directory for test databases."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def test_sqlite_pandas_roundtrip(self):
        """Test uploading and downloading DataFrame from SQLite preserves data."""
        db_path = os.path.join(self.temp_dir, 'test_roundtrip.db')
        
        original_df = pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=5, tz='UTC'),
            'metric': [1.1, 2.2, 3.3, 4.4, 5.5],
            'category': ['A', 'B', 'C', 'D', 'E']
        })
        
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['ts'],
            sample_data=original_df
        )
        
        # Upload to SQLite
        manager.upload_from_pandas(original_df, user='test')
        
        # Download from SQLite
        downloaded_df = manager.download_to_pandas()
        
        # Verify data integrity (excluding metadata columns)
        self.assertEqual(len(downloaded_df), len(original_df))
        
        manager.conn.close()
    
    def test_cross_database_pandas_compatibility(self):
        """Test pandas compatibility when moving between SQLite and DuckDB."""
        sqlite_path = os.path.join(self.temp_dir, 'test_cross_source.db')
        duckdb_path = os.path.join(self.temp_dir, 'test_cross_target.duckdb')
        
        original_df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=4, tz='UTC'),
            'value': [10.0, 20.0, 30.0, 40.0],
            'label': ['w', 'x', 'y', 'z']
        })
        
        # Setup SQLite
        sqlite_mgr = SQLiteTimeSeriesManager(
            db_path=sqlite_path,
            table_keys=['date'],
            sample_data=original_df
        )
        sqlite_mgr.upload_from_pandas(original_df, user='test')
        
        # Setup DuckDB sync
        duckdb_mgr = DuckDBSyncManager(
            duckdb_path=duckdb_path,
            sqlite_manager=sqlite_mgr
        )
        duckdb_mgr.sync()
        
        # Read from DuckDB as pandas
        duckdb_df = duckdb_mgr.conn.execute(
            "SELECT * FROM analytics_live_data"
        ).fetchdf()
        
        # Verify data can be used as pandas DataFrame
        self.assertIsInstance(duckdb_df, pd.DataFrame)
        self.assertEqual(len(duckdb_df), 4)
        
        sqlite_mgr.conn.close()
        duckdb_mgr.conn.close()


class TestCustomTypeOverrides(unittest.TestCase):
    """Test custom type overrides via variables parameter."""
    
    def setUp(self):
        """Create temporary directory for test databases."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def test_custom_override_applied(self):
        """Test that custom type overrides are applied correctly."""
        db_path = os.path.join(self.temp_dir, 'test_override.db')
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [1, 2, 3]  # Would normally infer as INTEGER
        })
        
        # Override value column to be METRIC (REAL/DOUBLE)
        manager = SQLiteTimeSeriesManager(
            db_path=db_path,
            table_keys=['id'],
            sample_data=df,
            variables={'value': 'METRIC'}
        )
        
        # Check override was applied
        self.assertIn('value', manager.column_types)
        self.assertEqual(manager.column_types['value']['sqlite'], 'REAL')
        self.assertEqual(manager.column_types['value']['duckdb'], 'DOUBLE')
        self.assertEqual(manager.column_types['value']['pandas'], 'float64')
        
        manager.conn.close()


if __name__ == '__main__':
    unittest.main()
