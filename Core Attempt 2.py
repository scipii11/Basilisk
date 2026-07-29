# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 18:28:06 2026

@author: farquharsona

SQLite Time Series Database Manager - Version 2
================================================
This module provides a dual-database architecture for time series data management:
- Class 1 (SQLiteTimeSeriesManager): System of record with full audit trail
- Class 2 (DuckDBSyncManager): High-performance analytical layer synced from SQLite

Key Features:
- Atomic transactions with rollback support
- Complete audit logging (who, what, when, old vs new values)
- Historical data versioning
- Automatic type conversion between SQL and Pandas
- Incremental sync to DuckDB for analytics
"""

import sqlite3
import duckdb
import pandas as pd
import numpy as np
import json
import datetime
import logging
import os
from typing import List, Dict, Optional, Any

# Configure logging for monitoring and debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SQLiteTimeSeriesManager:
    """
    Class 1: Manages the SQLite3 System of Record.
    
    This class handles the primary database operations including:
    - Atomic transactions with BEGIN IMMEDIATE for write locking
    - Comprehensive audit logging in tx_log table
    - Historical data versioning in historical_data table
    - Live/current data storage in live_data table
    - Dynamic type derivation from input data
    - Seamless Pandas DataFrame integration for uploads/downloads
    
    Database Schema:
    - type_conversions: Stores derived SQL/Pandas types per column (populated from input data)
    - tx_log: Audit trail of all data changes
    - historical_data: Full version history of all records
    - live_data: Current state of all records (optimized for reads)
    """
    
    # Default mapping of semantic types to their SQL and Pandas equivalents
    # Used as fallback if type cannot be derived from input data
    TYPE_MAP = {
        'TIMESTAMP': {'sqlite': 'TEXT', 'pandas': 'datetime64[ns, UTC]'},
        'METRIC':    {'sqlite': 'REAL', 'pandas': 'float64'},
        'CATEGORY':  {'sqlite': 'TEXT', 'pandas': 'string'},
        'ID':        {'sqlite': 'INTEGER', 'pandas': 'Int64'}
    }

    def __init__(self, db_path: str, table_keys: List[str], variables: Dict[str, str], default_user: str = "system"):
        """
        Initialize the SQLite Time Series Manager.
        
        Args:
            db_path: Path to the SQLite database file
            table_keys: List of column names that form the primary key (e.g., ['Date', 'Time Series'])
            variables: Dictionary mapping column names to semantic types (e.g., {'Value': 'METRIC'})
            default_user: Default username for audit logging when not specified
        """
        self.db_path = db_path
        self.table_keys = table_keys  # Primary key columns for identifying unique records
        self.variables = variables     # Data columns with their semantic types
        self.default_user = default_user
        self.column_types = {}         # Derived SQL/Pandas types per column from input data
        
        # Ensure directory exists for database file
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        
        # Connect to SQLite with timeout for concurrent access handling
        self.conn = sqlite3.connect(self.db_path, timeout=10)
        # Enable WAL mode for better concurrency (readers don't block writers)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        # Use NORMAL synchronous mode for balance between safety and performance
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create database tables if they don't exist
        self._initialize_schema()

    def _quote_id(self, name: str) -> str:
        """
        Safely quote SQL identifiers to prevent SQL injection and handle special characters.
        Doubles any existing double quotes per SQLite escaping rules.
        
        Args:
            name: The column or table name to quote
            
        Returns:
            Properly quoted identifier wrapped in double quotes
        """
        return f'"{name.replace("\"", "\"\"")}"'

    def _initialize_schema(self):
        """
        Create all required database tables if they don't exist.
        
        Tables created:
        - type_conversions: Reference table for semantic type mappings (populated dynamically from input)
        - tx_log: Audit trail recording every transaction with before/after values
        - historical_data: Complete version history linked to transactions
        - live_data: Current state of all records (optimized for fast reads)
        """
        cur = self.conn.cursor()
        
        # Create type conversion reference table for consistent type handling
        # Stores both semantic type mappings and column-specific derived types
        cur.execute("""
            CREATE TABLE IF NOT EXISTS type_conversions (
                semantic_type TEXT PRIMARY KEY, sqlite_type TEXT, python_type TEXT
            )
        """)
        # Populate default type conversions from TYPE_MAP
        for sem_type, mapping in self.TYPE_MAP.items():
            cur.execute("INSERT OR IGNORE INTO type_conversions VALUES (?, ?, ?)", 
                        (sem_type, mapping['sqlite'], mapping['pandas']))

        # Create transaction log table for audit trail
        # Records: who made the change, when, what operation, which keys, old/new values, status
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tx_log (
                tx_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, user TEXT,
                operation TEXT, table_keys_json TEXT, old_values_json TEXT, 
                new_values_json TEXT, status TEXT
            )
        """)

        # Build column definitions for historical data table based on table_keys and variables
        # Uses derived column types if available, otherwise falls back to semantic type defaults
        hist_cols = []
        for k in self.table_keys:
            col_type = self.column_types.get(k, {}).get('sqlite', self.TYPE_MAP[self.variables.get(k, 'CATEGORY')]['sqlite'])
            hist_cols.append(f"{self._quote_id(k)} {col_type}")
        for v, t in self.variables.items():
            col_type = self.column_types.get(v, {}).get('sqlite', self.TYPE_MAP[t]['sqlite'])
            hist_cols.append(f"{self._quote_id(v)} {col_type}")
        hist_cols_str = ", ".join(hist_cols)
        
        # Create historical data table with full version history
        # Each row represents a snapshot at a point in time, linked to a transaction
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS historical_data (
                version_id INTEGER PRIMARY KEY AUTOINCREMENT, tx_id INTEGER, timestamp TEXT,
                {hist_cols_str}, FOREIGN KEY(tx_id) REFERENCES tx_log(tx_id)
            )
        """)

        # Build column definitions for live data table (current state)
        # Uses derived column types if available, otherwise falls back to semantic type defaults
        live_cols = []
        for k in self.table_keys:
            col_type = self.column_types.get(k, {}).get('sqlite', self.TYPE_MAP[self.variables.get(k, 'CATEGORY')]['sqlite'])
            live_cols.append(f"{self._quote_id(k)} {col_type}")
        for v, t in self.variables.items():
            col_type = self.column_types.get(v, {}).get('sqlite', self.TYPE_MAP[t]['sqlite'])
            live_cols.append(f"{self._quote_id(v)} {col_type}")
        live_cols_str = ", ".join(live_cols)
        pk_str = ", ".join([self._quote_id(k) for k in self.table_keys])
        
        # Create live data table for current state (fast reads, upsert support)
        # Uses composite primary key for uniqueness enforcement
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS live_data (
                {live_cols_str}, last_updated TEXT, PRIMARY KEY ({pk_str})
            )
        """)
        self.conn.commit()

    def _coerce_df_to_sql(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Pandas DataFrame to SQL-compatible format.
        
        Handles:
        - Timezone-aware datetime conversion to UTC ISO format strings
        - NaN/None value handling for SQL compatibility
        - Derives column types from input data and stores in type_conversions table
        
        Args:
            df: Input Pandas DataFrame
            
        Returns:
            DataFrame with SQL-compatible types (datetime as ISO strings, NaN as None)
            
        Note:
            This method dynamically derives SQL and Pandas types for each column based on
            the actual data types in the input DataFrame, storing them in self.column_types
            and the type_conversions table for consistent type handling across SQLite and DuckDB.
        """
        df = df.copy()
        
        # Derive column types from input data on first upload or when new columns appear
        for col in df.columns:
            if col not in self.column_types:
                dtype = df[col].dtype
                
                # Determine semantic type based on pandas dtype
                if pd.api.types.is_datetime64_any_dtype(dtype):
                    sqlite_type = 'TEXT'
                    pandas_type = 'datetime64[ns, UTC]'
                elif pd.api.types.is_float_dtype(dtype):
                    sqlite_type = 'REAL'
                    pandas_type = 'float64'
                elif pd.api.types.is_integer_dtype(dtype):
                    sqlite_type = 'INTEGER'
                    pandas_type = 'Int64'
                else:
                    sqlite_type = 'TEXT'
                    pandas_type = 'string'
                
                # Store derived types for this column
                self.column_types[col] = {'sqlite': sqlite_type, 'pandas': pandas_type}
                
                # Update type_conversions table with derived column-specific types
                # Use column name as semantic_type key for column-specific mappings
                cur = self.conn.cursor()
                cur.execute("INSERT OR REPLACE INTO type_conversions VALUES (?, ?, ?)",
                           (col, sqlite_type, pandas_type))
                self.conn.commit()
        
        for col in df.columns:
            # Convert any datetime type to UTC ISO format string for SQL storage
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if getattr(df[col].dt, 'tz', None) is not None:
                    df[col] = df[col].dt.tz_convert('UTC')
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # Replace NaN/NaT values with None for SQL NULL representation
        df = df.where(pd.notnull(df), None)
        return df

    def _coerce_sql_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert SQL query results back to proper Pandas DataFrame types.
        
        Handles:
        - Uses derived column types from type_conversions table for accurate type restoration
        - Falls back to semantic type mappings if column-specific types not found
        - TIMESTAMP columns converted to timezone-aware datetime64[ns, UTC]
        - METRIC columns converted to numeric float64
        - Table key columns attempted as datetime if they're strings
        
        Args:
            df: DataFrame from SQL query result
            
        Returns:
            DataFrame with proper Pandas types based on derived or semantic type mappings
        """
        df = df.copy()
        
        # First, try to use derived column-specific types from type_conversions table
        for col in df.columns:
            if col in self.column_types:
                pandas_type = self.column_types[col]['pandas']
                if pandas_type == 'datetime64[ns, UTC]':
                    df[col] = pd.to_datetime(df[col], utc=True)
                elif pandas_type == 'float64':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif pandas_type == 'Int64':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                # 'string' type doesn't need conversion as it's already the default
        
        # Fall back to semantic type mappings for columns without derived types
        for col, sem_type in self.variables.items():
            if col not in df.columns:
                continue
            if sem_type == 'TIMESTAMP':
                df[col] = pd.to_datetime(df[col], utc=True)
            elif sem_type == 'METRIC':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Try to parse table key columns as dates (e.g., 'Date' key might be a date string)
        for col in self.table_keys:
            if col in df.columns and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], utc=True)
                except (ValueError, TypeError):
                    pass  # Not a date, leave as-is
        return df

    def upload_from_pandas(self, df: pd.DataFrame, user: Optional[str] = None, chunk_size: int = 50000):
        """
        Upload data from a Pandas DataFrame to SQLite with automatic APPEND/EDIT detection.
        
        Processes data in chunks for memory efficiency with large datasets.
        Each chunk is processed atomically - either all rows succeed or all rollback.
        
        Args:
            df: Pandas DataFrame containing data to upload
            user: Username for audit logging (defaults to self.default_user)
            chunk_size: Number of rows to process per transaction (default 50,000)
            
        Raises:
            ValueError: If DataFrame is missing required columns (table_keys + variables)
        """
        user = user or self.default_user
        df = self._coerce_df_to_sql(df)
        
        # Validate that all required columns are present in the DataFrame
        required_cols = self.table_keys + list(self.variables.keys())
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in DataFrame: {missing}")

        # Process data in chunks to handle large datasets efficiently
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            self._process_chunk_atomic(chunk, user)

    def _process_chunk_atomic(self, chunk: pd.DataFrame, user: str):
        """
        Process a single chunk of data atomically within a transaction.
        
        For each row in the chunk:
        1. Check if record exists in live_data
        2. If exists and data changed: log as EDIT, update historical_data and live_data
        3. If exists and data unchanged: skip (optimization)
        4. If doesn't exist: log as APPEND, insert into historical_data and live_data
        
        All operations are wrapped in BEGIN IMMEDIATE transaction for write locking.
        On any error, the entire chunk is rolled back.
        
        Args:
            chunk: DataFrame subset to process
            user: Username for audit logging
        """
        cur = self.conn.cursor()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        try:
            # BEGIN IMMEDIATE acquires write lock immediately, preventing concurrent writes
            cur.execute("BEGIN IMMEDIATE;")
            
            # Get unique keys in this chunk to query existing records
            keys_in_chunk = chunk[self.table_keys].drop_duplicates()
            placeholders = ",".join(["?"] * len(self.table_keys))
            in_clause = ",".join([f"({placeholders})"] * len(keys_in_chunk))
            flat_keys = [val for row in keys_in_chunk.values for val in row]
            
            quoted_keys = [self._quote_id(k) for k in self.table_keys]
            query = f"SELECT * FROM live_data WHERE ({','.join(quoted_keys)}) IN ({in_clause})"
            existing_df = pd.read_sql_query(query, self.conn, params=flat_keys)
            
            cols = self.table_keys + list(self.variables.keys())
            insert_cols = cols + ['last_updated']
            
            tx_records = []      # Audit log entries
            hist_records = []    # Historical data snapshots
            live_records = []    # Live data upserts
            
            # Process each row in the chunk to determine operation type and prepare records
            for idx, row in chunk.iterrows():
                # Normalize numpy/Pandas scalar types to native Python types
                # This prevents JSON serialization errors and ensures accurate value comparison
                key_vals = {k: (row[k].item() if hasattr(row[k], 'item') else row[k]) for k in self.table_keys}
                var_vals = {v: (row[v].item() if hasattr(row[v], 'item') else row[v]) for v in self.variables.keys()}
                
                # Find matching existing record by comparing primary key values
                existing_row = existing_df[
                    (existing_df[self.table_keys] == pd.Series(key_vals)).all(axis=1)
                ]
                
                if not existing_row.empty:
                    # Record exists - extract old values for audit trail
                    old_vals = existing_row.iloc[0][list(self.variables.keys())].to_dict()
                    # Normalize old_vals to native Python types as well
                    old_vals = {k: (v.item() if hasattr(v, 'item') else v) for k, v in old_vals.items()}
                    
                    # OPTIMIZATION: Skip database write if data hasn't actually changed
                    # This reduces unnecessary transactions and storage growth
                    if old_vals == var_vals:
                        continue
                    
                    op = 'EDIT'  # Existing record with changed values
                else:
                    op = 'APPEND'  # New record that doesn't exist yet
                    old_vals = None
                    
                # Prepare audit log entry with full before/after information
                tx_records.append((
                    now, user, op, 
                    json.dumps(key_vals),        # Which record was affected
                    json.dumps(old_vals) if old_vals else None,  # Previous values (None for APPEND)
                    json.dumps(var_vals),        # New values
                    'SUCCESS'                    # Transaction status
                ))
                
                # Prepare historical snapshot record (version_id and tx_id set later)
                hist_records.append(tuple([None, None, now] + [row[c] for c in cols]))
                # Prepare live data upsert record
                live_records.append(tuple([row[c] for c in cols] + [now]))

            # If all rows were duplicates (no actual changes), commit empty transaction and exit early
            if not tx_records:
                self.conn.commit()
                logging.info(f"Chunk processed, but all rows were identical to existing data. Skipped updates.")
                return

            # Insert audit log entries - these generate auto-incrementing tx_ids
            cur.executemany("""
                INSERT INTO tx_log (timestamp, user, operation, table_keys_json, old_values_json, new_values_json, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, tx_records)
            
            # Calculate the first tx_id assigned to this batch for linking historical records
            first_tx_id = cur.execute("SELECT last_insert_rowid()").fetchone()[0] - len(tx_records) + 1
            
            # Link each historical record to its corresponding transaction ID
            for i, rec in enumerate(hist_records):
                hist_records[i] = (rec[0], first_tx_id + i, *rec[2:])
                
            quoted_cols = [self._quote_id(c) for c in cols]
            insert_cols_quoted = [self._quote_id(c) for c in insert_cols]
            
            # Insert historical snapshots with version tracking
            cur.executemany(f"""
                INSERT INTO historical_data (version_id, tx_id, timestamp, {','.join(quoted_cols)})
                VALUES (?, ?, ?, {','.join(['?']*len(cols))})
            """, hist_records)
            
            # Build UPSERT clause: insert new or update existing on conflict
            placeholders = ','.join(['?'] * len(insert_cols))
            update_clause = ','.join([f"{self._quote_id(c)}=excluded.{self._quote_id(c)}" for c in self.variables.keys()]) + ", last_updated=excluded.last_updated"
            
            # Upsert into live_data: INSERT ... ON CONFLICT DO UPDATE (SQLite upsert pattern)
            cur.executemany(f"""
                INSERT INTO live_data ({','.join(insert_cols_quoted)}) 
                VALUES ({placeholders})
                ON CONFLICT({','.join(quoted_keys)}) 
                DO UPDATE SET {update_clause}
            """, live_records)
            
            # Commit the entire transaction - all inserts/updates are now permanent
            self.conn.commit()
            logging.info(f"Successfully committed chunk of {len(chunk)} rows ({len(tx_records)} actual changes).")
            
        except Exception as e:
            # Rollback entire transaction on any error - ensures atomicity
            self.conn.rollback()
            logging.error(f"Transaction failed and rolled back: {e}")
            raise

    def download_to_pandas(self, table: str = 'live_data', filters: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Download data from a SQLite table to a Pandas DataFrame with optional filtering.
        
        Args:
            table: Table name to query ('live_data', 'historical_data', or 'tx_log')
            filters: Optional dictionary of column=value conditions for WHERE clause
                     Supports single values and lists (for IN clauses)
                     Datetime values are automatically converted to UTC ISO format
                     
        Returns:
            Pandas DataFrame with proper type coercion based on semantic types
            
        Example:
            df = mgr.download_to_pandas('live_data', {'Date': '2024-01-01', 'Organisation': ['RBNZ', 'TREASURY']})
        """
        query = f"SELECT * FROM {self._quote_id(table)}"
        params = []
        
        if filters:
            conditions = []
            for k, v in filters.items():
                # Convert datetime objects to SQL-compatible UTC ISO format strings
                if isinstance(v, (pd.Timestamp, datetime.datetime)):
                    v = pd.to_datetime(v, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                elif isinstance(v, list):
                    v = [pd.to_datetime(x, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%fZ') if isinstance(x, (pd.Timestamp, datetime.datetime)) else x for x in v]

                quoted_k = self._quote_id(k)
                if isinstance(v, list):
                    # Use IN clause for list values
                    placeholders = ','.join(['?'] * len(v))
                    conditions.append(f"{quoted_k} IN ({placeholders})")
                    params.extend(v)
                else:
                    # Use equality for single values
                    conditions.append(f"{quoted_k} = ?")
                    params.append(v)
            query += " WHERE " + " AND ".join(conditions)
            
        df = pd.read_sql_query(query, self.conn, params=params)
        return self._coerce_sql_to_df(df)

    def get_table_names(self) -> List[str]:
        """
        Get list of all user tables in the database (excluding SQLite system tables).
        
        Returns:
            List of table names
        """
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        tables = self.conn.execute(query).fetchall()
        return [table[0] for table in tables]

    def get_table(self, table_name: str) -> pd.DataFrame:
        """
        Get a specific table as a Pandas DataFrame with validation.
        
        Args:
            table_name: Name of the table to retrieve
            
        Returns:
            Pandas DataFrame containing the table data
            
        Raises:
            ValueError: If table_name doesn't exist in the database
        """
        available_tables = self.get_table_names()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' does not exist. Available tables: {available_tables}")
        
        df = pd.read_sql_query(f"SELECT * FROM {self._quote_id(table_name)}", self.conn)
        # Apply type coercion for live_data to restore proper Pandas types
        if table_name == 'live_data':
            df = self._coerce_sql_to_df(df)
        return df

    def close(self):
        """Close the SQLite database connection."""
        self.conn.close()


class DuckDBSyncManager:
    """
    Class 2: Manages the DuckDB Analytical Layer.
    
    This class provides a high-performance analytical interface synced from SQLite:
    - Incremental sync from SQLite tx_log (only new transactions are processed)
    - Optimized for fast analytical queries on large datasets
    - Maintains analytics_live_data table mirroring SQLite live_data
    - Tracks sync progress via _sync_metadata table
    
    Architecture:
    - SQLite: System of record with full audit trail (write-optimized)
    - DuckDB: Analytical layer for fast reads (query-optimized)
    """
    
    # Mapping of semantic types to DuckDB column types
    DUCKDB_TYPE_MAP = {
        'TIMESTAMP': 'TIMESTAMP',
        'METRIC': 'DOUBLE',
        'CATEGORY': 'VARCHAR',
        'ID': 'BIGINT'
    }

    def __init__(self, duckdb_path: str, sqlite_manager: SQLiteTimeSeriesManager):
        """
        Initialize the DuckDB Sync Manager.
        
        Args:
            duckdb_path: Path to the DuckDB database file
            sqlite_manager: Instance of SQLiteTimeSeriesManager to sync from
        """
        self.duckdb_path = duckdb_path
        self.sqlite_mgr = sqlite_manager
        
        # Ensure directory exists for DuckDB file
        # Connect to DuckDB database (creates file if doesn't exist)
        self.conn = duckdb.connect(self.duckdb_path)
        
        # Create metadata table to track sync progress
        # Stores the last synced transaction ID from SQLite for incremental sync
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _sync_metadata (key VARCHAR PRIMARY KEY, value BIGINT)
        """)
        self.conn.execute("INSERT OR IGNORE INTO _sync_metadata VALUES ('last_synced_tx_id', 0)")
        
        # Create analytics tables based on SQLite schema
        self._initialize_schema()

    def _quote_id(self, name: str) -> str:
        """
        Safely quote SQL identifiers for DuckDB (same logic as SQLite).
        
        Args:
            name: The column or table name to quote
            
        Returns:
            Properly quoted identifier wrapped in double quotes
        """
        return f'"{name.replace("\"", "\"\"")}"'

    def _initialize_schema(self):
        """
        Create DuckDB analytics table with columns matching SQLite schema.
        
        Creates analytics_live_data table with:
        - Same primary key columns as SQLite live_data
        - Same variable columns with DuckDB-appropriate types derived from type_conversions table
        - last_updated timestamp column
        
        Type mapping process:
        1. First checks SQLite's type_conversions table for column-specific derived types
        2. Falls back to semantic type mappings if column-specific types not found
        3. Maps SQLite types to equivalent DuckDB types for accurate type preservation
        """
        # Build column definitions using DuckDB type mappings
        # First, try to get derived types from SQLite's type_conversions table
        cur = self.sqlite_mgr.conn.cursor()
        cur.execute("SELECT semantic_type, sqlite_type, python_type FROM type_conversions")
        type_rows = cur.fetchall()
        derived_types = {row[0]: {'sqlite': row[1], 'pandas': row[2]} for row in type_rows}
        
        cols = []
        all_columns = self.sqlite_mgr.table_keys + list(self.sqlite_mgr.variables.keys())
        
        for col_name in all_columns:
            # Determine the appropriate DuckDB type for this column
            if col_name in derived_types:
                # Use derived column-specific type from type_conversions table
                sqlite_type = derived_types[col_name]['sqlite']
            else:
                # Fall back to semantic type mapping
                sem_type = self.sqlite_mgr.variables.get(col_name, 'CATEGORY')
                sqlite_type = self.sqlite_mgr.TYPE_MAP.get(sem_type, self.sqlite_mgr.TYPE_MAP['CATEGORY'])['sqlite']
            
            # Map SQLite type to DuckDB type
            duckdb_type = self._map_sqlite_to_duckdb(sqlite_type, col_name)
            cols.append(f"{self._quote_id(col_name)} {duckdb_type}")
        
        cols_str = ", ".join(cols)
        pk_str = ", ".join([self._quote_id(k) for k in self.sqlite_mgr.table_keys])
        
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS analytics_live_data (
                {cols_str},
                last_updated TIMESTAMP,
                PRIMARY KEY ({pk_str})
            )
        """)

    def _map_sqlite_to_duckdb(self, sqlite_type: str, col_name: str) -> str:
        """
        Map SQLite column type to equivalent DuckDB type.
        
        Args:
            sqlite_type: The SQLite column type (e.g., 'TEXT', 'REAL', 'INTEGER')
            col_name: Column name for context (used in error messages)
            
        Returns:
            Equivalent DuckDB type string
            
        Note:
            This ensures type consistency when syncing data between SQLite and DuckDB.
        """
        # Map common SQLite types to DuckDB equivalents
        type_mapping = {
            'TEXT': 'VARCHAR',
            'REAL': 'DOUBLE',
            'INTEGER': 'BIGINT',
            'NUMERIC': 'DECIMAL'
        }
        return type_mapping.get(sqlite_type, 'VARCHAR')  # Default to VARCHAR for unknown types

    def sync(self):
        """
        Incrementally sync new transactions from SQLite to DuckDB.
        
        This method:
        1. Queries SQLite tx_log for transactions since last sync point
        2. Processes DELETE operations by removing records from DuckDB
        3. Processes APPEND/EDIT operations by upserting latest data from SQLite
        4. Updates the sync metadata with the new max transaction ID
        
        Only processes transactions with status='SUCCESS' to ensure data consistency.
        Efficient for large datasets as it only transfers changed data.
        """
        # Get the last transaction ID that was synced to DuckDB
        last_synced = self.conn.execute("SELECT value FROM _sync_metadata WHERE key='last_synced_tx_id'").fetchone()[0]
        
        # Query SQLite for all successful transactions since last sync
        new_txs = pd.read_sql_query(
            "SELECT tx_id, operation, table_keys_json FROM tx_log WHERE tx_id > ? AND status='SUCCESS'", 
            self.sqlite_mgr.conn, 
            params=[last_synced]
        )
        
        # No new transactions to sync - exit early
        if new_txs.empty:
            return

        logging.info(f"Syncing {len(new_txs)} new transactions to DuckDB...")
        
        # Separate transactions into upserts (APPEND/EDIT) and DELETEs
        upsert_keys_json = new_txs[new_txs['operation'] != 'DELETE']['table_keys_json'].unique()
        delete_keys_json = new_txs[new_txs['operation'] == 'DELETE']['table_keys_json'].unique()
        
        # Process DELETE operations first
        if len(delete_keys_json) > 0:
            delete_keys = [json.loads(k) for k in delete_keys_json]
            df_del = pd.DataFrame(delete_keys)
            
            # Delete matching records from DuckDB analytics table
            self.conn.execute("DELETE FROM analytics_live_data USING df_del WHERE " + 
                              " AND ".join([f"analytics_live_data.{self._quote_id(k)} = df_del.{self._quote_id(k)}" for k in self.sqlite_mgr.table_keys]))

        # Process UPSERT operations (APPEND and EDIT)
        if len(upsert_keys_json) > 0:
            upsert_keys = [json.loads(k) for k in upsert_keys_json]
            df_keys = pd.DataFrame(upsert_keys)
            
            # Build query to fetch current state of affected records from SQLite live_data
            placeholders = ",".join(["?"] * len(self.sqlite_mgr.table_keys))
            in_clause = ",".join([f"({placeholders})"] * len(df_keys))
            flat_keys = [val for row in df_keys.values for val in row]
            
            quoted_keys = [self._quote_id(k) for k in self.sqlite_mgr.table_keys]
            query = f"SELECT * FROM live_data WHERE ({','.join(quoted_keys)}) IN ({in_clause})"
            df_live = pd.read_sql_query(query, self.sqlite_mgr.conn, params=flat_keys)
            
            if not df_live.empty:
                # Apply type conversions based on derived column types from type_conversions table
                # This ensures data pulled from SQLite is correctly typed before syncing to DuckDB
                for col in df_live.columns:
                    if col in self.sqlite_mgr.column_types:
                        pandas_type = self.sqlite_mgr.column_types[col]['pandas']
                        if pandas_type == 'datetime64[ns, UTC]':
                            df_live[col] = pd.to_datetime(df_live[col], utc=True)
                        elif pandas_type == 'float64':
                            df_live[col] = pd.to_numeric(df_live[col], errors='coerce')
                        elif pandas_type == 'Int64':
                            df_live[col] = pd.to_numeric(df_live[col], errors='coerce').astype('Int64')
                
                # Convert last_updated to datetime for DuckDB compatibility
                df_live['last_updated'] = pd.to_datetime(df_live['last_updated'])
                
                # Upsert data into DuckDB analytics table using INSERT OR REPLACE
                # This handles both new records (INSERT) and updated records (REPLACE)
                # Types are preserved through the explicit type conversion above
                self.conn.execute(f"""
                    INSERT OR REPLACE INTO analytics_live_data 
                    SELECT * FROM df_live
                """)

        # Update sync metadata with the highest transaction ID processed
        # This ensures next sync only processes newer transactions
        max_tx_id = new_txs['tx_id'].max()
        self.conn.execute("UPDATE _sync_metadata SET value = ? WHERE key='last_synced_tx_id'", [max_tx_id])
        logging.info(f"Sync complete. Updated to tx_id {max_tx_id}.")

    def read_to_pandas(self, query: str) -> pd.DataFrame:
        """
        Execute a custom SQL query against DuckDB and return results as DataFrame.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Pandas DataFrame containing query results
            
        Example:
            df = duckdb.read_to_pandas("SELECT AVG(Value) FROM analytics_live_data GROUP BY Organisation")
        """
        return self.conn.execute(query).fetchdf()

    def get(self, **filters) -> pd.DataFrame:
        """
        Query analytics_live_data with optional column filters.
        
        Args:
            **filters: Keyword arguments where each key is a column name
                       and value is either a single value or iterable for IN clause
                       
        Returns:
            Pandas DataFrame containing matching records
            
        Raises:
            ValueError: If column name contains double quotes (SQL injection prevention)
            
        Example:
            df = duckdb.get(Date='2024-01-01', Organisation=['RBNZ', 'TREASURY'])
        """
        query = "SELECT * FROM analytics_live_data"
        params = []
        conditions = []
        
        for col, value in filters.items():
            # Validate column name to prevent SQL injection
            if '"' in col:
                raise ValueError(f"Invalid column name: '{col}'. Column names cannot contain double quotes.")
                
            quoted_col = self._quote_id(col)
                
            if isinstance(value, (list, tuple, set)):
                # Skip empty iterables
                if not value:
                    continue 
                # Use IN clause for multiple values
                placeholders = ','.join(['?'] * len(value))
                conditions.append(f"{quoted_col} IN ({placeholders})")
                for v in value:
                    # Convert Pandas Timestamp to Python datetime for parameter binding
                    params.append(v.to_pydatetime() if isinstance(v, pd.Timestamp) else v)
            else:
                # Use equality for single value
                conditions.append(f"{quoted_col} = ?")
                params.append(value.to_pydatetime() if isinstance(value, pd.Timestamp) else value)
                
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        return self.conn.execute(query, params).fetchdf()

    def close(self):
        """Close the DuckDB database connection."""
        self.conn.close()

#%%
# =============================================================================
# DEMONSTRATION / USAGE EXAMPLE SECTION
# =============================================================================
# This section demonstrates the typical workflow for using the dual-database
# architecture. In production, you would import the classes and use them
# separately in your application code.
        
import pandas as pd
import numpy as np
from RBNZ_Toolbox import ffs
import time

# -----------------------------------------------------------------------------
# Step 1: Initialize Class 1 (SQLite System of Record)
# -----------------------------------------------------------------------------
# Configure with your database path, primary key columns, and variable definitions
sqlite_db = SQLiteTimeSeriesManager(
    db_path=r"C:\Test\timeseries_operational.db",
    table_keys=["Date", "Time Series", "Organisation"],  # Composite primary key
    variables={"Value": "METRIC"},                        # Data columns with types
    default_user="data_engineer"                          # Default audit user
)

# -----------------------------------------------------------------------------
# Step 2: Initialize Class 2 (DuckDB Analytical Layer)
# -----------------------------------------------------------------------------
# DuckDB syncs from SQLite - pass the SQLite manager instance
duckdb_manager = DuckDBSyncManager(
    duckdb_path=r"C:\Test\timeseries_analytics.duckdb",
    sqlite_manager=sqlite_db
)

# --- SCENARIO 1: User uploads new data via Pandas ---
print("\n--- Uploading Data via Pandas ---")
# Create dummy data (in real usage, this would come from your data source)
dates = pd.date_range("2023-10-01", periods=5, freq='D', tz='UTC')
df_upload = ffs.get('LVRN.MMb1.AA', release_date='2024-08-30')

# Upload to SQLite - automatically detects APPEND vs EDIT based on primary key
# Records are written to both live_data and historical_data with full audit trail
sqlite_db.upload_from_pandas(df_upload, user="analyst_1")

# --- SCENARIO 2: User edits existing data ---
print("\n--- Editing Data via Pandas ---")
time.sleep(10)  # Simulate time passing between operations
df_edit = ffs.get('LVRN.MMB1.AA')
# Same method handles EDITs automatically - compares old vs new values
sqlite_db.upload_from_pandas(df_edit, user="analyst_3")

df_edit.iloc[-1, -1] = 100  # Modify a value to demonstrate edit detection

# --- SCENARIO 3: Sync changes to DuckDB analytical layer ---
print("\n--- Syncing to DuckDB ---")
# Incremental sync - only processes transactions since last sync point
# Efficient for large datasets with frequent small updates
duckdb_manager.sync()

# --- SCENARIO 4: Query analytics layer with filters ---
print("\n--- Downloading Live State to Pandas ---")
# Fast analytical queries against DuckDB with optional filtering
df_live = duckdb_manager.get()
print(df_live)

# Example filtered query (uncomment to use):
# df_filtered = duckdb_manager.get(Date='2024-01-01', Organisation=['RBNZ'])

# Cleanup (uncomment in production to properly close connections)
# sqlite_db.close()
# duckdb_manager.close()