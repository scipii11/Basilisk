# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 18:28:06 2026

@author: farquharsona
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SQLiteTimeSeriesManager:
    """
    Class 1: Manages the SQLite3 System of Record.
    Handles atomic transactions, audit logging, and Pandas integration.
    """
    
    TYPE_MAP = {
        'TIMESTAMP': {'sqlite': 'TEXT', 'pandas': 'datetime64[ns, UTC]'},
        'METRIC':    {'sqlite': 'REAL', 'pandas': 'float64'},
        'CATEGORY':  {'sqlite': 'TEXT', 'pandas': 'string'},
        'ID':        {'sqlite': 'INTEGER', 'pandas': 'Int64'}
    }

    def __init__(self, db_path: str, table_keys: List[str], variables: Dict[str, str], default_user: str = "system"):
        self.db_path = db_path
        self.table_keys = table_keys
        self.variables = variables
        self.default_user = default_user
        
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        
        self.conn = sqlite3.connect(self.db_path, timeout=10)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        
        self._initialize_schema()

    def _quote_id(self, name: str) -> str:
        return f'"{name.replace("\"", "\"\"")}"'

    def _initialize_schema(self):
        cur = self.conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS type_conversions (
                semantic_type TEXT PRIMARY KEY, sqlite_type TEXT, python_type TEXT
            )
        """)
        for sem_type, mapping in self.TYPE_MAP.items():
            cur.execute("INSERT OR IGNORE INTO type_conversions VALUES (?, ?, ?)", 
                        (sem_type, mapping['sqlite'], mapping['pandas']))

        cur.execute("""
            CREATE TABLE IF NOT EXISTS tx_log (
                tx_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, user TEXT,
                operation TEXT, table_keys_json TEXT, old_values_json TEXT, 
                new_values_json TEXT, status TEXT
            )
        """)

        hist_cols = [f"{self._quote_id(k)} {self.TYPE_MAP[self.variables.get(k, 'CATEGORY')]['sqlite']}" for k in self.table_keys]
        hist_cols += [f"{self._quote_id(v)} {self.TYPE_MAP[t]['sqlite']}" for v, t in self.variables.items()]
        hist_cols_str = ", ".join(hist_cols)
        
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS historical_data (
                version_id INTEGER PRIMARY KEY AUTOINCREMENT, tx_id INTEGER, timestamp TEXT,
                {hist_cols_str}, FOREIGN KEY(tx_id) REFERENCES tx_log(tx_id)
            )
        """)

        live_cols = [f"{self._quote_id(k)} {self.TYPE_MAP[self.variables.get(k, 'CATEGORY')]['sqlite']}" for k in self.table_keys]
        live_cols += [f"{self._quote_id(v)} {self.TYPE_MAP[t]['sqlite']}" for v, t in self.variables.items()]
        live_cols_str = ", ".join(live_cols)
        pk_str = ", ".join([self._quote_id(k) for k in self.table_keys])
        
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS live_data (
                {live_cols_str}, last_updated TEXT, PRIMARY KEY ({pk_str})
            )
        """)
        self.conn.commit()

    def _coerce_df_to_sql(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if getattr(df[col].dt, 'tz', None) is not None:
                    df[col] = df[col].dt.tz_convert('UTC')
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        df = df.where(pd.notnull(df), None)
        return df

    def _coerce_sql_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col, sem_type in self.variables.items():
            if sem_type == 'TIMESTAMP':
                df[col] = pd.to_datetime(df[col], utc=True)
            elif sem_type == 'METRIC':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        for col in self.table_keys:
            if col in df.columns and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], utc=True)
                except (ValueError, TypeError):
                    pass
        return df

    def upload_from_pandas(self, df: pd.DataFrame, user: Optional[str] = None, chunk_size: int = 50000):
        user = user or self.default_user
        df = self._coerce_df_to_sql(df)
        
        required_cols = self.table_keys + list(self.variables.keys())
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in DataFrame: {missing}")

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            self._process_chunk_atomic(chunk, user)

    def _process_chunk_atomic(self, chunk: pd.DataFrame, user: str):
        cur = self.conn.cursor()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        try:
            cur.execute("BEGIN IMMEDIATE;")
            
            keys_in_chunk = chunk[self.table_keys].drop_duplicates()
            placeholders = ",".join(["?"] * len(self.table_keys))
            in_clause = ",".join([f"({placeholders})"] * len(keys_in_chunk))
            flat_keys = [val for row in keys_in_chunk.values for val in row]
            
            quoted_keys = [self._quote_id(k) for k in self.table_keys]
            query = f"SELECT * FROM live_data WHERE ({','.join(quoted_keys)}) IN ({in_clause})"
            existing_df = pd.read_sql_query(query, self.conn, params=flat_keys)
            
            cols = self.table_keys + list(self.variables.keys())
            insert_cols = cols + ['last_updated']
            
            tx_records = []
            hist_records = []
            live_records = []
            
            for idx, row in chunk.iterrows():
                # Normalize to native Python types to prevent JSON serialization errors and ensure safe comparison
                key_vals = {k: (row[k].item() if hasattr(row[k], 'item') else row[k]) for k in self.table_keys}
                var_vals = {v: (row[v].item() if hasattr(row[v], 'item') else row[v]) for v in self.variables.keys()}
                
                existing_row = existing_df[
                    (existing_df[self.table_keys] == pd.Series(key_vals)).all(axis=1)
                ]
                
                if not existing_row.empty:
                    old_vals = existing_row.iloc[0][list(self.variables.keys())].to_dict()
                    # Normalize old_vals to native Python types as well
                    old_vals = {k: (v.item() if hasattr(v, 'item') else v) for k, v in old_vals.items()}
                    
                    # --- OPTIMIZATION: Skip if data is exactly the same ---
                    if old_vals == var_vals:
                        continue
                    # ----------------------------------------------------
                    
                    op = 'EDIT'
                else:
                    op = 'APPEND'
                    old_vals = None
                    
                tx_records.append((
                    now, user, op, 
                    json.dumps(key_vals), 
                    json.dumps(old_vals) if old_vals else None, 
                    json.dumps(var_vals), 
                    'SUCCESS'
                ))
                
                hist_records.append(tuple([None, None, now] + [row[c] for c in cols]))
                live_records.append(tuple([row[c] for c in cols] + [now]))

            # If all rows were skipped, commit empty transaction and return
            if not tx_records:
                self.conn.commit()
                logging.info(f"Chunk processed, but all rows were identical to existing data. Skipped updates.")
                return

            cur.executemany("""
                INSERT INTO tx_log (timestamp, user, operation, table_keys_json, old_values_json, new_values_json, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, tx_records)
            
            first_tx_id = cur.execute("SELECT last_insert_rowid()").fetchone()[0] - len(tx_records) + 1
            
            for i, rec in enumerate(hist_records):
                hist_records[i] = (rec[0], first_tx_id + i, *rec[2:])
                
            quoted_cols = [self._quote_id(c) for c in cols]
            insert_cols_quoted = [self._quote_id(c) for c in insert_cols]
            
            cur.executemany(f"""
                INSERT INTO historical_data (version_id, tx_id, timestamp, {','.join(quoted_cols)})
                VALUES (?, ?, ?, {','.join(['?']*len(cols))})
            """, hist_records)
            
            placeholders = ','.join(['?'] * len(insert_cols))
            update_clause = ','.join([f"{self._quote_id(c)}=excluded.{self._quote_id(c)}" for c in self.variables.keys()]) + ", last_updated=excluded.last_updated"
            
            cur.executemany(f"""
                INSERT INTO live_data ({','.join(insert_cols_quoted)}) 
                VALUES ({placeholders})
                ON CONFLICT({','.join(quoted_keys)}) 
                DO UPDATE SET {update_clause}
            """, live_records)
            
            self.conn.commit()
            logging.info(f"Successfully committed chunk of {len(chunk)} rows ({len(tx_records)} actual changes).")
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Transaction failed and rolled back: {e}")
            raise

    def download_to_pandas(self, table: str = 'live_data', filters: Dict[str, Any] = None) -> pd.DataFrame:
        query = f"SELECT * FROM {self._quote_id(table)}"
        params = []
        
        if filters:
            conditions = []
            for k, v in filters.items():
                if isinstance(v, (pd.Timestamp, datetime.datetime)):
                    v = pd.to_datetime(v, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                elif isinstance(v, list):
                    v = [pd.to_datetime(x, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%fZ') if isinstance(x, (pd.Timestamp, datetime.datetime)) else x for x in v]

                quoted_k = self._quote_id(k)
                if isinstance(v, list):
                    placeholders = ','.join(['?'] * len(v))
                    conditions.append(f"{quoted_k} IN ({placeholders})")
                    params.extend(v)
                else:
                    conditions.append(f"{quoted_k} = ?")
                    params.append(v)
            query += " WHERE " + " AND ".join(conditions)
            
        df = pd.read_sql_query(query, self.conn, params=params)
        return self._coerce_sql_to_df(df)

    def get_table_names(self) -> List[str]:
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        tables = self.conn.execute(query).fetchall()
        return [table[0] for table in tables]

    def get_table(self, table_name: str) -> pd.DataFrame:
        available_tables = self.get_table_names()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' does not exist. Available tables: {available_tables}")
        
        df = pd.read_sql_query(f"SELECT * FROM {self._quote_id(table_name)}", self.conn)
        if table_name == 'live_data':
            df = self._coerce_sql_to_df(df)
        return df

    def close(self):
        self.conn.close()


class DuckDBSyncManager:
    """
    Class 2: Manages the DuckDB Analytical Layer.
    Syncs from SQLite3 at runtime, optimized for fast reads and batched writes.
    """
    
    DUCKDB_TYPE_MAP = {
        'TIMESTAMP': 'TIMESTAMP',
        'METRIC': 'DOUBLE',
        'CATEGORY': 'VARCHAR',
        'ID': 'BIGINT'
    }

    def __init__(self, duckdb_path: str, sqlite_manager: SQLiteTimeSeriesManager):
        self.duckdb_path = duckdb_path
        self.sqlite_mgr = sqlite_manager
        
        os.makedirs(os.path.dirname(os.path.abspath(duckdb_path)), exist_ok=True)
        
        self.conn = duckdb.connect(self.duckdb_path)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _sync_metadata (key VARCHAR PRIMARY KEY, value BIGINT)
        """)
        self.conn.execute("INSERT OR IGNORE INTO _sync_metadata VALUES ('last_synced_tx_id', 0)")
        
        self._initialize_schema()

    def _quote_id(self, name: str) -> str:
        return f'"{name.replace("\"", "\"\"")}"'

    def _initialize_schema(self):
        cols = [f"{self._quote_id(k)} {self.DUCKDB_TYPE_MAP[self.sqlite_mgr.variables.get(k, 'CATEGORY')]}" for k in self.sqlite_mgr.table_keys]
        cols += [f"{self._quote_id(v)} {self.DUCKDB_TYPE_MAP[t]}" for v, t in self.sqlite_mgr.variables.items()]
        cols_str = ", ".join(cols)
        
        pk_str = ", ".join([self._quote_id(k) for k in self.sqlite_mgr.table_keys])
        
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS analytics_live_data (
                {cols_str},
                last_updated TIMESTAMP,
                PRIMARY KEY ({pk_str})
            )
        """)

    def sync(self):
        last_synced = self.conn.execute("SELECT value FROM _sync_metadata WHERE key='last_synced_tx_id'").fetchone()[0]
        
        new_txs = pd.read_sql_query(
            "SELECT tx_id, operation, table_keys_json FROM tx_log WHERE tx_id > ? AND status='SUCCESS'", 
            self.sqlite_mgr.conn, 
            params=[last_synced]
        )
        
        if new_txs.empty:
            return

        logging.info(f"Syncing {len(new_txs)} new transactions to DuckDB...")
        
        upsert_keys_json = new_txs[new_txs['operation'] != 'DELETE']['table_keys_json'].unique()
        delete_keys_json = new_txs[new_txs['operation'] == 'DELETE']['table_keys_json'].unique()
        
        if len(delete_keys_json) > 0:
            delete_keys = [json.loads(k) for k in delete_keys_json]
            df_del = pd.DataFrame(delete_keys)
            
            self.conn.execute("DELETE FROM analytics_live_data USING df_del WHERE " + 
                              " AND ".join([f"analytics_live_data.{self._quote_id(k)} = df_del.{self._quote_id(k)}" for k in self.sqlite_mgr.table_keys]))

        if len(upsert_keys_json) > 0:
            upsert_keys = [json.loads(k) for k in upsert_keys_json]
            df_keys = pd.DataFrame(upsert_keys)
            
            placeholders = ",".join(["?"] * len(self.sqlite_mgr.table_keys))
            in_clause = ",".join([f"({placeholders})"] * len(df_keys))
            flat_keys = [val for row in df_keys.values for val in row]
            
            quoted_keys = [self._quote_id(k) for k in self.sqlite_mgr.table_keys]
            query = f"SELECT * FROM live_data WHERE ({','.join(quoted_keys)}) IN ({in_clause})"
            df_live = pd.read_sql_query(query, self.sqlite_mgr.conn, params=flat_keys)
            
            if not df_live.empty:
                df_live['last_updated'] = pd.to_datetime(df_live['last_updated'])
                for col, sem_type in self.sqlite_mgr.variables.items():
                    if sem_type == 'TIMESTAMP':
                        df_live[col] = pd.to_datetime(df_live[col])
                
                self.conn.execute(f"""
                    INSERT OR REPLACE INTO analytics_live_data 
                    SELECT * FROM df_live
                """)

        max_tx_id = new_txs['tx_id'].max()
        self.conn.execute("UPDATE _sync_metadata SET value = ? WHERE key='last_synced_tx_id'", [max_tx_id])
        logging.info(f"Sync complete. Updated to tx_id {max_tx_id}.")

    def read_to_pandas(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()

    def get(self, **filters) -> pd.DataFrame:
        query = "SELECT * FROM analytics_live_data"
        params = []
        conditions = []
        
        for col, value in filters.items():
            if '"' in col:
                raise ValueError(f"Invalid column name: '{col}'. Column names cannot contain double quotes.")
                
            quoted_col = self._quote_id(col)
                
            if isinstance(value, (list, tuple, set)):
                if not value:
                    continue 
                placeholders = ','.join(['?'] * len(value))
                conditions.append(f"{quoted_col} IN ({placeholders})")
                for v in value:
                    params.append(v.to_pydatetime() if isinstance(v, pd.Timestamp) else v)
            else:
                conditions.append(f"{quoted_col} = ?")
                params.append(value.to_pydatetime() if isinstance(value, pd.Timestamp) else value)
                
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        return self.conn.execute(query, params).fetchdf()

    def close(self):
        self.conn.close()

#%%
        
import pandas as pd
import numpy as np
from RBNZ_Toolbox import ffs
import time

# 1. Initialize Class 1 (SQLite)
sqlite_db = SQLiteTimeSeriesManager(
    db_path=r"C:\Test\timeseries_operational.db",
    table_keys=["Date", "Time Series", "Organisation"],
    variables={"Value": "METRIC"},
    default_user="data_engineer"
)

# 2. Initialize Class 2 (DuckDB)
duckdb_manager = DuckDBSyncManager(
    duckdb_path=r"C:\Test\timeseries_analytics.duckdb",
    sqlite_manager=sqlite_db
)

# --- SCENARIO: User uploads data via Pandas ---
print("\n--- Uploading Data via Pandas ---")
# Create dummy data
dates = pd.date_range("2023-10-01", periods=5, freq='D', tz='UTC')
df_upload = ffs.get('LVRN.MMb1.AA', release_date='2024-08-30')

# Upload to SQLite (Handles APPEND automatically)
sqlite_db.upload_from_pandas(df_upload, user="analyst_1")

# --- SCENARIO: User edits existing data ---
print("\n--- Editing Data via Pandas ---")
time.sleep(10)
df_edit = ffs.get('LVRN.MMB1.AA')
sqlite_db.upload_from_pandas(df_edit, user="analyst_3") # Handles EDIT automatically

df_edit.iloc[-1, -1] = 100

# --- SCENARIO: Sync to DuckDB ---
print("\n--- Syncing to DuckDB ---")
duckdb_manager.sync()

# --- SCENARIO: Download current live state to Pandas ---
print("\n--- Downloading Live State to Pandas ---")
df_live = duckdb_manager.get()
print(df_live)

# Cleanup
# sqlite_db.close()
# duckdb_manager.close()