# -*- coding: utf-8 -*-
"""
Created on Fri May 29 16:23:17 2026

@author: farquharsona
"""

import duckdb
import pandas as pd
from typing import Optional

class VectorisedStorage:
    """
    DuckDB storage layer optimized for Pandas workflows.

    Core idea:
        DataFrames in → DataFrames out

    Designed for:
    - storing time series
    - caching query results
    - replacing CSV / parquet / JSON pipelines
    """

    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)

    # ==========================
    # Core Table Operations
    # ==========================

    def write_df(self, df: pd.DataFrame, table: str, mode: str = "append"):
        """
        Write a DataFrame to a table.

        Parameters
        ----------
        table : str
        df : pd.DataFrame
        mode : 'append' | 'replace'
        """

        if mode == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            self.conn.register("temp_df", df)
            self.conn.execute(f"CREATE TABLE {table} AS SELECT * FROM temp_df")

        elif mode == "append":
            if not self._table_exists(table):
                self.write_df(table, df, mode="replace")
            else:
                self.conn.register("temp_df", df)
                self.conn.execute(f"INSERT INTO {table} SELECT * FROM temp_df")

        else:
            raise ValueError("mode must be 'append' or 'replace'")

    def read_df(self, table: str) -> pd.DataFrame:
        """Load full table as DataFrame"""
        try:
            return self.conn.execute(f"SELECT * FROM {table}").fetchdf()
        except Exception as e:
            if not table in self.list_tables()['name']:
                raise Exception('Table does not exist --- Run list_tables command to see available tables')
            else:
                raise e

    def query_df(self, query: str) -> pd.DataFrame:
        """Run SQL and return DataFrame"""
        return self.conn.execute(query).fetchdf()

    def delete_table(self, table: str):
        self.conn.execute(f"DROP TABLE IF EXISTS {table}")

    def list_tables(self):
        return self.conn.execute("SHOW TABLES").fetchdf()

    # ==========================
    # Upsert (very useful)
    # ==========================

    def upsert_df(self, table: str, df: pd.DataFrame, keys: list):
        """
        Upsert DataFrame into table using key columns.

        Equivalent to:
        - insert new rows
        - update existing rows

        Parameters
        ----------
        table : str
        df : pd.DataFrame
        keys : list of column names (primary key)
        """

        if not self._table_exists(table):
            self.write_df(table, df, mode="replace")
            return

        temp_table = f"{table}_temp"

        self.conn.register("temp_df", df)
        self.conn.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS SELECT * FROM temp_df")

        key_join = " AND ".join([f"t.{k} = s.{k}" for k in keys])

        self.conn.execute(f"""
            DELETE FROM {table} t
            USING {temp_table} s
            WHERE {key_join}
        """)

        self.conn.execute(f"""
            INSERT INTO {table}
            SELECT * FROM {temp_table}
        """)

    # ==========================
    # Time-Series Convenience
    # ==========================

    def write_timeseries(
        self,
        table: str,
        df: pd.DataFrame,
        time_col: str = "Date"
    ):
        """
        Standardised time-series write.

        Ensures:
        - datetime conversion
        - sorted index
        """
        df = df.copy()
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.sort_values(time_col)

        self.write_df(table, df, mode="append")

    def read_timeseries(
        self,
        table: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        time_col: str = "Date"
    ) -> pd.DataFrame:
        """
        Load time-series with optional filtering
        """

        query = f"SELECT * FROM {table}"

        conditions = []
        if start:
            conditions.append(f"{time_col} >= TIMESTAMP '{start}'")
        if end:
            conditions.append(f"{time_col} <= TIMESTAMP '{end}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        return self.query_df(query)

    # ==========================
    # Fast Filtering Helpers
    # ==========================

    def filter_df(
        self,
        table: str,
        where: str
    ) -> pd.DataFrame:
        """
        Example:
            store.filter_df("fs_data", "Organisation = 'ANZ'")
        """
        return self.query_df(f"SELECT * FROM {table} WHERE {where}")
    
    # ==========================
    # CSV / JSON I/O
    # ==========================

    def from_csv(
        self,
        file_path: str,
        table: str,
        mode: str = "replace",
        **read_csv_kwargs
    ):
        """
        Load CSV into DuckDB table.

        Uses DuckDB's fast CSV reader.

        Parameters
        ----------
        file_path : str
        table : str
        mode : 'replace' | 'append'
        read_csv_kwargs : passed to DuckDB read_csv_auto
        """

        if mode == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            self.conn.execute(f"""
                CREATE TABLE {table} AS
                SELECT * FROM read_csv_auto('{file_path}')
            """)

        elif mode == "append":
            if not self._table_exists(table):
                self.from_csv(file_path, table, mode="replace")
            else:
                self.conn.execute(f"""
                    INSERT INTO {table}
                    SELECT * FROM read_csv_auto('{file_path}')
                """)

        else:
            raise ValueError("mode must be 'append' or 'replace'")

    def to_csv(
        self,
        table: str,
        file_path: str,
        **to_csv_kwargs
    ):
        """
        Export table to CSV using Pandas.

        Parameters
        ----------
        table : str
        file_path : str
        to_csv_kwargs : passed to pandas.DataFrame.to_csv
        """

        df = self.read_df(table)
        df.to_csv(file_path, index=False, **to_csv_kwargs)

    def from_json(
        self,
        file_path: str,
        table: str,
        mode: str = "replace",
        orient: Optional[str] = None,
        lines: bool = False,
        **read_json_kwargs
    ):
        """
        Load JSON into DuckDB table.

        Uses Pandas for flexibility.

        Parameters
        ----------
        file_path : str
        table : str
        mode : 'replace' | 'append'
        orient : JSON orientation
        lines : JSON lines format
        """

        df = pd.read_json(
            file_path,
            orient=orient,
            lines=lines,
            **read_json_kwargs
        )

        self.write_df(df, table, mode=mode)

    def to_json(
        self,
        table: str,
        file_path: str,
        orient: str = "records",
        lines: bool = False,
        **to_json_kwargs
    ):
        """
        Export table to JSON.

        Parameters
        ----------
        table : str
        file_path : str
        orient : 'records', 'split', 'index', etc.
        lines : write as JSON lines
        """

        df = self.read_df(table)
        df.to_json(
            file_path,
            orient=orient,
            lines=lines,
            **to_json_kwargs
        )

    # ==========================
    # Internal Helpers
    # ==========================

    def _table_exists(self, table: str) -> bool:
        result = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table}'
        """).fetchone()

        return result[0] > 0

    def close(self):
        self.conn.close()

import sqlite3
import pandas as pd
from typing import Optional

class StableStorage:
    """
    SQLite storage layer optimized for Pandas workflows.

    Core idea:
        DataFrames in → DataFrames out
    """

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)

    # ==========================
    # Core Table Operations
    # ==========================

    def write_df(self, df: pd.DataFrame, table: str, mode: str = "append"):
        """
        Write DataFrame to SQLite table
        """

        if mode == "replace":
            df.to_sql(table, self.conn, if_exists="replace", index=False)

        elif mode == "append":
            if not self._table_exists(table):
                df.to_sql(table, self.conn, if_exists="replace", index=False)
            else:
                df.to_sql(table, self.conn, if_exists="append", index=False)

        else:
            raise ValueError("mode must be 'append' or 'replace'")

    def read_df(self, table: str) -> pd.DataFrame:
        try:
            return pd.read_sql_query(f"SELECT * FROM {table}", self.conn)
        except Exception as e:
            if table not in self.list_tables()["name"].values:
                raise Exception(
                    "Table does not exist --- Run list_tables to see available tables"
                )
            else:
                raise e

    def query_df(self, query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, self.conn)

    def delete_table(self, table: str):
        self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        self.conn.commit()

    def list_tables(self):
        return pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table'", self.conn
        )

    # ==========================
    # Upsert (SQLite version)
    # ==========================

    def upsert_df(self, table: str, df: pd.DataFrame, keys: list):
        """
        SQLite-compatible upsert:

        Strategy:
        - delete matching keys
        - append DataFrame
        """

        if not self._table_exists(table):
            self.write_df(df, table, mode="replace")
            return

        # Build WHERE clause
        conditions = " OR ".join(
            [
                " AND ".join([f"{k} = ?" for k in keys])
                for _ in df.itertuples(index=False)
            ]
        )

        # Flatten key values
        values = []
        for row in df[keys].itertuples(index=False):
            values.extend(row)

        if conditions:
            self.conn.execute(
                f"DELETE FROM {table} WHERE {conditions}", values
            )

        # Append new data
        df.to_sql(table, self.conn, if_exists="append", index=False)
        self.conn.commit()

    # ==========================
    # Time-Series Convenience
    # ==========================

    def write_timeseries(self, table: str, df: pd.DataFrame, time_col: str = "Date"):
        df = df.copy()
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.sort_values(time_col)

        self.write_df(df, table, mode="append")

    def read_timeseries(
        self,
        table: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        time_col: str = "Date",
    ) -> pd.DataFrame:

        query = f"SELECT * FROM {table}"

        conditions = []
        if start:
            conditions.append(f"{time_col} >= '{start}'")
        if end:
            conditions.append(f"{time_col} <= '{end}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        return self.query_df(query)

    # ==========================
    # Fast Filtering Helpers
    # ==========================

    def filter_df(self, table: str, where: str) -> pd.DataFrame:
        return self.query_df(f"SELECT * FROM {table} WHERE {where}")

    # ==========================
    # CSV / JSON I/O
    # ==========================

    def from_csv(
        self,
        file_path: str,
        table: str,
        mode: str = "replace",
        **read_csv_kwargs
    ):
        df = pd.read_csv(file_path, **read_csv_kwargs)
        self.write_df(df, table, mode=mode)

    def to_csv(
        self,
        table: str,
        file_path: str,
        **to_csv_kwargs
    ):
        df = self.read_df(table)
        df.to_csv(file_path, index=False, **to_csv_kwargs)

    def from_json(
        self,
        file_path: str,
        table: str,
        mode: str = "replace",
        orient: Optional[str] = None,
        lines: bool = False,
        **read_json_kwargs
    ):
        df = pd.read_json(
            file_path,
            orient=orient,
            lines=lines,
            **read_json_kwargs
        )

        self.write_df(df, table, mode=mode)

    def to_json(
        self,
        table: str,
        file_path: str,
        orient: str = "records",
        lines: bool = False,
        **to_json_kwargs
    ):
        df = self.read_df(table)
        df.to_json(
            file_path,
            orient=orient,
            lines=lines,
            **to_json_kwargs
        )

    # ==========================
    # Internal Helpers
    # ==========================

    def _table_exists(self, table: str) -> bool:
        result = self.conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()

        return result[0] > 0

    def close(self):
        self.conn.close()

    