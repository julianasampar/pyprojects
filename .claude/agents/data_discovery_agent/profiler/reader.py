"""
reader.py

This module defines HOW the profiler reads data.
It does NOT profile, describe, or call any LLM.
Its only job: connect to a data source and return data/schema.
"""

import duckdb
from pathlib import Path
from abc import ABC, abstractmethod

class DataSource(ABC):

    @abstractmethod
    def list_tables(self) -> list[str]:
        """
        Returns a list of table (or file) names available in this source.
        Example: ['rental', 'customer', 'film']
        """
        pass

    @abstractmethod
    def read_table(self, table_name: str, sample_size: int = 10_000) -> "duckdb.DuckDBPyRelation":
        """
        Returns a DuckDB relation (lazy query) for the given table.
        The data is NOT fully loaded into memory — DuckDB reads it on demand.

        Parameters:
            table_name  : the name of the table to read
            sample_size : max number of rows to sample (default: 10,000)
        """
        pass

    @abstractmethod
    def get_schema(self, table_name: str) -> list[dict]:
        """
        Returns the schema of a table as a list of column definitions.
        Each column is a dict with keys: 'name', 'type', 'nullable'

        Example:
            [
                {"name": "rental_id",   "type": "INTEGER", "nullable": False},
                {"name": "rental_date", "type": "TIMESTAMP", "nullable": True},
            ]
        """
        pass


# ─────────────────────────────────────────────────────────────
# CSV IMPLEMENTATION
# ─────────────────────────────────────────────────────────────
# This is the concrete implementation for a folder of CSV files.
# It fulfills the DataSource contract for local CSV files.

class CSVDataSource(DataSource):

    def __init__(self, folder_path: str):
        """
        Sets up the data source pointing to a folder of CSV files.

        Parameters:
            folder_path : path to the folder containing your .csv files
                          Example: "./data/dvd_rentals"
        """
        # Path() turns a string like "./data" into a proper filesystem path
        self.folder_path = Path(folder_path)

        # One shared DuckDB connection for all queries in this source
        # duckdb.connect() without arguments = in-memory database
        self.conn = duckdb.connect()

        # Validate that the folder actually exists
        if not self.folder_path.exists():
            raise FileNotFoundError(f"Folder not found: {self.folder_path}")

    def list_tables(self) -> list[str]:
        """
        Scans the folder and returns the name of each CSV file (without extension).
        Example: 'rental.csv' → 'rental'
        """
        # glob("*.csv") finds all files ending in .csv inside the folder
        # .stem gives us the filename without the extension
        return [f.stem for f in self.folder_path.glob("*.csv")]

    def read_table(self, table_name: str, sample_size: int = 10_000) -> "duckdb.DuckDBPyRelation":
        """
        Reads a CSV file using DuckDB and returns a sampled relation.

        - read_csv_auto() detects column types automatically
        - USING SAMPLE limits rows BEFORE loading into memory (efficient)
        - Returns a DuckDB relation, not a full dataframe
          (call .df() on the result if you need a pandas dataframe)
        """
        path = self.folder_path / f"{table_name}.csv"

        if not path.exists():
            raise FileNotFoundError(f"Table not found: {path}")

        # The f-string builds the SQL query dynamically with the actual path and sample size
        query = f"""
            SELECT *
            FROM read_csv_auto('{path}')
            USING SAMPLE {sample_size} ROWS
        """

        return self.conn.execute(query)

    def get_schema(self, table_name: str) -> list[dict]:
        """
        Uses DuckDB's DESCRIBE statement to get column names and types.
        Returns a clean list of dicts — easy to pass to the LLM later.
        """
        path = self.folder_path / f"{table_name}.csv"

        if not path.exists():
            raise FileNotFoundError(f"Table not found: {path}")

        # DESCRIBE returns: column_name, column_type, null, key, default, extra
        rows = self.conn.execute(f"""
            DESCRIBE SELECT * FROM read_csv_auto('{path}')
        """).fetchall()

        # Reshape into clean dicts for easier use downstream
        return [
            {
                "name":     row[0],
                "type":     row[1],
                "nullable": row[2] == "YES",
            }
            for row in rows
        ]
    
    
# ─────────────────────────────────────────────────────────────
# DEFAULT FUNCTION (optional but useful)
# ─────────────────────────────────────────────────────────────
# Instead of importing CSVDataSource directly everywhere,
# you can use this function to get the right source by name.
# Later you'll add "bigquery", "snowflake", etc. here.

def get_datasource(source_type: str, **kwargs) -> DataSource:
    """
    Returns the correct DataSource implementation based on source_type.

    Usage:
        source = get_datasource("csv", folder_path="./data/dvd_rentals")

    Parameters:
        source_type : one of "csv" (more to come)
        **kwargs    : arguments passed to the DataSource constructor
    """
    sources = {
        "csv": CSVDataSource,
        # "bigquery":  BigQueryDataSource,   ← add later
        # "snowflake": SnowflakeDataSource,  ← add later
    }

    if source_type not in sources:
        raise ValueError(f"Unknown source type '{source_type}'. Available: {list(sources.keys())}")

    return sources[source_type](**kwargs)


