"""
profiler.py

Computes descriptive statistics for every column in a table.
Uses DuckDB SQL — no LLM involved. Pure deterministic computation.

Returns a structured dict that the orchestrator will later pass to the LLM.
"""

import duckdb
from reader import DataSource


# ─────────────────────────────────────────────────────────────
# COLUMN TYPE CLASSIFICATION
# ─────────────────────────────────────────────────────────────
# DuckDB returns type names like "INTEGER", "VARCHAR", "DOUBLE", etc.
# We group them into two buckets: numeric or categorical.

NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",
    "REAL", "INT", "INT2", "INT4", "INT8",
}

def _is_numeric(col_type: str) -> bool:
    """Returns True if the DuckDB column type is numeric."""
    # Normalize to uppercase and strip precision info (e.g. "DECIMAL(10,2)" → "DECIMAL")
    base_type = col_type.upper().split("(")[0].strip()
    return base_type in NUMERIC_TYPES


# ─────────────────────────────────────────────────────────────
# NUMERIC METRICS QUERY
# ─────────────────────────────────────────────────────────────

def _profile_numeric_column(conn: duckdb.DuckDBPyConnection, table_ref: str, col: str) -> dict:
    """
    Runs a single SQL query to compute all numeric metrics for one column.

    Parameters:
        conn      : the active DuckDB connection
        table_ref : SQL reference to the table (e.g. "read_csv_auto('path/to/file.csv')")
        col       : column name to profile
    """
    # We wrap the column name in double quotes to handle names with spaces or special chars
    query = f"""
        SELECT
            COUNT("{col}")                                      AS count,
            AVG("{col}")                                        AS mean,
            STDDEV("{col}")                                     AS std,
            MIN("{col}")                                        AS min,
            QUANTILE_CONT("{col}", 0.25)                       AS "25%",
            QUANTILE_CONT("{col}", 0.50)                       AS "50%",
            QUANTILE_CONT("{col}", 0.75)                       AS "75%",
            MAX("{col}")                                        AS max
        FROM {table_ref}
    """

    row = conn.execute(query).fetchone()

    # fetchone() returns a tuple — we zip it with the column names to make a dict
    keys = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
    return dict(zip(keys, row))


# ─────────────────────────────────────────────────────────────
# CATEGORICAL METRICS QUERY
# ─────────────────────────────────────────────────────────────

def _profile_categorical_column(conn: duckdb.DuckDBPyConnection, table_ref: str, col: str) -> dict:
    """
    Runs SQL queries to compute all categorical metrics for one column.

    Parameters:
        conn      : the active DuckDB connection
        table_ref : SQL reference to the table
        col       : column name to profile
    """
    # Query 1: count and unique
    base_query = f"""
        SELECT
            COUNT("{col}")              AS count,
            COUNT(DISTINCT "{col}")     AS unique
        FROM {table_ref}
    """
    base_row = conn.execute(base_query).fetchone()

    # Query 2: top value and its frequency
    # We order by frequency descending and take the first row
    top_query = f"""
        SELECT
            "{col}"         AS top,
            COUNT("{col}")  AS freq
        FROM {table_ref}
        WHERE "{col}" IS NOT NULL
        GROUP BY "{col}"
        ORDER BY freq DESC
        LIMIT 1
    """
    top_row = conn.execute(top_query).fetchone()

    # top_row could be None if the column is entirely null
    top_val  = top_row[0] if top_row else None
    freq_val = top_row[1] if top_row else 0

    return {
        "count":  base_row[0],
        "unique": base_row[1],
        "top":    top_val,
        "freq":   freq_val,
    }


# ─────────────────────────────────────────────────────────────
# DISTINCT VALUES
# ─────────────────────────────────────────────────────────────

def _get_distinct_values(
    conn: duckdb.DuckDBPyConnection,
    table_ref: str,
    col: str,
    threshold: int = 25,
) -> dict:
    """
    Returns the distinct values of a column, ordered by frequency descending.
    If the number of distinct values exceeds `threshold`, returns None instead
    of the list — to avoid cluttering the JSON and the LLM context window.

    Parameters:
        conn      : the active DuckDB connection
        table_ref : SQL reference to the table
        col       : column name
        threshold : max distinct values to list (default: 25)
                    Columns with more distinct values get skipped —
                    their count is already captured in the metrics dict.

    Returns a dict with:
        {
            "distinct_count": 5,
            "values": ["PG", "G", "PG-13", "R", "NC-17"],  ← or None if above threshold
            "skipped": False                                 ← True means too many values
        }
    """
    # Step 1: check how many distinct values exist BEFORE fetching them all
    # This avoids pulling thousands of rows just to decide to skip the column
    count_row = conn.execute(f"""
        SELECT COUNT(DISTINCT "{col}") FROM {table_ref}
    """).fetchone()

    distinct_count = count_row[0]

    # Step 2: if above threshold, skip fetching the actual values
    if distinct_count > threshold:
        return {
            "distinct_count": distinct_count,
            "values":         None,
            "skipped":        True,
        }

    # Step 3: fetch actual values ordered by frequency (most common first)
    # NULL values are excluded — their presence is already captured in metrics
    rows = conn.execute(f"""
        SELECT "{col}"
        FROM {table_ref}
        WHERE "{col}" IS NOT NULL
        GROUP BY "{col}"
        ORDER BY COUNT("{col}") DESC
    """).fetchall()

    # fetchall() returns a list of 1-tuples: [("PG",), ("G",), ...]
    # We unpack each tuple to get just the value
    values = [row[0] for row in rows]

    return {
        "distinct_count": distinct_count,
        "values":         values,
        "skipped":        False,
    }


# ─────────────────────────────────────────────────────────────
# MAIN PROFILING FUNCTION
# ─────────────────────────────────────────────────────────────

def profile_table(source: DataSource, table_name: str, distinct_threshold: int = 25) -> dict:
    """
    Profiles all columns in a single table.
    Dispatches to numeric or categorical profiling per column,
    and appends distinct value information for each column.

    Parameters:
        source             : a DataSource instance (e.g. CSVDataSource)
        table_name         : name of the table to profile
        distinct_threshold : max distinct values to list per column (default: 25).
                             Columns with more values get skipped (values=None, skipped=True).
                             Tune down for wide/large tables, up for small lookup tables.
    """
    schema = source.get_schema(table_name)
    conn   = source.conn

    # Build the SQL table reference DuckDB will use in FROM clauses
    # For CSV: "read_csv_auto('/path/to/rental.csv')"
    table_ref = _build_table_ref(source, table_name)

    # Get total row count (includes nulls — this is the full table size)
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0]

    columns_profile = {}

    for col_def in schema:
        col_name = col_def["name"]
        col_type = col_def["type"]

        if _is_numeric(col_type):
            metrics = _profile_numeric_column(conn, table_ref, col_name)
            col_kind = "numeric"
        else:
            metrics = _profile_categorical_column(conn, table_ref, col_name)
            col_kind = "categorical"

        distinct = _get_distinct_values(conn, table_ref, col_name, threshold=distinct_threshold)

        columns_profile[col_name] = {
            "type":     col_kind,
            "dtype":    col_type,       # original DuckDB type (e.g. "VARCHAR", "INTEGER")
            "nullable": col_def["nullable"],
            "metrics":  metrics,
            "distinct": distinct,
        }

    return {
        "table":     table_name,
        "row_count": row_count,
        "columns":   columns_profile,
    }


def profile_all_tables(source: DataSource, distinct_threshold: int = 25) -> dict:
    """
    Profiles every table available in a DataSource.
    Returns a dict keyed by table name.

    Usage:
        source  = get_datasource("csv", folder_path="./data/dvd_rentals")
        results = profile_all_tables(source)
        results = profile_all_tables(source, distinct_threshold=50)  # more permissive

    Parameters:
        source             : a DataSource instance
        distinct_threshold : passed through to profile_table (default: 25)
    """
    results = {}

    tables = source.list_tables()
    print(f"Found {len(tables)} table(s): {tables}\n")

    for table_name in tables:
        print(f"  Profiling: {table_name}...")
        results[table_name] = profile_table(source, table_name, distinct_threshold=distinct_threshold)
        print(f"  Done: {table_name} — {results[table_name]['row_count']} rows")

    return results


# ─────────────────────────────────────────────────────────────
# INTERNAL HELPER
# ─────────────────────────────────────────────────────────────

def _build_table_ref(source: DataSource, table_name: str) -> str:
    """
    Builds the SQL FROM clause reference for a given source type.
    Currently handles CSV. Extend this when adding new source types.
    """
    from reader import CSVDataSource

    if isinstance(source, CSVDataSource):
        path = source.folder_path / f"{table_name}.csv"
        return f"read_csv_auto('{path}')"

    # Future: BigQueryDataSource, SnowflakeDataSource, etc.
    raise NotImplementedError(f"No table_ref builder for source type: {type(source).__name__}")

