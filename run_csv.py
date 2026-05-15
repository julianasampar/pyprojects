import sqlite3
import duckdb
from pathlib import Path
import pandas as pd

def import_csv_to_sqlite(database, database_path, folder_path):
    path = Path(folder_path)

    if database == 'sqlite':
        conn = sqlite3.connect(database_path)
    elif database == 'duckdb':
        conn = duckdb.connect(database_path)
        
    for file in path.glob("*.csv"):
        table_name = f"{path.name}__{file.stem}"

        if database == 'sqlite':
            df = pd.read_csv(f"{file}")
            df = df.to_sql(f"{table_name}", conn, if_exists="replace", index=False)
            print(f"Successfully imported csv file {table_name} into SQLite")

            conn.close()

        elif database == 'duckdb':
            conn.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file}')")
            print(f"Successfully created table {table_name} in DuckDB")
        
    conn.close()


import_csv_to_sqlite("duckdb", \
                     "./dbt/dbt_database.duckdb", \
                     "/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/dvd_rental_store")
