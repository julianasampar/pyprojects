# Script dedicated to ingesting data into local databases (SQLite, DuckDB)

import sqlite3
import duckdb
from pathlib import Path
import pandas as pd

import os
from dotenv import load_dotenv
load_dotenv()

def import_csv_to_sqlite(database, folder_path):
    path = Path(folder_path)

    if database == 'sqlite':
        database_path = os.getenv('SQLITE')
        conn = sqlite3.connect(database_path)

    elif database == 'duckdb':
        database_path = os.getenv('DUCKDB')
        conn = duckdb.connect(database_path)
        
    for file in path.glob("*.csv"):
        table_name = f"{path.name}__{file.stem}"

        if database == 'sqlite':
            df = pd.read_csv(f"{file}")
            df = df.to_sql(f"{table_name}", conn, if_exists="replace", index=False)
            print(f"Successfully imported csv file {table_name} into SQLite")


        elif database == 'duckdb':
            conn.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file}')")
            print(f"Successfully created table {table_name} in DuckDB")
        
    conn.close()

# How to use it:
# import_csv_to_sqlite("duckdb", \
    #                 "/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/airbnb/rio_de_janeiro")
