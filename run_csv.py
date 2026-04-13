import sqlite3
from pathlib import Path
import pandas as pd

def import_csv_to_sqlite(database, folder_path):
    ## Database name: "dbt_database.db"
    conn = sqlite3.connect(database)
    path = Path(folder_path)
    
    for file in path.glob("*.csv"):
        df = pd.read_csv(f"{file}")
        df = df.to_sql(f"{path.name}__{file.stem}", conn, if_exists="replace", index=False)
        print(f"Successfully imported csv file {path.name}__{file.stem} into SQLite")

    conn.close()

import_csv_to_sqlite("dbt_database.db", "/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/dvd_rental_store")
