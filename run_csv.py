import sqlite3
import pandas as pd

conn = sqlite3.connect("dbt_database.db")
df = pd.read_csv("others/archive/Air_Quality_20250825.csv")
df = df.to_sql("nyc_air_quality__historical", conn, if_exists="replace", index=False)
conn.close()
