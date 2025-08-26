import sqlite3
import pandas as pd

conn = sqlite3.connect("dbt_database.db")
df = pd.read_csv("others/archive/squirrel-data - squirrel-data.csv")
df = df.to_sql("nyc_central_park_squirrels_census__2020", conn, if_exists="replace", index=False)
conn.close()
