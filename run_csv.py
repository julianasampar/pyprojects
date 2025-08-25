import sqlite3
import pandas as pd

conn = sqlite3.connect("dbt_database.db")
df = pd.read_csv("dbt_project/seeds/nyc_street_tree_census__2015.csv")
df = df.to_sql("nyc_street_tree_census__2015", conn, if_exists="replace", index=False)
conn.close()
