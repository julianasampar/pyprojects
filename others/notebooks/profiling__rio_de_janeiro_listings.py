from pathlib import Path

import pandas as pd
from ydata_profiling import ProfileReport

ROOT = Path(__file__).resolve().parents[2]  # pyprojects/
csv_path = ROOT / "others/archive/rio_de_janeiro/airbnb_listings.csv"

df = pd.read_csv(csv_path)
df["price"] = pd.to_numeric(
    df["price"].str.replace(r"[$,]", "", regex=True), errors="coerce"
)

profile = ProfileReport(df, title="Rio de Janeiro Listings Profile", minimal=True)
profile.to_file(ROOT / "others/rio_de_janeiro_listings_profile.html")