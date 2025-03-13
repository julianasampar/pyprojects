import requests
import sys
import dagster as dg

TAXI_TRIPS_TEMPLATE_FILE_PATH = "data/raw/taxi_trips_{}.parquet"
TAXI_ZONES_FILE_PATH = "data/raw/taxi_zones.csv"


@dg.asset
def taxi_trips_file():
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

@dg.asset
def taxi_zone_file():
    """
      The raw parquet files for the taxi zone dataset. Sourced from the NYC Open Data portal.
      The data contains distinct keys and values for each zone of NYC.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)