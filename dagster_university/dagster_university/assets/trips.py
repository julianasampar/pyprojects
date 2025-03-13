import requests
import sys

import duckdb
import os
import dagster as dg
from dagster._utils.backoff import backoff


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

@dg.asset(
    deps=["taxi_trips_file"]
)
def taxi_trips():
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)

@dg.asset(
        deps=["taxi_zone_file"]
)
def zones():
    query = """
    create or replace table zones as (
        select 
            LocationID as zone_id
            , zone
            , borough
            , the_geom as geometry
        from 'data/raw/taxi_zones.csv'
    )
    """
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)
