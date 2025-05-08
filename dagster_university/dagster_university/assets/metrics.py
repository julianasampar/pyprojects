import dagster as dg
from dagster_duckdb import DuckDBResource
import matplotlib.pyplot as plt
import geopandas as gpd
import os

@dg.asset
def manhattan_stats(database: DuckDBResource):
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(os.getenv("MANHATTAN_STATS_FILE_PATH"), 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map():
    trips_by_zone = gpd.read_file(os.getenv("MANHATTAN_STATS_FILE_PATH"))

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(os.getenv("MANHATTAN_MAP_FILE_PATH"), format="png", bbox_inches="tight")
    plt.close(fig)

@dg.asset(
    deps=["taxi_trips"]
)
def trips_by_week(database: DuckDBResource):
    query = """
        SELECT
            DATE_TRUNC('week', pickup_datetime) AS week
            , COUNT(*) AS num_trips
            , SUM(passenger_count) AS passenger_count
            , SUM(total_amount) AS total_amount
            , SUM(trip_distance) AS trip_distance
        FROM trips
        GROUP BY ALL
    """

    with database.get_connection() as conn:
        trips_by_week = conn.execute(query).fetch_df()

    trips_by_week.to_csv(os.getenv("TRIPS_BY_WEEK_FILE_PATH"), index=False)

