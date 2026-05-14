import dagster as dg
import sys

sys.path.append("/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/dagster_university/dagster_university")
from resources import database_resource
from assets import trips, metrics

trip_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules([metrics])


defs = dg.Definitions(
    assets=[*trip_assets, *metric_assets]
    , resources={
        "database":database_resource
    }
)