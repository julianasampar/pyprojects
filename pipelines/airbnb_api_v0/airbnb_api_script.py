## This script define the functions to be called on Airbnb API DAG

# Importing libraries

import sys
from datetime import datetime, timedelta
import pandas as pd

sys.path.append("/home/tabas/personal-dev/pyprojects")
from pipelines.utils.get_credentials import getting_gcp_credentials

# The function below sets the Check-In and Check-Out parameters for the API GET request

def set_checkin_and_checkout_parameters():
 
    currentTimestamp =  datetime.today()

    ## Creating list to save CheckIn and Checkout combinations

    checkInAndOutDates = [
            {'checkin': (currentTimestamp + timedelta(days=30)).strftime('%Y-%m-%d'), 
            'checkout': (currentTimestamp + timedelta(days=60)).strftime('%Y-%m-%d')
            },
            #{'checkin': (currentTimestamp + timedelta(days=60)).strftime('%Y-%m-%d'), 
            #'checkout': (currentTimestamp + timedelta(days=90)).strftime('%Y-%m-%d')
            #},
            ##{'checkin': (currentTimestamp + timedelta(days=90)).strftime('%Y-%m-%d'), 
            ## 'checkout': (currentTimestamp + timedelta(days=120)).strftime('%Y-%m-%d')
            ##}
        ]
    
    return checkInAndOutDates

# The function below sets the Location parameters for the API GET request

def set_location_parameters():
 
    ## Query collecting desired Neighbourhoods

    sql =  """
            SELECT 
                DISTINCT CONCAT(city, ', ', neighborhood) AS city_and_neighbourhood_search
            FROM `tabas-dw.master_data.dim_tabas_buildings_and_apartments`
            LIMIT 1
            """
            
    ## Creating dataframe neighbourhoods to write the results
    
    getting_gcp_credentials()

    neighbourhoods = BIGQUERY.query(sql).result().to_dataframe()
    neighbourhoods = neighbourhoods.values.tolist()
    
    return neighbourhoods
