## This script define the functions to be called on Airbnb API DAG

# Importing libraries

import sys
from datetime import datetime, timedelta
import pandas as pd

sys.path.append("/home/tabas/personal-dev/pyprojects")
import pipelines.utils.get_credentials as cr

# The function below sets the Check-In and Check-Out parameters for the API GET request

def set_checkin_and_checkout_parameters():
    
    # Importing libraries
    
    from datetime import datetime, timedelta
 
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
    
    cr.getting_gcp_credentials()

    neighbourhoods = BIGQUERY.query(sql).result().to_dataframe()
    neighbourhoods = neighbourhoods.values.tolist()
    
    return neighbourhoods

def get_airbnb_api_request():
    
    import requests
    import json
    import pandas as pd
    
    ## Creating dataframe df to write the following loop results

    df = pd.DataFrame(columns=['badges'
                            , 'coordinates'
                                , 'id'
                                , 'images'
                                , 'price'
                                , 'rating'
                                , 'reviews'
                                , 'roomTitle'
                                , 'roomType'
                                , 'subTitle'
                                , 'title'
                                , 'url'
                                , 'location'
                                , 'checkin'
                                , 'checkout'
                                , 'adults'
                                , 'scrappedPage'
                                , 'extractionTimestamp'
                            ])
    ## Getting RapidAPI credentials
    
    cr.getting_rapidapi_credentials()
    
    ## Define the variables to access Airbnb Scraper API

    url = "https://airbnb-scraper-api.p.rapidapi.com/airbnb_search_stays_v2"

    headers = {
        'x-rapidapi-key': rapidapi_key,
        'x-rapidapi-host': "airbnb-scraper-api.p.rapidapi.com"
}
