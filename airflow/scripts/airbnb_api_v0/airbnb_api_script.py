## This script define the functions to be called on Airbnb API DAG

def set_checkin_and_checkout_parameters(ti, **kwargs):
# This function sets the Check-In and Check-Out parameters for the API GET request 
    
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
    
    ti.xcom_push(key='date_api_parameter_key', value=checkInAndOutDates)


def set_location_parameters(ti, **kwargs):
# This function sets the Location parameters for the API GET request
    
    # Importing libraries
    
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pipelines.utils.personal_env as penv

    ## Importing Credentials from Google Cloud

    CREDENTIALS = service_account.Credentials.from_service_account_file(penv.bq_path)
    BIGQUERY = bigquery.Client(credentials=CREDENTIALS)
 
    ## Query collecting desired Neighbourhoods

    sql =  """
            SELECT 
                DISTINCT CONCAT(city, ', ', neighborhood) AS city_and_neighbourhood_search
            FROM `tabas-dw.master_data.dim_tabas_buildings_and_apartments`
            LIMIT 1
            """
            
    ## Creating dataframe neighbourhoods to write the results

    neighbourhoods = BIGQUERY.query(sql).result().to_dataframe()
    neighbourhoods = neighbourhoods.values.tolist()
    
    ti.xcom_push(key='location_api_parameter_key', value=neighbourhoods)


def get_airbnb_api_request(ti, **kwargs): 
# This function make the GET request to Airbnb Scrapper API and writes a dataframe with the result
    
    import requests
    import json
    import pandas as pd
    import pipelines.utils.personal_env as penv
    from datetime import datetime
    
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
        
    ## Define the variables to access Airbnb Scraper API

    url = "https://airbnb-scraper-api.p.rapidapi.com/airbnb_search_stays_v2"

    headers = {
        'x-rapidapi-key': penv.rapidapi_key,
        'x-rapidapi-host': "airbnb-scraper-api.p.rapidapi.com"
}
    
    checkInAndOutDates = ti.xcom_pull(
        key='date_api_parameter_key'
        , task_ids='set_date_parameters'
        )
    neighbourhoods = ti.xcom_pull(
        key='location_api_parameter_key'
        , task_ids='set_location_parameters'
        )

    
    for i in range(len(neighbourhoods)):

        for j in range(len(checkInAndOutDates)):

            ## The cursor is an unique indicator of the page, this helps the API to know which page to scrap next
            ## It is re-set no None on a new request

            cursor = None
            hasNextPage = True

            ## Creating the following dataframe to follow-up the amount of pages scrapped
            ## It is re-set to empty on a new request

            cursorDataFrame = []

            ## The following parameter estipulates the limit amount of pages to be scrapped
            ## , if desired

            pageLimitation = 3

            while hasNextPage and len(cursorDataFrame) < pageLimitation:
        
                querystring = {
                    "location": neighbourhoods[i][0],                  # Desired location
                    "checkIn": checkInAndOutDates[j]['checkin'],       # Check-in Date
                    "checkOut": checkInAndOutDates[j]['checkout'],      # Check-out Date
                    "adults": "2",                                      # Number of adults
                    "roomType": "2",                                    # Type of Acommodation: Entire Space
                    "cursor": cursor
                }

                ## Logging the location being sent to the request

                print("Getting Request: ", querystring)

                ## Send GET request to the API

                response = requests.get(url, headers=headers, params=querystring)

                ## Extract the JSON text data into the variable 'data'

                data = response.text

                ## Convert JSON into a Pandas Dataframe

                data = json.loads(data)
                extracted = pd.DataFrame.from_dict(data['data'])

                ## Setting the new cursor value to scrape the following page

                cursor = data['pageInfo']['endCursor']
                hasNextPage = data['pageInfo']['hasNextPage']

                ## Add the cursor result to the cursor dataframe

                cursorDataFrame.append(cursor)

                ## Create new columns on extracted DataFrame to append API variables

                extracted['location'] = neighbourhoods[i][0]
                extracted['checkin'] = checkInAndOutDates[j]['checkin']
                extracted['checkout'] = checkInAndOutDates[j]['checkout']
                extracted['adults'] = 2
                #extracted['roomType'] = 2     -> Information already exists on JSON
                extracted['scrappedPage'] = len(cursorDataFrame)
                extracted['extractionTimestamp'] = datetime.today().strftime('%Y-%m-%d %X')

                ## Add the result to the previous created Dataframe
                
                df = df = pd.concat([df, extracted])

                print("Successfully added API request to DataFrame")

            else:
                if len(cursorDataFrame) > 0:
                    print("Sucessfully scraped ", len(cursorDataFrame), " pages")
                else: 
                    print("Error on API request")

    print("End of API request")
    
    ## Unloading data on GCP
    
    ## Importing Credentials from Google Cloud

    from google.cloud import storage
    from google.oauth2 import service_account
    
    CREDENTIALS = service_account.Credentials.from_service_account_file(penv.bq_path)
    STORAGE = storage.Client(credentials=CREDENTIALS)
    
    # Acessing Bucket Path

    bucket = STORAGE.get_bucket(penv.bucket_path)
    
    # Getting currentTimestamp (again)

    currentTimestamp = datetime.today().strftime('%Y-%m-%d %X')

    # Adding currentTimestamp on file name, so it doesn't overwrite itself. 
    # Also, it helps keep track on incremental models

    file_name = f"airbnb_api_test_data__{currentTimestamp}"
    
    ## Defining a function called avro_df_prep to prepare the dataframe for the Avro format

    def avro_df_prep():

        # pip install fastavro

        from fastavro import writer, parse_schema

        # Converting all columns to string, because Avro doesn't support object type

        columns_to_convert = [  # Lista de colunas definidas no esquema Avro
            'badges', 'coordinates', 'id', 'images', 'price', 'rating', 
            'reviews', 'roomTitle', 'roomType', 'subTitle', 'title', 
            'url', 'location', 'checkin', 'checkout', 'adults', 
            'scrappedPage', 'extractionTimestamp'
        ]

        df[columns_to_convert] = df[columns_to_convert].astype(str)

        # Declaring dataframe schema

        schema = {
            'name': 'test_data'
            , 'type': 'record'
            , 'fields': [
                            {'name': 'badges', 'type': 'string'}, 
                            {'name': 'coordinates', 'type': 'string'}, 
                            {'name': 'id', 'type': 'string'}, 
                            {'name': 'images', 'type': 'string'}, 
                            {'name': 'price', 'type': 'string'}, 
                            {'name': 'rating', 'type': 'string'}, 
                            {'name': 'reviews', 'type': 'string'}, 
                            {'name': 'roomTitle', 'type': 'string'}, 
                            {'name': 'roomType', 'type': 'string'}, 
                            {'name': 'subTitle', 'type': 'string'}, 
                            {'name': 'title', 'type': 'string'}, 
                            {'name': 'url', 'type': 'string'}, 
                            {'name': 'location', 'type': 'string'}, 
                            {'name': 'checkin', 'type': 'string'}, 
                            {'name': 'checkout', 'type': 'string'}, 
                            {'name': 'adults', 'type': 'string'}, 
                            {'name': 'scrappedPage', 'type': 'string'}, 
                            {'name': 'extractionTimestamp', 'type': 'string'}
                        ]

        }

        parsed_schema = parse_schema(schema)
        records = df.to_dict('records')

        # Writing an Avro file on 'archive' directory

        with open(f'/home/tabas/personal-dev/pyprojects/pipelines/archive/{file_name}.avro', 'wb') as out:
            writer(out, parsed_schema, records)
            
    ## Writing Dataframe to Bucket folder with desired file format 

    file_formats = [
                    'csv'
                    , 'parquet'
                    , 'json'
                    , 'orc'
                    , 'avro'
    ]

    for i in range(len(file_formats)):
        
        blob = bucket.blob(f"{penv.bucket_folder}/{file_name}.{file_formats[i]}")
        
        if file_formats[i] == 'csv':
            blob.upload_from_string(df.to_csv(), '/text/csv')
            print("Sucessfully written in ", file_formats[i])
        if file_formats[i] == 'parquet':
            blob.upload_from_string(df.to_parquet(), '/text/plain')
            print("Sucessfully written in ", file_formats[i])
        if file_formats[i] == 'json':
            blob.upload_from_string(df.to_json(orient='table'), '/text/plain')
            print("Sucessfully written in ", file_formats[i])
        if file_formats[i] == 'orc':
            blob.upload_from_string(df.reset_index().to_orc(index=None), '/text/plain')
            print("Sucessfully written in ", file_formats[i])        
        if file_formats[i] == 'avro':
            avro_df_prep()
            blob.upload_from_filename(f'/home/tabas/personal-dev/pyprojects/pipelines/archive/{file_name}.avro', '/text/plain')
            print("Sucessfully written in ", file_formats[i])        
