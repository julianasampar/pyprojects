## Defining a function called avro_df_prep to prepare the dataframe for the Avro format

def avro_df_prep(df, file_name):

    # pip install fastavro

    from fastavro import writer, parse_schema

    # Converting all columns to string, because Avro doesn't support object type

    columns_to_convert = [  # Lista de colunas definidas no esquema Avro
        'album_type', 'external_urls', 'href',
       'id', 'images', 'name', 'release_date', 'release_date_precision',
       'type', 'uri', 'artists', 'restrictions']

    df[columns_to_convert] = df[columns_to_convert].astype(str)

    # Declaring dataframe schema

    schema = { ###!!! it does not work yet, I need to figure out a way to extract the schema automatically
        'name': 'spotify'
        , 'type': 'record'
        , 'fields': [
                        {'name': 'album_type', 'type': 'string'}, 
                        {'name': 'total_tracks', 'type': 'int'}, 
                        {'name': 'is_playable', 'type': 'boolean'}, 
                        {'name': 'external_urls', 'type': 'string'},
                        {'name': 'id', 'type': 'string'},
                        {'name': 'images', 'type': 'string'},  
                        {'name': 'name', 'type': 'string'}, 
                        {'name': 'release_date', 'type': 'string'}, 
                        {'name': 'release_date_precision', 'type': 'string'}, 
                        {'name': 'href', 'type': 'string'}, 
                        {'name': 'type', 'type': 'string'}, 
                        {'name': 'uri', 'type': 'string'}, 
                        {'name': 'artists', 'type': 'string'}, 
                        {'name': 'restrictions', 'type': 'string'}, 
                    ]
    }

    parsed_schema = parse_schema(schema)
    records = df.to_dict('records')

    # Writing an Avro file on 'archive' directory

    with open(f'/home/tabas/personal-dev/pyprojects/pipelines/archive/{file_name}.avro', 'wb') as out:
       writer(out, parsed_schema, records)
       

## Function to write Dataframe to Bucket folder with desired file format 
    
def unload_data(origin, file_formats, df_dict, bucket, bucket_folder):
    ## origin: name of table source
    ## file_formats: LIST of file formats - can take csv, parquet, json, orc and avro as arguments; 
    ## df_dict: DICTIONARY where the key is the dataframe name and the value is the dataframe itself
    
    from datetime import datetime
    from google.cloud import storage
    from google.oauth2 import service_account
    import pipelines.utils.personal_env as penv

    CREDENTIALS = service_account.Credentials.from_service_account_file(penv.bq_path)
    STORAGE = storage.Client(credentials=CREDENTIALS)
    bucket = STORAGE.get_bucket(bucket)

    df = df_dict

    for i in range(len(file_formats)):
        
        for key, value in df.items():
            
            # Getting currentTimestamp            
            currentTimestamp = datetime.today().strftime('%Y-%m-%d %X')

            # Adding currentTimestamp on file name, so it doesn't overwrite itself. 
            # Also, it helps keep trackof data unloaded for incremental models

            file_name = f"{origin}__{key}_data__{currentTimestamp}"
        
            blob = bucket.blob(f"{bucket_folder}/{file_name}.{file_formats[i]}")
            
            if file_formats[i] == 'csv':
                print("Begin at: ", datetime.today().strftime('%Y-%m-%d %X'))
                blob.upload_from_string(value.to_csv(), '/text/csv')
                print("Sucessfully written ", key, " in ", file_formats[i])
                print("End at: ", datetime.today().strftime('%Y-%m-%d %X'))
                
            if file_formats[i] == 'parquet':
                print("\nBegin at: ", datetime.today().strftime('%Y-%m-%d %X'))
                blob.upload_from_string(value.to_parquet(), '/text/plain')
                print("Sucessfully written ", key, " in ", file_formats[i])
                print("End at: ", datetime.today().strftime('%Y-%m-%d %X'))
                
            if file_formats[i] == 'json':
                print("\nBegin at: ", datetime.today().strftime('%Y-%m-%d %X'))
                blob.upload_from_string(value.to_json(orient='table'), '/text/plain')
                print("Sucessfully written ", key, " in ", file_formats[i])
                print("End at: ", datetime.today().strftime('%Y-%m-%d %X'))
                
            if file_formats[i] == 'orc':
                print("\nBegin at: ", datetime.today().strftime('%Y-%m-%d %X'))
                blob.upload_from_string(value.reset_index().to_orc(index=None), '/text/plain')
                print("Sucessfully written ", key, " in ", file_formats[i])     
                print("End at: ", datetime.today().strftime('%Y-%m-%d %X'))
                
            if file_formats[i] == 'avro':
                print("\nBegin at: ", datetime.today().strftime('%Y-%m-%d %X'))
                avro_df_prep()
                blob.upload_from_filename(f'/home/tabas/personal-dev/pyprojects/pipelines/archive/{file_name}.avro', 'wb', '/text/plain')
                print("Sucessfully written ", key, " in ", file_formats[i])        
                print("End at: ", datetime.today().strftime('%Y-%m-%d %X'))
                