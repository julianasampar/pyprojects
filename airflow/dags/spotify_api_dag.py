# Importing libraries

import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Importing local scripts

sys.path.append("/home/tabas/personal-dev/pyprojects")
import pipelines.spotify_api_v0.spotify_api_script as api

with DAG(
    "extract_spotify_api_data"
    , start_date=datetime(2025, 2, 1)
    , schedule_interval='@daily'
    , catchup=False
    , tags=['spotify_api', 'api']
):
    ## Defining DAG tasks
    # This DAG will be developed with several splitted tasks so that I can further understand how Xcom works
      
    get_new_releases = PythonOperator(
        task_id = 'get_new_releases'
        , python_callable=api.get_new_releases
    )
    
    prep_new_releases = PythonOperator(
        task_id='prep_new_releases'
        , python_callable=api.prep_new_releases
    )
    
    get_artists = PythonOperator(
        task_id='get_artists'
        , python_callable=api.get_artists
    )
    
    prep_artists = PythonOperator(
        task_id='prep_artists'
        , python_callable=api.prep_artists
    )
    
    get_albums = PythonOperator(
        task_id='get_albums'
        , python_callable=api.get_albums
    )
    
    prep_albums = PythonOperator(
        task_id='prep_album'
        , python_callable=api.prep_albums
    )
    
    get_tracks = PythonOperator(
        task_id='get_tracks'
        , python_callable=api.get_tracks
    )
    
    prep_tracks = PythonOperator(
        task_id='prep_tracks'
        , python_callable=api.prep_tracks
    )
    
    #unload_data = 
    
    get_new_releases >> prep_new_releases >> [get_artists, get_albums ]
    get_artists >> prep_artists
    get_albums >> prep_albums >> get_tracks >> prep_tracks
    #[prep_new_releases, prep_artists, prep_albums, prep_tracks] >> unload_data

    