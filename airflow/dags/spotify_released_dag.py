# Importing libraries

import sys 
sys.path.append("airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importing local scripts
import scripts.spotify_api_v0.spotify_api_script as api
import scripts.utils.common as com

with DAG(
    "extract_spotify_releases_data"
    , start_date=datetime(2025, 2, 1)
    , schedule=timedelta(days=15)
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
    
    unload_data = PythonOperator(
        task_id='unload_data'
        , python_callable=api.unload_spotify_data
    )
    
    get_new_releases >> prep_new_releases >> unload_data

    