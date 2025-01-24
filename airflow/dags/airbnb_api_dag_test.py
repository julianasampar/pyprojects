## Importing libraries from Airflow

import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

## Importing local scripts

sys.path.append("/home/tabas/personal-dev/pyprojects")
import pipelines.airbnb_api_v0.airbnb_api_script as api

## Creating DAG

with DAG(
    "airbnb_api_dag_test"
    , start_date=datetime(2025, 1, 20)
    , schedule_interval='@weekly'
    , catchup=False
    , tags=['airbnb_api', 'api']
):
    
    ## Defining DAG tasks
    
    set_date_parameters = PythonOperator(
        task_id = 'set_date_parameters'
        , python_callable=api.set_checkin_and_checkout_parameters
    )
    
    set_location_parameters = PythonOperator(
        task_id = 'set_location_parameters'
        , python_callable=api.set_location_parameters
    )
    
    get_api_request = PythonOperator(
        task_id = 'get_api_request'
        , python_callable=api.get_airbnb_api_request
    )
    
    set_date_parameters >> set_location_parameters >> get_api_request 

