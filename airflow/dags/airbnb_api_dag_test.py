## Importing libraries from Airflow

import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Importing local scripts

sys.path.append("/home/tabas/personal-dev/pyprojects")
from pipelines.utils.get_credentials import getting_gcp_credentials
from pipelines.airbnb_api_v0.airbnb_api_script import set_checkin_and_checkout_parameters, set_location_parameters

## Creating DAG

with DAG(
    "airbnb_api_dag_test"
    , start_date=datetime(2025, 1, 20)
    , schedule_interval='@weekly'
    , catchup=False
    , tags=['airbnb_api', 'api']
):
    
    ## Defining DAG tasks
    
    get_credentials = PythonOperator(
        task_id = 'get_credentials'
        , python_callable=getting_gcp_credentials
    )
    
    get_date_parameters = PythonOperator(
        task_id = 'get_date_parameters'
        , python_callable=set_checkin_and_checkout_parameters
    )
    
    get_location_parameters = PythonOperator(
        task_id = 'get_location_parameters'
        , python_callable=set_location_parameters
    )
    
    get_api_request = PythonOperator(
        task_id = 'get_api_request'
        , python_callable=getting_gcp_credentials
    )
    
    unload_data_to_gcp = PythonOperator(
        task_id = 'unload_data_to_gcp'
        , python_callable=getting_gcp_credentials
    )
    
    get_credentials >> get_api_parameters >> get_api_request >> unload_data_to_gcp

