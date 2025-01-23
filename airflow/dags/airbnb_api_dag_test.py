## Importing libraries from Airflow

import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Importing local scripts

sys.path.append("/home/tabas/personal-dev/pyprojects")
from pipelines.utils.get_credentials import getting_gcp_credentials

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
    
    get_api_parameters = PythonOperator(
        task_id = 'get_api_parameters'
        , python_callable=getting_gcp_credentials
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

