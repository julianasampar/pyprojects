## Importing libraries from Airflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    "airbnb_api_dag_test"
    , start_date=datetime(2025, 1, 20)
    , schedule_interval='@weekly'
    , catchup=False
    , tag=['airbnb_api', 'api']
):
    
    get_credentials = PythonOperator(
        task_id = 'get_credentials'
    )
    
    get_api_parameters = PythonOperator(
        task_id = 'get_api_parameters'
    )
    
    get_api_request = PythonOperator(
        task_id = 'get_api_request'
    )
    
    unload_data_to_gcp = PythonOperator(
        task_id = 'unload_data_to_gcp'
    )
    
    get_credentials >> get_api_parameters >> get_api_request >> unload_data_to_gcp

