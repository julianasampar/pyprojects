## Creating DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import subprocess

with DAG(
    "install_libraries"
    , start_date=datetime(2025, 1, 20)
    , schedule='@daily'
    , catchup=True
    , tags=['lib', 'pip install']
):
    packages = [
            'pandas'
            , 'google.cloud'
            , 'google.cloud.bigquery'
            , 'google.cloud.storage'
            , 'json'
            , 'pyarrow'
    ]
        
    for i in range(len(packages)):
            
        def install_libraries():

            subprocess.check_call([sys.executable, '-m', 'pip', 'install', packages[i]])
        
            ## Defining DAG tasks
                
        task = PythonOperator(
            task_id = f"install_{packages[i]}"
            , python_callable=install_libraries
        )
        
        task