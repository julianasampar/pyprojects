## Creating DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import subprocess

with DAG(
    "install_packages"
    , start_date=datetime(2025, 8, 4)
    , schedule='@once'
    , catchup=False
    , tags=['lib', 'pip install']
):
    packages = [
            'pandas'
            , 'google.cloud'
            , 'google.cloud.bigquery'
            , 'google.cloud.storage'
            , 'json'
            , 'pyarrow'
            , 'fastavro'
            , 'spotipy'
    ]
        
    for i in range(len(packages)):
            
        def install_packages():

            subprocess.check_call([sys.executable, '-m', 'pip', 'install', packages[i]])
        
            ## Defining DAG tasks
                
        task = PythonOperator(
            task_id = f"install_{packages[i]}"
            , python_callable=install_packages
        )
        
        task