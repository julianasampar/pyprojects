## Creating DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pip

with DAG(
    "install_libraries"
    , start_date=datetime(2025, 1, 20)
    , schedule='@daily'
    , catchup=True
    , tags=['lib', 'pip install']
):
    
    def install_libraries():
        import sys
        import subprocess
        
        packages = [
            'pandas'
            , 'google.cloud'
            , 'google.cloud.bigquery'
            , 'google.cloud.storage'
            , 'json'
        ]
        
        for i in range(len(packages)):

            subprocess.check_call([sys.executable, '-m', 'pip', 'install', packages[i]])
    
    ## Defining DAG tasks
    
    install_libs = PythonOperator(
        task_id = 'install_libs'
        , python_callable=install_libraries
    )
    
    install_libs