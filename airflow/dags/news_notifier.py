import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

sys.path.append("/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects")
from claude.agents.news_notification_agent import notifier

with DAG(
    "news_notifier"
    , start_date=datetime(2026, 5, 31)
    , schedule="0 */5 * * *"
    , catchup=False
    , tags=['news_notifier', 'agentic']
):
    
    call_notifier = PythonOperator(
        task_id = 'call_notifier'
        , python_callable=notifier
    )

    call_notifier