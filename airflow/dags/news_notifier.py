import sys
import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator 
from datetime import datetime
from anthropic import Anthropic

sys.path.append("/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects")
from claude.agents.notification_agent import notifier

client = Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY")
)

user_input = "Send me a Mac Notifications containg headline and content for one of the latest news in the world and/or Brazil of the last 5 HOURS."

system = """ You are a news reporter. Your role is to create OS Notifications to report the latest news.
            Focus on the text. Don't include any HTML or XML tags.
            The content must fit into the size (320 pixels x 340 pixels) of the MacOs notification banner.
            One notification must report only one news.
            Make sure to keep it concise and to format the text in a readable and appropriate way for Mac notifications.
            Interests: Politics, Economics,  International Affairs.
        """
print(os.getenv("ANTHROPIC_API_KEY"))

def run_notifier():
    notifier.chat(user_input=user_input, system=system)

with DAG(
    "news_notifier"
    , start_date=datetime(2026, 5, 31)
    , schedule="0 */5 * * *"
    , catchup=False
    , tags=['news_notifier', 'agentic']
):
    
    call_notifier = PythonOperator(
        task_id = 'call_notifier'
        , python_callable=run_notifier
    )

    call_notifier