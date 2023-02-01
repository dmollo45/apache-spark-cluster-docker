import json
import time
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='parallel_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    pass

def get(url: str) -> None:
   pass


task1 = PythonOperator(
    task_id='get_users',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/users'}
)

tast2 = PythonOperator(
    task_id='get_posts',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/posts'}
)

tast3 = PythonOperator(
    task_id='get_comments',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/comments'}
)

task4 = PythonOperator(
    task_id='get_todos',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/todos'}
)

task5 = BashOperator(
    task_id='start',
    bash_command='date'
)

task6 = BashOperator(
    task_id='start',
    bash_command='date'
)


task1 >> [task2, task3] >> [task4, task5, task6]

