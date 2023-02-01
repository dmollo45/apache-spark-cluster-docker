# Part 2: Spark Dataframe API
# Task 5
# Using Apache Airflow's Dummy Operator, create an 
# Airflow Dag that runs task 1, followed by tasks 2, and 3 in parallel, followed 
# by tasks 4, 5, 6 all in parallel,
#  


from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# a typical DAG
with DAG(
    dag_id='parallel_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    pass
# dummy function
def get(url: str) -> None:
   pass

# task to be run seq or parralles
task1 = PythonOperator(
    task_id='get_users',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/users'}
)
# task to be run seq or parralles
tast2 = PythonOperator(
    task_id='get_posts',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/posts'}
)
# task to be run seq or parralles
tast3 = PythonOperator(
    task_id='get_comments',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/comments'}
)
# task to be run seq or parralles
task4 = PythonOperator(
    task_id='get_todos',
    python_callable=get,
    op_kwargs={'url': 'https://gorest.co.in/public/v2/todos'}
)
# task to be run seq or parralles
task5 = BashOperator(
    task_id='start',
    bash_command='date'
)
# task to be run seq or parralles
task6 = BashOperator(
    task_id='start',
    bash_command='date'
)

# connecting the tasks as prescribed
task1 >> [task2, task3] >> [task4, task5, task6]

"""
this file is not meant to be *run 
"""

