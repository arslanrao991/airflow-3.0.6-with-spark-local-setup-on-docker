from airflow import DAG
from airflow.operators.python import PythonOperator
from github_plugin import GithubFileOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import requests
from datetime import timedelta

my_dag = DAG(
    dag_id = 'github_connection',
    default_args = {
        'owner': 'Arslan Tariq',
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    schedule = None,
    catchup = False
)


start = EmptyOperator(
    task_id = 'start',
    dag = my_dag
)

end = EmptyOperator(
    task_id = 'end',
    dag = my_dag
)

fetch_file = GithubFileOperator(
    task_id='fetch_github_file',
    github_conn_id='github-default',
    repo_name='airflow-3.0.6-with-spark-local-setup-on-docker',
    file_path='Dockerfile.spark',
    ref='main'
)

start >> fetch_file >> end
