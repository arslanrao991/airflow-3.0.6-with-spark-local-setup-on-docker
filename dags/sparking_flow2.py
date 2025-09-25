from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="sparking_flow2",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="start")
