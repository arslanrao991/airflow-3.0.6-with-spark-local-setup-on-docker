from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

my_dag = DAG(
    dag_id = "file_sensor",
    description = "Testing File Sensor",
    default_args = {
        "owner": "Arslan Tariq",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    catchup = False
)

start = EmptyOperator(
    task_id = "start",
    dag = my_dag
)
end = EmptyOperator(
    task_id = "end",
    dag = my_dag
)

waiting_for_file = ExternalTaskSensor(
    task_id = 'waiting_for_file',
    # file_path = "/opt/airflow/dags/sensor_testing.py",
    poke_interval = 5,
    timeout = 60*2,
    dag = my_dag,
    external_dag_id="email_operator_example",
)

start >> waiting_for_file >> end