from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def wait():
    time.sleep(30)
    return

my_dag = DAG(
    dag_id = 'email_operator_example',
    default_args = {
        "owner": "Arslan Tariq"
    },
    catchup=False

)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=my_dag,
)

skeep_wait = PythonOperator(
    task_id = "wait",
    python_callable = wait,
    dag = my_dag
)

email_task = EmailOperator(
    task_id = "send_email",
    to = "arslanrao991@gmail.com",
    subject = "Airflow Task Completed",
    html_content = "<p> Your task has been completed.</p>",
    dag=my_dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=my_dag,
)

start >> skeep_wait >> email_task >> end