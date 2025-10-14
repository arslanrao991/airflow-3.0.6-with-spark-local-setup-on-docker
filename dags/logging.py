from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def failure_email_alert(context):
    """
    Callback function to send an email on task failure.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    logical_date = context['logical_date']
    log_url = context['task_instance'].log_url
    exception = context.get('exception')

    email_body = f"""
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {logical_date}
    Status: Failed

    Log URL: {log_url}

    Error Details:
    {exception if exception else 'No specific exception message available.'}
    """

    # You would typically configure the 'to' address in default_args or pass it here
    email_op = EmailOperator(
        task_id='send_failure_email',
        to='arslanrao991@gmail.com',
        subject=f'Airflow Task Failed: {dag_id}.{task_id}',
        html_content=email_body
        # conn_id='smtp_default'
    )

    email_op.execute(context)

my_dag = DAG(
    dag_id = 'task_log_email_example',
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        # 'email_on_failure': True,
        # 'email': ['arslanrao991@gmail.com'],
        'on_failure_callback': failure_email_alert
    },
    schedule = None,
    catchup = False
)

start = EmptyOperator(
    task_id = 'start',
    dag = my_dag
)

failing_task = BashOperator(
    task_id = 'failing_task',
    bash_command = 'exit 1',
    dag = my_dag
)

end = EmptyOperator(
    task_id = 'end',
    dag = my_dag
)


start >> failing_task >> end