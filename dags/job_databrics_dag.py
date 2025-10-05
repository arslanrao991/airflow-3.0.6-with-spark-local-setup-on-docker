from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("Asia/Karachi")


my_dag = DAG(
    dag_id = 'Job_Pipeline',
    default_args = {
        'owner': 'Arslan'
    },
    schedule = None,
    start_date = datetime(2025, 1, 1, tzinfo=local_tz),
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

trigger_databricks_job = DatabricksRunNowOperator(
    task_id = 'trigger_job',
    databricks_conn_id = 'databricks-default',
    job_id = '41451491860381',
    dag = my_dag
)



start >> trigger_databricks_job >> end