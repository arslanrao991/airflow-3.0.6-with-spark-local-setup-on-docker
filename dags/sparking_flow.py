from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Arslan Tariq",
        "start_date": datetime(2023, 1, 1),  # ✅ fixed
    },
    schedule="@daily",
    catchup=False,  # ✅ prevents backfilling old runs
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag,
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="/opt/airflow/jobs/python/wordcountjob.py",
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

start >> python_job >> end
