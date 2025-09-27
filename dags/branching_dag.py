from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


def chooseBranch(**kwargs):
    return 'branch_a'


my_dag = DAG(
    dag_id = 'branching_dag',
    description = 'Test Brancing',
    default_args = {
        "owner": "Arslan Tariq",
    },
    catchup = False
)

start = EmptyOperator(
    task_id = 'start',
    dag = my_dag
)


branching = BranchPythonOperator(
    task_id = 'branching',
    python_callable = chooseBranch,
    dag = my_dag
)

branch_a = EmptyOperator(
    task_id = 'branch_a',
    dag = my_dag
)
branch_b = EmptyOperator(
    task_id = 'branch_b',
    dag = my_dag
)

branch_c = PythonOperator(
    task_id = 'branch_c',
    python_callable = lambda: print(f"Maintenance Window: {Variable.get('MAINTENANCE_WINDOW')}")
)

join = EmptyOperator(
    task_id = 'join',
    trigger_rule = 'none_failed_min_one_success',
    dag = my_dag
)

end = EmptyOperator(
    task_id = 'end',
    dag = my_dag
)

start >> branching >> [branch_a, branch_b] >> join >> end
start >> branch_c >> end


