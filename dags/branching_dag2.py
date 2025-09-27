"""Example DAG demonstrating the usage of the `@task.branch`
TaskFlow API decorator."""

from airflow.sdk import dag, Label, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator

import random

@dag
def branch_python_operator_decorator_example():

    start = EmptyOperator(task_id="start")
    end  = EmptyOperator(task_id="end")

    options = ["branch_a", "branch_b", "branch_c", "branch_d"]

    @task.branch(task_id="branching")
    def random_choice(choices):
        return random.choice(choices)

    random_choice_instance = random_choice(choices=options)

    email_task = EmailOperator(
        task_id = "send_email",
        to = "myemail@gmail.com",
        subject = "Airflow Task Completed",
        html_content = "<p> Your task has been Successful.</p>",
        trigger_rule = "all_success"
    )

    start >> random_choice_instance

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success"
    )
    join2 = EmptyOperator(
        task_id="join2",
        trigger_rule="none_failed_min_one_success"
    )

    for option in options:

        t = EmptyOperator(
            task_id=option
        )

        empty_follow = EmptyOperator(
            task_id="follow_" + option
        )

        # Label is optional here, but it can help identify more complex branches
        random_choice_instance >> Label(option) >> t >> empty_follow >> join >> join2 >> end
        empty_follow >> email_task >> join2 >> end

branch_python_operator_decorator_example()
