
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
        
    "example_dag_error",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    default_args={"run_as_user": "user1"},
)
def example_dag_1():

    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'Hello from task1!'",
        run_as_user=None,
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command="echo 'Hello from task2!'",
        run_as_user="user2",
    )

    task1 >> task2


dag = example_dag_1()
