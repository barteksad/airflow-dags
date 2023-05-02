
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "example_dag_1",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
				"owner": "airflow",
		},
) as dag:
    
		task1 = BashOperator(
				task_id="task1",
				bash_command="echo 'Hello from task1!'",
				run_as_user="airflow",
		)

		task2 = BashOperator(
				task_id="task2",
				bash_command="echo 'Hello from task2!'",
				run_as_user="not_airflow",
		)

		task1 >> task2

