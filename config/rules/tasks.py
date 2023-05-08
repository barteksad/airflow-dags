from typing import Callable, List

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation

def check_task_user_is_same_as_dag(task: BaseOperator):
    """Ensure Tasks have non-default owners."""
    task_dag = task.get_dag()
    if task_dag:
        default_args = task_dag.default_args
        if default_args and default_args.get('run_as_user', None):
            if task.run_as_user and task.run_as_user != default_args['run_as_user']:
                raise AirflowClusterPolicyViolation(
                    f"Task {task.task_id} has user different than its dag. Task user: {task.run_as_user}. Dag user: {default_args['run_as_user']}."
                )
            
TASK_RULES: List[Callable[[BaseOperator], None]] = [
    check_task_user_is_same_as_dag,
]