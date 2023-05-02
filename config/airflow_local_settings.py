# $AIRFLOW_HOME/config/airflow_local_settings.py

from typing import Callable, List

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation


def _check_task_user(task: BaseOperator):
    """Ensure Tasks have non-default owners."""
    if task.run_as_user and task.run_as_user != 'airflow':
        raise AirflowClusterPolicyViolation(
            f"Task {task.task_id} has user different than 'airflow'. Task user: {task.run_as_user}."
        )


TASK_RULES: List[Callable[[BaseOperator], None]] = [
    _check_task_user,
]


def task_policy(task: BaseOperator):
    _check_task_rules(task)


def _check_task_rules(current_task: BaseOperator):
    """Check task rules for given task."""
    notices = []
    for rule in TASK_RULES:
        try:
            rule(current_task)
        except AirflowClusterPolicyViolation as ex:
            notices.append(str(ex))
    if notices:
        notices_list = " * " + "\n * ".join(notices)
        raise AirflowClusterPolicyViolation(
            f"DAG policy violation (DAG ID: {current_task.dag_id}, Path: {current_task.dag.fileloc}):\n"
            f"Notices:\n"
            f"{notices_list}"
        )
