# $AIRFLOW_HOME/config/airflow_local_settings.py

from typing import Callable

from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation

from rules.task_user import check_task_user

TASK_RULES: list[Callable[[BaseOperator], None]] = [
  check_task_user,
]

def example_task_policy(task: BaseOperator):
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