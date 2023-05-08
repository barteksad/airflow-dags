# $AIRFLOW_HOME/config/airflow_local_settings.py

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation

from rules.tasks import TASK_RULES

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
