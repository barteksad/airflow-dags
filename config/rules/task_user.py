from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation

def check_task_user(task: BaseOperator):
		"""Ensure Tasks have non-default owners."""
		if task.run_as_user and task.run_as_user != 'airflow':
				raise AirflowClusterPolicyViolation(
						f"Task {task.task_id} has user different than 'airflow'. Task user: {task.run_as_user}."
				)