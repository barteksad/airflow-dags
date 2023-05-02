from airflow.models.taskinstance import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation

def check_task_user(task: BaseOperator):
		"""Ensure Tasks have non-default owners."""
		if task.owner != 'airflow':
				raise AirflowClusterPolicyViolation(
						f"Task {task.task_id} has default owner different than 'airflow'. Task owner: {task.owner}."
				)