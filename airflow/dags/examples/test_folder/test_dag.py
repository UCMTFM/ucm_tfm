from datetime import datetime
from pprint import pprint

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.decorators import task


@task(task_id="print_the_context")
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return "Whatever you return gets printed in the logs"


with DAG(
    dag_id="airflow_installation_test_inside_folder",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    # Define the task dependencies
    start_task >> print_context() >> end_task
