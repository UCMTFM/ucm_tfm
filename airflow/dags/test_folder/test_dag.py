from datetime import datetime
from pprint import pprint

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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

    # Task to print a simple message
    # print_message = BashOperator(
    #     task_id="print_message",
    #     bash_command="echo 'Airflow installation test successful!'",
    # )
    #
    # # Task to sleep for a few seconds (simulating some work)
    # sleep_task = BashOperator(
    #     task_id="sleep_for_5_seconds",
    #     bash_command="sleep 5",
    # )

    end_task = EmptyOperator(task_id="end")

    # Define the task dependencies
    start_task >> print_context() >> end_task
