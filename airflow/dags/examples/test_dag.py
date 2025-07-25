from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="airflow_installation_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    start_task = EmptyOperator(task_id="start")

    # Task to print a simple message
    print_message = BashOperator(
        task_id="print_message",
        bash_command="echo 'Airflow installation test successful!'",
    )

    # Task to sleep for a few seconds (simulating some work)
    sleep_task = BashOperator(
        task_id="sleep_for_5_seconds",
        bash_command="sleep 5",
    )

    end_task = EmptyOperator(task_id="end")

    # Define the task dependencies
    start_task >> print_message >> sleep_task >> end_task
