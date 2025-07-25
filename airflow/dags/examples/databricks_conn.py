from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSQLOperator

with DAG(
    dag_id="databricks_sql_warehouse_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["databricks", "sql"],
) as dag:
    create_table = DatabricksSQLOperator(
        task_id="show_tables",
        databricks_conn_id="DATABRICKS_DEFAULT",
        sql="""
           SHOW TABLES IN bronze; 
        """,
        do_xcom_push=True,
    )
