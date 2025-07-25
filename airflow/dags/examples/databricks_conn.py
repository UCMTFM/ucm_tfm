from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from commons.enums import DatabricksClusters, AirflowConnections

with DAG(
    dag_id="databricks_sql_warehouse_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    tags=["databricks", "sql"],
) as dag:
    create_table = DatabricksSqlOperator(
        task_id="show_tables",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        sql="SHOW TABLES",
        catalog="adbtfmappinovalakehouse",
        schema="bronze",
        http_path=DatabricksClusters.SERVERLESS_SQL_WH,
    )
