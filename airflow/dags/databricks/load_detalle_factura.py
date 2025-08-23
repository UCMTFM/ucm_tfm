import pendulum

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksTaskOperator,
)
from commons.enums import AirflowConnections, DatabricksClusters

with DAG(
    dag_id="load_detalle_factura",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    tags=["databricks", "sql", "detalle_factura"],
) as dag:
    # opr_run_now = DatabricksRunNowOperator(
    #     task_id="load_sample_table",
    #     databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
    #     job_id="1043258284634899",
    #     notebook_params={"dataset": "facturas", "workload": "batch"},
    # )

    load_facturas_into_bronze = DatabricksTaskOperator(
        task_id="load-facturas-into-bronze",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        existing_cluster_id=DatabricksClusters.SHARED_CLUSTER.split("/")[-1],
        task_config={
            "notebook_task": {
                "notebook_path": "/Repos/ucm_tfm/databricks_notebooks/databricks_notebooks/Bronze",
                "source": "WORKSPACE",
                "base_parameters": {"dataset": "facturas", "workload": "batch"},
            },
        },
    )

    load_facturas_into_silver = DatabricksTaskOperator(
        task_id="load-facturas-into-silver",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        existing_cluster_id=DatabricksClusters.SHARED_CLUSTER.split("/")[-1],
        task_config={
            "notebook_task": {
                "notebook_path": "/Repos/ucm_tfm/databricks_notebooks/databricks_notebooks/Silver",
                "source": "WORKSPACE",
                "base_parameters": {"dataset": "facturas", "workload": "batch"},
            },
        },
    )
