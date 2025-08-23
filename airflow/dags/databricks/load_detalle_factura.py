import pendulum

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksTaskOperator,
)
from commons.enums import AirflowConnections

with DAG(
    dag_id="load_detalle_factura",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    tags=["databricks", "sql", "detalle_factura"],
) as dag:
    opr_run_now = DatabricksRunNowOperator(
        task_id="load_sample_table",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        job_id="1043258284634899",
        notebook_params={"dataset": "facturas", "workload": "batch"},
    )

    task_run = DatabricksTaskOperator(
        task_id="test1",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        task_config={
            "task_key": "Test Task",
            "existing_cluster_id": "0811-190647-smbozjci",
            "notebook_task": {
                "notebook_path": "/Repos/ucm_tfm/databricks_notebooks/databricks_notebooks/Bronze",
                "source": "WORKSPACE",
            },
        },
    )
