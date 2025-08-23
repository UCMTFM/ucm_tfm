from airflow.models.baseoperator import chain
import pendulum

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksTaskOperator,
)
from airflow.decorators import task_group
from commons.enums import AirflowConnections, DatabricksClusters


def load_dataset_into_bronze(dataset: str):
    return DatabricksTaskOperator(
        task_id=f"load-{dataset}-into-bronze",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        existing_cluster_id=DatabricksClusters.SHARED_CLUSTER.split("/")[-1],
        task_config={
            "notebook_task": {
                "notebook_path": "/Repos/ucm_tfm/databricks_notebooks/databricks_notebooks/Bronze",
                "source": "WORKSPACE",
                "base_parameters": {"dataset": dataset, "workload": "batch"},
            },
        },
    )


def load_dataset_into_silver(dataset: str):
    return DatabricksTaskOperator(
        task_id=f"load-{dataset}-into-silver",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        existing_cluster_id=DatabricksClusters.SHARED_CLUSTER.split("/")[-1],
        task_config={
            "notebook_task": {
                "notebook_path": "/Repos/ucm_tfm/databricks_notebooks/databricks_notebooks/Silver",
                "source": "WORKSPACE",
                "base_parameters": {"dataset": dataset},
            },
        },
    )


with DAG(
    dag_id="ingest_datasets",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    tags=["databricks", "factura", "clientes"],
) as dag:
    load_detalle_facturas = load_dataset_into_bronze("detalle_facturas")
    load_facturas = load_dataset_into_bronze("facturas")

    load_clientes = load_dataset_into_bronze("clientes")
    load_departamento = load_dataset_into_bronze("departamento")
    load_municipio = load_dataset_into_bronze("municipio")

    chain(
        [
            load_detalle_facturas >> load_facturas,
            load_clientes,
            load_departamento,
            load_municipio,
        ]
    )
