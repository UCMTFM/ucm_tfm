from airflow.models.baseoperator import chain
import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksTaskOperator,
)
from commons.enums import AirflowConnections, DatabricksClusters, SilverDatasets


def load_dataset_into_silver(dataset: SilverDatasets):
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
    dag_id="ingest_datasets_into_silver",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    tags=["databricks", "silver"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_detalle_facturas = load_dataset_into_silver(SilverDatasets.DETALLE_FACTURAS)
    load_facturas = load_dataset_into_silver(SilverDatasets.FACTURAS)

    load_detalle_notas_credito = load_dataset_into_silver(
        SilverDatasets.DETALLE_NOTAS_CREDITO
    )
    load_notas_credito = load_dataset_into_silver(SilverDatasets.NOTAS_CREDITO)

    chain(
        start,
        [
            load_detalle_facturas >> load_facturas,
            load_notas_credito >> load_detalle_notas_credito,
        ],
        end,
    )
