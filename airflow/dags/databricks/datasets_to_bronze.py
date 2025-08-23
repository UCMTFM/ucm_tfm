from airflow.models.baseoperator import chain
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksTaskOperator,
)
from commons.enums import AirflowConnections, BronzeDatasets, DatabricksClusters


def load_dataset_into_bronze(dataset: BronzeDatasets):
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


with DAG(
    dag_id="ingest_datasets_into_bronze",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    tags=["databricks", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_detalle_facturas = load_dataset_into_bronze(BronzeDatasets.DETALLE_FACTURAS)
    load_facturas = load_dataset_into_bronze(BronzeDatasets.FACTURAS)

    # load_clientes = load_dataset_into_bronze(BronzeDatasets.CLIENTES)
    # load_departamento = load_dataset_into_bronze(BronzeDatasets.DEPARTAMENTOS)
    # load_municipio = load_dataset_into_bronze(BronzeDatasets.MUNICIPIOS)

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_datasets_to_silver_dag",
        trigger_dag_id="datasets_to_silver",
        wait_for_completion=False,
    )

    chain(
        start,
        [
            load_detalle_facturas >> load_facturas,
            # load_clientes,
            # load_departamento,
            # load_municipio,
        ],
        end,
        trigger_silver_dag,
    )
