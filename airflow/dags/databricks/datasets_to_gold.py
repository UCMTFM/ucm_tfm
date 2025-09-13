from airflow.models.baseoperator import chain
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow.decorators import task_group

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksTaskOperator,
)
from commons.enums import AirflowConnections, GoldDatasets, DatabricksClusters


def load_dataset_into_gold(dataset: GoldDatasets):
    return DatabricksTaskOperator(
        task_id=f"load-{dataset}-into-gold",
        databricks_conn_id=AirflowConnections.DATABRICKS_CONN,
        existing_cluster_id=DatabricksClusters.SHARED_CLUSTER.split("/")[-1],
        task_config={
            "notebook_task": {
                "notebook_path": "/Repos/ucm_tfm/databricks_notebooks/databricks_notebooks/Gold",
                "source": "WORKSPACE",
                "base_parameters": {"dataset": dataset},
            },
        },
    )


@task_group(group_id="load_datasets_into_bronze")
def load_datasets_into_gold():
    load_dim_cliente = load_dataset_into_gold(GoldDatasets.DIM_CLIENTES)
    load_dim_rutas = load_dataset_into_gold(GoldDatasets.DIM_RUTAS)
    load_fact_facturas = load_dataset_into_gold(GoldDatasets.FACT_FACTURAS)

    chain(
        [
            load_dim_cliente >> load_dim_rutas >> load_fact_facturas,
        ]
    )


with DAG(
    dag_id="ingest_datasets_into_gold",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    tags=["databricks", "gold"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_datasets_to_gold_dag",
        trigger_dag_id="ingest_datasets_into_gold",
        wait_for_completion=False,
    )

    chain(
        start,
        load_datasets_into_gold(),
        end,
        trigger_silver_dag,
    )
