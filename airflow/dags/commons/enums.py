from enum import StrEnum


class AirflowConnections(StrEnum):
    DATABRICKS_CONN = "DATABRICKS_DEFAULT"


class DatabricksClusters(StrEnum):
    SERVERLESS_SQL_WH = "/sql/1.0/warehouses/03fd33f4766cd79e"
    SHARED_CLUSTER = "sql/protocolv1/o/273238931413943/0907-164510-tyg0t6hc"


class BronzeDatasets(StrEnum):
    DETALLE_FACTURAS = "detalle_facturas"
    FACTURAS = "facturas"
    MUNICIPIOS = "municipio"
    CLIENTES = "clientes"
    DEPARTAMENTOS = "departamento"


class SilverDatasets(StrEnum):
    DETALLE_FACTURAS = "detalle_facturas"
    FACTURAS = "facturas"
    DETALLE_NOTAS_CREDITO = "detalle_notas_credito"
    NOTAS_CREDITO = "notas_credito"


class GoldDatasets(StrEnum):
    DIM_CLIENTES = "dim_clientes"
    DIM_RUTAS = "dim_rutas"
    FACT_FACTURAS = "fact_facturas"
