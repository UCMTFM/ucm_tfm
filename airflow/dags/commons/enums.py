from enum import StrEnum


class AirflowConnections(StrEnum):
    DATABRICKS_CONN = "DATABRICKS_DEFAULT"


class DatabricksClusters(StrEnum):
    SERVERLESS_SQL_WH = "/sql/1.0/warehouses/8ac654e00eed9165"
    SHARED_CLUSTER = "sql/protocolv1/o/710379830264717/0811-190647-smbozjci"


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
