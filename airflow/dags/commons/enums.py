from enum import StrEnum


class AirflowConnections(StrEnum):
    DATABRICKS_CONN = "DATABRICKS_DEFAULT"


class DatabricksClusters(StrEnum):
    SERVERLESS_SQL_WH = "/sql/1.0/warehouses/8ac654e00eed9165"
    SHARED_CLUSTER = "sql/protocolv1/o/710379830264717/0811-190647-smbozjci"
