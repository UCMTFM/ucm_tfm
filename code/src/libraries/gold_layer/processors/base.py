import json
import os
from abc import ABC, abstractmethod

from loguru import logger
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame as DF

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BaseProcessor(ABC):
    """
    Abstract base class for implementing data processing workflows using PySpark
    in a Databricks + Azure Data Lake environment.

    This class handles:
      - Loading configuration from a JSON file stored in DBFS.
      - Establishing authenticated connections to Azure Data Lake Storage using
        Databricks secrets.
      - Providing helper methods for reading tables from catalogs and schemas.
      - Granting schema/table permissions to users.

    Subclasses must implement the abstract :meth:`process` method to define
    the specific processing workflow.

    Attributes:
        spark (SparkSession): The active Spark session.
        config_path (str): Path to the JSON configuration file.
        dbutils (DBUtils): Databricks utilities object for DBFS and secrets.
        config (dict): Configuration loaded from the JSON file.
    """

    def __init__(self, config_path: str):
        """
        Initialize the processor and load the configuration.

        Args:
            config_path (str): Path to the JSON config file.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.config_path = config_path
        self.dbutils = DBUtils(self.spark)

        cfg_str = self.dbutils.fs.head(config_path, 10_000_000)
        self.config = json.loads(cfg_str)
        self.connect_to_storage_account()

    @abstractmethod
    def process(self):
        """
        Abstract method to be implemented by subclasses to define the data
        processing workflow.
        """
        pass

    def connect_to_storage_account(self) -> None:
        """
        Configure Spark to access Azure Data Lake Storage using secrets
        stored in Databricks secret scope.
        """
        storage_account_name = self.config.get("lakehouse_storage_account_name")
        secret_scope = self.config.get("secret_scope")
        secret_key_name = self.config.get("lakehouse_secret_key_name")
        storage_account_key = self.dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
        self.spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key
        )

    def read_table(self, schema: str, table: str) -> DF:
        """
        Read a table from the configured catalog and return it as a Spark DataFrame.

        Args:
            schema (str): Schema name (e.g., "silver", "bronze", "gold").
            table (str): Table name within the schema.

        Returns:
            DF: A Spark DataFrame containing the table data.
        """
        catalog = self.config.get("catalog")
        self.spark.sql(f"USE CATALOG {catalog}")

        df = self.spark.table(f"{schema}.{table}")

        logger.info(f"Data from {schema}.{table} read successfully")
        return df

    def read_silver_table(self, table_name: str) -> DF:
        """
        Convenience method for reading a table from the ``silver`` schema.

        Args:
            table_name (str): Table name within the ``silver`` schema.

        Returns:
            DF: A Spark DataFrame with the contents of the table.
        """
        schema = "silver"
        return self.read_table(schema, table_name)

    def grant_permissions(self, schema: str, catalog: str) -> None:
        """
        Grants standard permissions on a given schema within a catalog to a
        list of users defined in the environment variable ``USERS``.

        Granted permissions:
            - USE CATALOG
            - USE SCHEMA
            - SELECT
            - CREATE
            - MODIFY

        Args:
            schema (str): Name of the schema (e.g., "silver").
            catalog (str): Name of the catalog (e.g., "hive_metastore").
        """
        users = os.environ.get("USERS").split(",")
        for user in users:
            try:
                self.spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{user}`;")
                self.spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{user}`;")
                self.spark.sql(f"GRANT SELECT ON SCHEMA {schema} TO `{user}`;")
                self.spark.sql(f"GRANT CREATE ON SCHEMA {schema} TO `{user}`;")
                self.spark.sql(f"GRANT MODIFY ON SCHEMA {schema} TO `{user}`;")
            except Exception:
                pass

    def write_delta_table(self, df: DF) -> None:
        """
        Write the processed DataFrame to the silver Delta table.

        The table is partitioned by year.

        Args:
            df (DataFrame): Processed DataFrame to write.
        """
        account = self.config.get("lakehouse_storage_account_name")
        lkh_container_name = self.config.get("lakehouse_container_name")
        gold_path = f"abfss://{lkh_container_name}@{account}.dfs.core.windows.net/gold"
        datasource = self.config.get("datasource")
        dataset = self.config.get("dataset")
        location = f"{gold_path}/{datasource}/{dataset}"

        sink_config = self.config.get("sink")
        schema = sink_config.get("schema")
        table = sink_config.get("table")
        logger.info(f"Uploading external table {schema}.{table} in '{location}'")

        catalog = self.config.get("catalog")
        self.spark.sql(f"USE CATALOG {catalog}")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold MANAGED LOCATION '{gold_path}'")

        self.grant_permissions("gold", catalog)

        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("Anio")
            .option("path", location)
            .option("mergeSchema", "true")
            .saveAsTable(f"{schema}.{table}", overwrite=True)
        )
        logger.info("Gold data written successfully")
