import json
import os
from abc import ABC, abstractmethod

from loguru import logger
from pyspark.dbutils import DBUtils

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BaseIngestor(ABC):
    """
    Abstract base class for implementing data ingestion workflows using PySpark
    in a Databricks + Azure Data Lake environment.

    This class handles:
      - Loading configuration from a JSON file stored in DBFS.
      - Establishing authenticated connections to Azure Data Lake Storage
        using Databricks secrets.
      - Providing helper methods to grant schema/table permissions and create
        Delta tables.

    Subclasses must implement the abstract :meth:`ingest` method to define the
    ingestion logic.

    Attributes:
        spark (SparkSession): The active Spark session.
        dbutils (DBUtils): Databricks utilities object for interacting with DBFS and secrets.
        config (dict): Configuration loaded from the provided JSON file.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the BaseIngestor instance.

        Args:
            config_path (str): Path to the JSON configuration file in DBFS.
                               Example: "dbfs:/mnt/config/ingestion_config.json"
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(self.spark)

        cfg_str = self.dbutils.fs.head(config_path, 10_000_000)
        self.config = json.loads(cfg_str)
        self.connect_to_storage_accounts()

    @abstractmethod
    def ingest(self):
        """
        Abstract method to be implemented by subclasses to perform the actual
        ingestion logic.
        """
        pass

    def connect_to_storage_accounts(self):
        """
        Establish authenticated connections to Azure Data Lake Storage accounts
        defined in the configuration file.

        Retrieves account keys from Databricks secret scopes and sets Spark
        configuration values to allow access to:
          - Lakehouse storage account.
          - Landing storage account.
        """
        secret_scope = self.config.get("secret_scope")

        lkh_account_name = self.config.get("lakehouse_storage_account_name")
        lkh_secret_key_name = self.config.get("lakehouse_secret_key_name")
        lkh_account_key = self.dbutils.secrets.get(scope=secret_scope, key=lkh_secret_key_name)

        lnd_account_name = self.config.get("landing_storage_account_name")
        lnd_secret_key_name = self.config.get("landing_secret_key_name")
        lnd_account_key = self.dbutils.secrets.get(scope=secret_scope, key=lnd_secret_key_name)

        self.spark.conf.set(
            f"fs.azure.account.key.{lkh_account_name}.dfs.core.windows.net", lkh_account_key
        )
        self.spark.conf.set(
            f"fs.azure.account.key.{lnd_account_name}.dfs.core.windows.net", lnd_account_key
        )

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
            schema (str): Name of the schema.
            catalog (str): Name of the catalog.
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

    def create_table(self, dataset: str, location: str) -> None:
        """
        Creates an external Delta table in the ``bronze`` schema if it does not already exist.

        Args:
            dataset (str): Name of the dataset/table (e.g., "sales_data").
            location (str): Path to the dataset storage location in the Lakehouse
        """
        catalog = self.config.get("catalog", "hive_metastore")
        self.spark.sql(f"USE CATALOG {catalog}")
        if not self.spark.catalog.tableExists(f"bronze.{dataset}"):
            logger.info(f"Creating external table bronze.{dataset}")
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
            self.grant_permissions("bronze", catalog)

            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS bronze.{dataset}
                USING DELTA
                LOCATION '{location}'"""
            )
            logger.info("Table successfully created")
