import json
import os
import re
from abc import ABC, abstractmethod

from delta.tables import DeltaTable
from loguru import logger
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F
from pyspark.sql.column import Column

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BaseProcessor(ABC):
    """
    Abstract base class for implementing data processing logic on Delta Lake tables
    using PySpark.

    This class provides utility methods for:
    - Reading data from a bronze table with optional incremental filtering
    - Enriching and transforming data
    - Writing processed data to a silver Delta table
    - Maintaining metadata such as last processed timestamps

    Subclasses must implement the `process` method to define specific transformation logic.

    Attributes:
        spark (SparkSession): Active Spark session for processing data.
        config (dict): Configuration dictionary loaded from a JSON file.
        config_path (str): Path to the configuration file, used for updating metadata.
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

    def imputations(self, df: DF) -> DF:
        logger.info("Starting imputations on data")
        imputation_config = self.config.get("imputations", {})
        for column, value in imputation_config.items():
            df = df.withColumn(
                column, F.when(F.col(column).isNull(), value).otherwise(F.col(column))
            )
        logger.info("Imputations completed successfully")
        return df

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

    def read_table(self, schema: str, table: str, condition: Column = F.lit(True)) -> DF:
        """
        Read data from the bronze table, optionally filtered by the last processed timestamp.

        Returns:
            DataFrame: The loaded data from the bronze layer.
        """
        catalog = self.config.get("catalog")
        self.spark.sql(f"USE CATALOG {catalog}")

        pattern = r"'([^']*)'"
        condition_str = re.findall(pattern, str(condition))[0]
        logger.info(
            f"Reading data of table {schema}.{table} that met the condition '{condition_str}'"
        )
        df = self.spark.table(f"{schema}.{table}").filter(condition)

        logger.info("Data read successfully")
        return df

    def get_condition(self, table_name: str) -> Column:
        """
        Get the filter condition for the DataFrame.

        Returns:
            Column: The filter condition based on the last processed date.
        """
        last_ingest_processed = self.config.get("last_ingest_processed_date", "")
        dataset = self.config.get("dataset")
        if last_ingest_processed:
            if "facturas" in dataset and "detalle" in table_name:
                condition = (
                    F.col("_ingestion_time") > F.lit(last_ingest_processed).cast("timestamp")
                ) & (F.col("Cantidad") > 0)
            elif "notas_credito" in dataset and "detalle" in table_name:
                condition = (
                    F.col("_ingestion_time") > F.lit(last_ingest_processed).cast("timestamp")
                ) & (F.col("Devolucion") + F.col("Rotacion") > 0)
            else:
                condition = F.col("_ingestion_time") > F.lit(last_ingest_processed).cast(
                    "timestamp"
                )
        else:
            if "facturas" in dataset:
                condition = F.col("Cantidad") > 0
            elif "notas_credito" in dataset:
                condition = F.col("Devolucion") + F.col("Rotacion") > 0
            else:
                condition = F.lit(True)
        return condition

    def read_bronze_table(self) -> DF:
        source_config = self.config.get("sources")
        schema = source_config.get("main_schema")
        table = source_config.get("main_table")
        condition = self.get_condition(table)
        return self.read_table(schema, table, condition)

    def grant_permissions(self, schema: str, catalog: str) -> None:
        users = os.environ.get("USERS").split(",")
        for user in users:
            try:
                self.spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{user}`;")
                self.spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{user}`;")
            except Exception:
                pass

    def write_delta_table(self, df: DF) -> None:
        """
        Write the processed DataFrame to the silver Delta table.

        The table is partitioned by year, month, and day.

        Args:
            df (DataFrame): Processed DataFrame to write.
        """
        account = self.config.get("lakehouse_storage_account_name")
        lkh_container_name = self.config.get("lakehouse_container_name")
        silver_path = f"abfss://{lkh_container_name}@{account}.dfs.core.windows.net/silver"
        datasource = self.config.get("datasource")
        dataset = self.config.get("dataset")
        location = f"{silver_path}/{datasource}/{dataset}"

        sink_config = self.config.get("sink")
        schema = sink_config.get("schema")
        table = sink_config.get("table")
        logger.info(f"Uploading external table {schema}.{table} in '{location}'")

        catalog = self.config.get("catalog")
        self.spark.sql(f"USE CATALOG {catalog}")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver MANAGED LOCATION '{silver_path}'")
        self.grant_permissions("silver", catalog)

        table_exists = self.spark.catalog.tableExists(f"{schema}.{table}")
        if table_exists:
            delta_table = DeltaTable.forName(self.spark, f"{schema}.{table}")

            key = "IdDetalle" if "detalle" in table else "IdFactura"
            (
                delta_table.alias("target")
                .merge(df.alias("source"), f"target.{key} = source.{key}")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            last_op = delta_table.history(1).collect()[0]
            metrics = last_op.operationMetrics
            logger.info(f"Inserted records: {metrics['numTargetRowsInserted']}")
            logger.info(f"Updated records: {metrics['numTargetRowsUpdated']}")
        else:
            (
                df.write.format("delta")
                .mode("append")
                .partitionBy("Anio")
                .option("path", location)
                .option("mergeSchema", "true")
                .saveAsTable(f"{schema}.{table}", overwrite=False)
            )
        logger.info("Silver data written successfully")

    def update_last_processed(self, df: DF) -> None:
        """
        Update the `last_ingest_processed_date` in the config file with the latest
        `_ingestion_time` from the processed DataFrame.

        Args:
            df (DataFrame): DataFrame that was processed.
        """
        logger.info("Updating last processed data")
        if not df.isEmpty():
            max_timestamp_processed = df.select(F.max("_ingestion_time").cast("string")).first()[0]
            self.config["last_ingest_processed_date"] = max_timestamp_processed
            self.dbutils.fs.put(self.config_path, json.dumps(self.config, indent=4), overwrite=True)
