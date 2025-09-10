from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class FacturasProcessor(BaseProcessor):
    """
    Processor for handling invoice records (facturas) from the Bronze layer to the Silver layer.

    Workflow:
        1. Reads invoice data from the Bronze layer.
        2. Applies imputations and filters invalid records.
        3. Transforms columns (renames, derives fields such as date parts, prefixes, etc.).
        4. Reads and aggregates detalle_facturas (invoice line items) from the Silver layer.
        5. Joins aggregated detail data with the main invoice dataset.
        6. Writes the enriched dataset to the Silver layer.
        7. Updates processing metadata (last processed watermark).

    Inherits from:
        BaseProcessor: Provides utilities for reading/writing Delta tables,
        managing incremental loads, imputations, and metadata updates.
    """

    def __init__(self, config_path: str):
        """
        Initialize the FacturasProcessor.

        Args:
            config_path (str): Path to the JSON configuration file in DBFS.
        """
        super().__init__(config_path)

    def imputations(self, df: DF) -> DF:
        """
        Apply imputations and filter invalid facturas records.

        Args:
            df (DF): Input DataFrame containing raw facturas data.

        Returns:
            DF: Cleaned DataFrame after imputations.
        """
        logger.info("Starting imputations on facturas data")
        df_cleaned = df.filter(F.col("IdFactura").isNotNull())

        imputation_config = self.config.get("imputations", {})
        for column, value in imputation_config.items():
            df_cleaned = df_cleaned.withColumn(
                column, F.when(F.col(column).isNull(), value).otherwise(F.col(column))
            )

        logger.info("Imputations completed successfully")
        return df_cleaned

    def transformations(self, df: DF) -> DF:
        """
        Transform the facturas data by renaming, enriching, and deriving fields.

        Args:
            df (DF): DataFrame after imputations.

        Returns:
            DF: Transformed facturas DataFrame.
        """
        logger.info("Starting transformations on facturas data")
        df_transformed = (
            df.select(
                "IdFactura",
                "NumeroFactura",
                "Fecha",
                "IdentificacionCliente",
                "Latitud",
                "Longitud",
                "RazonSocialCliente",
                "Retefuente",
                "FechaVencimiento",
                "FormaPago",
                "IdResolucion",
                "DireccionCliente",
                "TelefonoCliente",
                "Saldo",
                "CodigoRuta",
                "ReteIva",
                "IdEmpresa",
                "FechaCreacion",
                "Cufe",
                "Anulada",
                "UsuarioAnulacion",
                "FechaAnulacion",
                "ComentarioAnulacion",
                "_ingestion_time",
            )
            .withColumnRenamed("IdentificacionCliente", "Identificacion")
            .withColumnRenamed("Retefuente", "ValorRetencion")
            .withColumnRenamed("FormaPago", "IdFormaPago")
            .withColumnRenamed("DireccionCliente", "Direccion")
            .withColumnRenamed("TelefonoCliente", "Telefono")
            .withColumnRenamed("CodigoRuta", "IdRuta")
            .withColumnRenamed("ReteIva", "ValorReteIva")
            .withColumnRenamed("Anulada", "Anulado")
            .withColumnRenamed("ComentarioAnulacion", "MotivoAnulacion")
            .withColumn("NombreNegocio", F.col("RazonSocialCliente"))
            .withColumn("Tipo", F.lit(1))
            .withColumn("Anio", F.year(F.col("Fecha")))
            .withColumn("Mes", F.month(F.col("Fecha")))
            .withColumn("Dia", F.dayofmonth(F.col("Fecha")))
            .withColumn("Prefijo", F.split(F.col("NumeroFactura"), "-").getItem(0))
            .withColumn("Consecutivo", F.split(F.col("NumeroFactura"), "-").getItem(1))
        )
        return df_transformed

    def read_detail_data(self) -> DF:
        """
        Read and aggregate the detalle_facturas (invoice line items) from the Silver layer.

        Returns:
            DF: Aggregated detalle_facturas DataFrame grouped by invoice ID.
        """
        sources = self.config.get("sources")
        detalle_schema = sources.get("detalle_schema")
        detalle_table = sources.get("detalle_table")
        condition = self.get_condition(detalle_table)
        df_details = self.read_table(detalle_schema, detalle_table, condition)
        df_details_grouped = df_details.groupBy("IdFactura").agg(
            F.sum("Subtotal").alias("Subtotal"),
            F.sum("Total").alias("Total"),
            F.sum("ValorDescuento").alias("Descuento"),
            F.sum("ValorIva").alias("Iva"),
            F.sum("Cantidad").alias("Cantidad"),
            F.sum("ValorImpuestoSaludable").alias("ImpuestoSaludable"),
        )
        logger.info("Detail data computed successfully")
        return df_details_grouped

    @staticmethod
    def add_detail_data(df: DF, df_details: DF) -> DF:
        """
        Join main facturas data with aggregated detalle_facturas data.

        Args:
            df (DF): Main facturas DataFrame (transformed).
            df_details (DF): Aggregated detalle_facturas DataFrame.

        Returns:
            DF: Joined DataFrame enriched with detail totals and retention percentage.
        """
        logger.info("Joining facturas with detalle_facturas data")
        df_joined = df.join(df_details, on="IdFactura", how="inner").withColumn(
            "PorcentajeRetencion", F.round((F.col("ValorRetencion") * 100) / F.col("Subtotal"), 2)
        )
        return df_joined

    def process(self):
        """
        Orchestrate the Bronze → Silver pipeline for facturas.

        1. Read Bronze facturas table.
        2. Read and aggregate Silver detalle_facturas data.
        3. Apply imputations to Bronze facturas.
        4. Transform facturas data (rename, enrich, derive fields).
        5. Join facturas with aggregated detail data.
        6. Write enriched dataset to Silver Delta table.
        7. Update last processed watermark for incremental loading.
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df_fact = self.read_bronze_table()
        df_detail = self.read_detail_data()
        df_imputed = self.imputations(df_fact)
        df_transformed = self.transformations(df_imputed)
        df_result = FacturasProcessor.add_detail_data(df_transformed, df_detail)
        self.write_delta_table(df_result)
        self.update_last_processed(df_result)
