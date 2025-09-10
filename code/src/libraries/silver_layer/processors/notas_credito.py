from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class NotasCreditoProcessor(BaseProcessor):
    """
    Processor for handling credit note records (notas de crédito) from the Bronze
    layer to the Silver layer.

    Workflow:
        1. Reads raw credit notes from the Bronze layer.
        2. Transforms fields (renames, enriches, derives dates and metadata).
        3. Reads and aggregates detalle_notas_credito (credit note line items).
        4. Classifies Type 2 credit notes based on business rules.
        5. Joins main notas_credito data with detail data and type classification.
        6. Writes the enriched dataset to the Silver layer.
        7. Updates processing metadata (last processed watermark).
    """

    def __init__(self, config_path: str):
        """
        Initialize the NotasCreditoProcessor.

        Args:
            config_path (str): Path to the JSON configuration file in DBFS.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        """
        Apply transformations to notas_credito data.

        Args:
            df (DF): Input DataFrame from the Bronze layer.

        Returns:
            DF: Transformed DataFrame ready for enrichment.
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
            .withColumnRenamed("FormaPago", "IdFormaPago")
            .withColumnRenamed("DireccionCliente", "Direccion")
            .withColumnRenamed("TelefonoCliente", "Telefono")
            .withColumnRenamed("CodigoRuta", "IdRuta")
            .withColumnRenamed("ReteIva", "ValorReteIva")
            .withColumnRenamed("Anulada", "Anulado")
            .withColumnRenamed("ComentarioAnulacion", "MotivoAnulacion")
            .withColumn("NombreNegocio", F.col("RazonSocialCliente"))
            .withColumn("Anio", F.year(F.col("Fecha")))
            .withColumn("Mes", F.month(F.col("Fecha")))
            .withColumn("Dia", F.dayofmonth(F.col("Fecha")))
            .withColumn("Prefijo", F.split(F.col("NumeroFactura"), "-").getItem(0))
            .withColumn("Consecutivo", F.split(F.col("NumeroFactura"), "-").getItem(1))
        )
        return df_transformed

    def read_detail_data(self) -> DF:
        """
        Read and aggregate detalle_notas_credito data from the Silver layer.

        Returns:
            DF: Aggregated detalle_notas_credito DataFrame grouped by IdFactura.
        """
        sources = self.config.get("sources")
        detalle_schema = sources.get("detalle_schema")
        detalle_table = sources.get("detalle_table")
        condition = self.get_condition(detalle_table)
        df_details = self.read_table(detalle_schema, detalle_table, condition)
        df_details_grouped = df_details.groupBy("IdFactura").agg(
            F.sum("Devolucion").alias("Devolucion"),
            F.sum("Rotacion").alias("Rotacion"),
            F.sum("DevolucionesRotaciones").alias("CantidadDevolucionesRotaciones"),
            F.sum("SubtotalDevoluciones").alias("SubtotalDevoluciones"),
            F.sum("ValorDescuentoDevoluciones").alias("ValorDescuentoDevoluciones"),
            F.sum("ValorImpuestoSaludableDevoluciones").alias("ValorImpuestoSaludableDevoluciones"),
            F.sum("TotalDevoluciones").alias("TotalDevoluciones"),
        )
        logger.info("Detail data computed successfully")
        return df_details_grouped

    def get_type_2(self) -> DF:
        """
        Identify and classify Type 2 notas_credito records.

        Business rule:
            - A credit note is Type 2 if all invoice detail rows have:
                Cantidad == Devolucion + Rotacion.
            - Validates this condition by comparing aggregated totals at the
              factura (invoice) level.

        Returns:
            DF: DataFrame with IdFactura classified as Type 2.
        """
        logger.info("Classifying Type 2 NotasCredito dataset")
        sources = self.config.get("sources")
        detalle_schema = sources.get("detalle_facturas_schema")
        detalle_table = sources.get("detalle_facturas_table")
        condition = self.get_condition(detalle_table)
        df_det = self.read_table(detalle_schema, detalle_table, condition)
        df_type_2_candidates = (
            df_det.filter(F.col("Cantidad") == F.col("Devolucion") + F.col("Rotacion"))
            .groupBy("IdFactura")
            .agg(
                F.sum("Cantidad").alias("TotalCantidad"),
                F.sum("Devolucion").alias("TotalDevolucion"),
                F.sum("Rotacion").alias("TotalRotacion"),
            )
            .withColumn(
                "TotalDevolucionRotacion", F.col("TotalDevolucion") + F.col("TotalRotacion")
            )
            .select("IdFactura", "TotalCantidad", "TotalDevolucionRotacion")
        )
        df_det_by_fact = (
            df_det.groupBy("IdFactura")
            .agg(
                F.sum("Cantidad").alias("TotalCantidadFactura"),
                F.sum("Devolucion").alias("TotalDevolucionFactura"),
                F.sum("Rotacion").alias("TotalRotacionFactura"),
            )
            .withColumn(
                "TotalDevolucionRotacionFactura",
                F.col("TotalDevolucionFactura") + F.col("TotalRotacionFactura"),
            )
            .select("IdFactura", "TotalCantidadFactura", "TotalDevolucionRotacionFactura")
        )

        df_type_2_result = (
            df_det_by_fact.join(df_type_2_candidates, on="IdFactura", how="inner")
            .filter(
                (F.col("TotalCantidadFactura") == F.col("TotalCantidad"))
                & (F.col("TotalDevolucionRotacionFactura") == F.col("TotalDevolucionRotacion"))
            )
            .select("IdFactura")
            .withColumn("Tipo", F.lit(2))
        )
        return df_type_2_result

    @staticmethod
    def add_detail_data(df: DF, df_details: DF, df_type_2: DF) -> DF:
        """
        Join notas_credito with aggregated detalle_notas_credito and classification results.

        - Left join notas_credito with detalle_notas_credito aggregates.
        - Inner join with Type 2 classification results.
        - Derive Tipo column:
            * If Tipo is null → assign default value 5.
            * Else → preserve Tipo = 2.

        Args:
            df (DF): Transformed notas_credito DataFrame.
            df_details (DF): Aggregated detalle_notas_credito DataFrame.
            df_type_2 (DF): DataFrame of Type 2 classified notas_credito.

        Returns:
            DF: Joined and enriched DataFrame with Tipo classification.
        """
        logger.info("Joining notas_credito with detalle_notas_credito data")
        df_joined_det = df.join(df_details, on="IdFactura", how="left")
        df_classified = df_joined_det.join(df_type_2, on="IdFactura", how="inner").withColumn(
            "Tipo", F.when(F.col("Tipo").isNull(), F.lit(5)).otherwise(F.col("Tipo"))
        )
        return df_classified

    def process(self):
        """
        Orchestrate the Bronze → Silver pipeline for notas_credito.

        1. Read notas_credito from Bronze.
        2. Read and aggregate detalle_notas_credito from Silver.
        3. Transform main notas_credito fields.
        4. Compute Type 2 classification.
        5. Join notas_credito with detail data and classification.
        6. Write enriched dataset to Silver Delta table.
        7. Update last processed watermark for incremental loads.

        Returns:
            None
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df_notas_credito = self.read_bronze_table()
        df_detail = self.read_detail_data()
        df_transformed = self.transformations(df_notas_credito)
        df_type_2 = self.get_type_2()
        df_result = NotasCreditoProcessor.add_detail_data(df_transformed, df_detail, df_type_2)
        self.write_delta_table(df_result)
        self.update_last_processed(df_result)
