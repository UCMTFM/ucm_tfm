from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class DetalleNotasCreditoProcessor(BaseProcessor):
    """
    Processor for handling credit note detail records (notas de crédito detalle)
    from the Bronze layer to the Silver layer.

    Workflow:
        1. Reads raw Bronze data incrementally.
        2. Applies imputations based on configuration.
        3. Transforms fields to compute devolutions/rotations, discounts,
           taxes, and totals.
        4. Writes the transformed dataset to the Silver layer.
        5. Updates processing metadata (last processed watermark).
    """

    def __init__(self, config_path: str):
        """
        Initialize the DetalleNotasCreditoProcessor instance.

        Args:
            config_path (str): Path to the JSON configuration file in DBFS.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        """
        Apply transformations to credit note detail data.

        Args:
            df (DF): Input DataFrame from Bronze layer after imputations.

        Returns:
            DF: Transformed DataFrame ready for Silver layer.
        """
        df_transformed = (
            df.select(
                "IdDetalle",
                "IdFactura",
                "IdProducto",
                "NombreProducto",
                "Devolucion",
                "Rotacion",
                "ValorUnitario",
                "Descuento",
                "PorcentajeIva",
                "ISPPorcentaje",
                "FechaCreacion",
                "Subtotal",
                "_ingestion_time",
            )
            .withColumnRenamed("ISPPorcentaje", "PorcentajeImpuestoSaludable")
            .withColumn(
                "DevolucionesRotaciones",
                F.col("Devolucion") + F.col("Rotacion"),
            )
            .withColumn("Anio", F.year(F.col("FechaCreacion")))
            .withColumn("Mes", F.month(F.col("FechaCreacion")))
            .withColumn("Dia", F.dayofmonth(F.col("FechaCreacion")))
            .withColumn(
                "SubtotalDevoluciones",
                F.round(F.col("ValorUnitario") * F.col("DevolucionesRotaciones"), 2),
            )
            .withColumn(
                "PorcentajeDescuento",
                F.when(
                    F.col("Subtotal") > 0, (F.col("Descuento") * 100) / F.col("Subtotal")
                ).otherwise(0),
            )
            .withColumn(
                "ValorDescuentoDevoluciones",
                F.round(F.col("SubtotalDevoluciones") * (F.col("PorcentajeDescuento") / 100), 2),
            )
            .withColumn(
                "ValorIvaDevoluciones",
                F.col("DevolucionesRotaciones") * (F.col("PorcentajeIva") / 100),
            )
            .withColumn(
                "ValorImpuestoSaludableDevoluciones",
                F.round(
                    F.col("SubtotalDevoluciones") * (F.col("PorcentajeImpuestoSaludable") / 100), 2
                ),
            )
            .withColumn(
                "TotalDevoluciones",
                F.round(
                    F.col("SubtotalDevoluciones")
                    - F.col("ValorDescuentoDevoluciones")
                    + F.col("ValorIvaDevoluciones")
                    + F.col("ValorImpuestoSaludableDevoluciones"),
                    2,
                ),
            )
            .drop("Descuento", "Subtotal")
        )
        return df_transformed

    def process(self):
        """
        Orchestrate the end-to-end processing pipeline for credit note details.

        Steps:
            1. Log start of processing with dataset name from config.
            2. Read Bronze table incrementally using configured conditions.
            3. Apply imputations and log the resulting row count.
            4. Apply business transformations via `transformations`.
            5. Write results to Silver Delta table.
            6. Update last processed watermark for incremental loads.

        Returns:
            None
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df = self.read_bronze_table()
        df_imputed = self.imputations(df)
        df_imputed_len = df_imputed.cache().count()
        logger.info(f"df_imputed: {df_imputed_len} rows")
        df_transformed = self.transformations(df_imputed)
        self.write_delta_table(df_transformed)
        self.update_last_processed(df_transformed)
