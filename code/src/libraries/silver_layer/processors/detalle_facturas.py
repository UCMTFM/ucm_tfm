from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class DetalleFacturasProcessor(BaseProcessor):
    """
    Processes invoice detail records from Bronze → Silver.

    Steps:
      1) Read incrementally from Bronze using watermark + dataset-specific filters.
      2) Apply simple imputations defined in config['imputations'].
      3) Transform: rename fields, derive Subtotal/discount, date parts, taxes, and Total.
      4) Write to the configured Silver table.
      5) Persist the new watermark (max _ingestion_time) back to the JSON config.
    """

    def __init__(self, config_path: str):
        """
        Initialize the processor.

        Args:
            config_path (str): Path to the JSON configuration file in DBFS.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        """
        Apply column selection/renames and derive financial fields.

        Derivations:
          - Subtotal = round(ValorUnitario * Cantidad, 2)
          - PorcentajeDescuento = (ValorDescuento * 100) / Subtotal  (0 if Subtotal == 0 or nulls)
          - Fecha parts: Anio, Mes, Dia
          - ValorImpuestoSaludable = round(Subtotal * (PorcentajeImpuestoSaludable / 100), 2)
          - Total = round(Subtotal - ValorDescuento + ValorIva + ValorImpuestoSaludable, 2)

        Args:
            df (DF): Input DataFrame from Bronze (after imputations).

        Returns:
            DF: Transformed DataFrame ready for Silver.
        """
        df_transformed = (
            df.select(
                "IdDetalle",
                "IdFactura",
                "IdProducto",
                "NombreProducto",
                "Cantidad",
                "ValorUnitario",
                "Descuento",
                "PorcentajeIva",
                "Iva",
                "ISPPorcentaje",
                "FechaCreacion",
                "_ingestion_time",
            )
            .withColumnRenamed("Descuento", "ValorDescuento")
            .withColumnRenamed("Iva", "ValorIva")
            .withColumnRenamed("ISPPorcentaje", "PorcentajeImpuestoSaludable")
            .withColumn("Subtotal", F.round(F.col("ValorUnitario") * F.col("Cantidad"), 2))
            .withColumn("PorcentajeDescuento", (F.col("ValorDescuento") * 100) / F.col("Subtotal"))
            .withColumn("Anio", F.year(F.col("FechaCreacion")))
            .withColumn("Mes", F.month(F.col("FechaCreacion")))
            .withColumn("Dia", F.dayofmonth(F.col("FechaCreacion")))
            .withColumn(
                "ValorImpuestoSaludable",
                F.round(F.col("Subtotal") * (F.col("PorcentajeImpuestoSaludable") / 100), 2),
            )
            .withColumn(
                "Total",
                F.round(
                    F.col("Subtotal")
                    - F.col("ValorDescuento")
                    + F.col("ValorIva")
                    + F.col("ValorImpuestoSaludable"),
                    2,
                ),
            )
        )
        return df_transformed

    def process(self):
        """
        Orchestrate the Bronze → Silver flow for invoice details.

        Pipeline:
          - Read Bronze incrementally (uses BaseProcessor.get_condition()).
          - Impute configured null values.
          - Transform and compute financial fields.
          - Write to Delta (Silver) using config['target_table'] unless overridden.
          - Update watermark (last_ingest_processed_date).
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df = self.read_bronze_table()
        df_imputed = self.imputations(df)
        df_transformed = self.transformations(df_imputed)
        self.write_delta_table(df_transformed)
        self.update_last_processed(df_transformed)
