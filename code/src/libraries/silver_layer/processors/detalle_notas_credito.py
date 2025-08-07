from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class DetalleNotasCreditoProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        """
        Initialize the DetalleNotasCreditoProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def imputations(self, df: DF) -> DF:
        logger.info("Starting imputations on detalle_notas_credito data")
        imputation_config = self.config.get("imputations", {})
        for column, value in imputation_config.items():
            df = df.withColumn(
                column, F.when(F.col(column).isNull(), value).otherwise(F.col(column))
            )
        logger.info("Imputations completed successfully")
        return df

    def transformations(self, df: DF) -> DF:
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
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df = self.read_bronze_table()
        df_imputed = self.imputations(df)
        df_imputed_len = df_imputed.cache().count()
        logger.info(f"df_imputed: {df_imputed_len} rows")
        df_transformed = self.transformations(df_imputed)
        self.write_delta_table(df_transformed)
        self.update_last_processed(df_transformed)
