from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class DetalleFacturasProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        """
        Initialize the DetalleFacturasProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
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
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df = self.read_bronze_table()
        df_transformed = self.transformations(df)
        self.write_delta_table(df_transformed)
        self.update_last_processed(df_transformed)
