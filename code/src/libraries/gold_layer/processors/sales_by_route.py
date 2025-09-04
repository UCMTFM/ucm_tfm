from loguru import logger
from pyspark.sql import Window
from pyspark.sql import functions as F

from .base import BaseProcessor


class SalesByRouteProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        """
        Initialize the DetalleFacturasProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def process(self):
        logger.info("Starting SalesByRoute processing")
        sources = self.config.get("sources")
        det_table_name = sources.get("detalle_facturas_table")
        fact_table_name = sources.get("facturas_table")
        df_det_fact = self.read_silver_table(det_table_name)
        df_fact = self.read_silver_table(fact_table_name)
        df_det_route = df_det_fact.join(
            df_fact.select("IdFactura", "IdRuta"), on="IdFactura", how="left"
        )
        df_agg = (
            df_det_route.groupBy(["IdRuta", "IdProducto", "NombreProducto", "Dia", "Mes", "Anio"])
            .agg(F.sum(F.col("Cantidad")).alias("CantidadTotal"))
            .orderBy("IdRuta", "IdProducto", "Anio", "Mes", "Dia")
        )
        w = Window.partitionBy(F.col("IdRuta"), F.col("IdProducto")).orderBy(
            F.col("Anio"), F.col("Mes"), F.col("Dia")
        )
        df_agg_model = df_agg.withColumn(
            "CantidadTotalPrevia", F.lag("CantidadTotal", 1).over(w)
        ).select(
            F.col("IdRuta"),
            F.col("IdProducto"),
            F.col("NombreProducto"),
            F.col("Anio"),
            F.col("Mes"),
            F.col("Dia"),
            F.col("CantidadTotalPrevia"),
            F.col("CantidadTotal"),
        )
        self.write_delta_table(df_agg_model)
        logger.info("SalesByRoute processing finished successfully")
