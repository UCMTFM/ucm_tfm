from loguru import logger
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql import DataFrame as DF
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

    @staticmethod
    def get_model_vars(df_fact: DF, df_det_fact: DF) -> DF:
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
        return df_agg_model

    def add_ohe_vars(self, df_agg_model: DF, categorical_cols: list[str]) -> DF:
        indexers = [
            StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep")
            for col in categorical_cols
        ]
        encoders = [
            OneHotEncoder(inputCol=col + "_idx", outputCol=col + "_ohe") for col in categorical_cols
        ]
        pipeline = Pipeline(stages=indexers + encoders)
        model = pipeline.fit(df_agg_model)

        year_since = self.config.get("data_since")
        df_encoded = model.transform(df_agg_model).filter(
            (F.col("IdRuta").isNotNull())
            & (F.col("CantidadTotalPrevia").isNotNull())
            & (F.col("Anio") >= year_since)
        )
        return df_encoded

    def process(self):
        logger.info("Starting SalesByRoute processing")
        sources = self.config.get("sources")
        det_table_name = sources.get("detalle_facturas_table")
        fact_table_name = sources.get("facturas_table")
        df_det_fact = self.read_silver_table(det_table_name)
        df_fact = self.read_silver_table(fact_table_name)

        df_agg_model = SalesByRouteProcessor.get_model_vars(df_fact, df_det_fact).cache()
        categorical_cols = ["IdRuta", "IdProducto", "NombreProducto", "Anio", "Mes", "Dia"]
        df_encoded = self.add_ohe_vars(df_agg_model, categorical_cols).withColumnRenamed(
            "prediction", "Dia_ohe"
        )
        self.write_delta_table(df_encoded)
