from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class FacturasProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        """
        Initialize the DetalleFacturasProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def imputations(self, df: DF) -> DF:
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
        Read the detalle_facturas table from the silver layer.

        Returns:
            DataFrame: The loaded data from the detalle_facturas table.
        """
        logger.info("Reading detalle_facturas data from silver layer")
        last_ingest_processed_date = self.config.get("last_ingest_processed_date", "")
        df_details = self.read_table("silver", "detalle_facturas", last_ingest_processed_date)
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

    # def read_client_data(self) -> DF:
    #     logger.info("Reading cliente data from bronze layer")
    #     last_ingest_processed_date = self.config.get("last_ingest_processed_date", "")
    #     df_clientes = self.read_table("silver", "clientes", last_ingest_processed_date)
    #     logger.info("Client data read successfully")
    #     return df_clientes

    @staticmethod
    def add_detail_data(df: DF, df_details: DF) -> DF:
        """
        Join the main DataFrame with the detalle_facturas data.

        Args:
            df (DataFrame): The main DataFrame to join.
            df_details (DataFrame): The detalle_facturas DataFrame.

        Returns:
            DataFrame: The joined DataFrame.
        """
        logger.info("Joining facturas with detalle_facturas data")
        df_joined = df.join(df_details, on="IdFactura", how="left").withColumn(
            "PorcentajeRetencion", (F.col("ValorRetencion") * 100) / F.col("Subtotal")
        )
        return df_joined

    def process(self):
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df_fact = self.read_bronze_table()
        df_detail = self.read_detail_data()
        df_imputed = self.imputations(df_fact)
        df_transformed = self.transformations(df_imputed)
        df_result = FacturasProcessor.add_detail_data(df_transformed, df_detail)
        self.write_delta_table(df_result)
        self.update_last_processed(df_result)
