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

    def process(self):
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")

        df = self.read_bronze_table()
        df_transformed = self.transformations(df)
        df_imputed = self.imputations(df_transformed)
        self.write_delta_table(df_imputed)
        self.update_last_processed(df_imputed)
