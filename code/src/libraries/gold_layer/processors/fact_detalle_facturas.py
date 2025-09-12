# fact_detalle_facturas.py
"""
Builds <catalog>.gold.fact_detalle_facturas from silver.detalle_facturas
"""

from loguru import logger
from delta.tables import DeltaTable
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base_gold import BaseGoldProcessor, spark


class GoldFactDetalleFacturasProcessor(BaseGoldProcessor):
    """Invoice line items from silver.detalle_facturas; late-binding IdProducto; idempotent via RowHash."""

    TABLE_COMMENT = (
        "Invoice line items (Anulado=false). Resolves IdProducto by normalized name; "
        "insert-only with idempotence via RowHash and late-binding updates."
    )

	
	DDL_COLUMNS_SQL = (
        "RowHash STRING COMMENT 'Deterministic hash over source business fields', "
        "IdFactura STRING COMMENT 'Invoice id from silver.facturas', "
        "IdProducto INT COMMENT 'Product surrogate id from gold.dim_productos (nullable if no match)', "
        "Cantidad DOUBLE COMMENT 'Quantity', "
        "ValorUnitario DOUBLE COMMENT 'Unit price', "
        "ValorDescuento DOUBLE COMMENT 'Discount amount', "
        "PorcentajeIva DOUBLE COMMENT 'VAT percentage', "
        "ValorIva DOUBLE COMMENT 'VAT amount', "
        "PorcentajeImpuestoSaludable DOUBLE COMMENT 'Health tax percentage', "
        "ValorImpuestoSaludable DOUBLE COMMENT 'Health tax amount', "
        "Subtotal DOUBLE COMMENT 'Line subtotal (pre-tax/discount as per source definition)', "
        "Total DOUBLE COMMENT 'Line total amount'"
    )

    # -------------------- helpers (product normalization) --------------------
    def _map_name_fixes(self, df: DF, col_in: str, col_out: str) -> DF:
        """
        Apply the same targeted fixes as the product dimension and re-normalize.
        We DO NOT filter keywords (OFERTA/PROMO/etc.) to keep all lines in the fact.
        """
        c = F.col(col_in)
        fixed = (
            df.select(
                F.when(c == F.lit("GELATINA BALNCA"), F.lit("GELATINA BLANCA"))
                 .when(c.rlike(r"PAQUETON\s*X\s*50"), F.lit("PAQUETON X 50"))
                 .when(c == F.lit("LAMINA ROSUQILLA X 12"), F.lit("LAMINA ROSQUILLA X 12"))
                 .when(c == F.lit("DOCENERA"), F.lit("BOLSA DOCENERA"))
                 .otherwise(c).alias(col_out),
                *[x for x in df.columns if x != col_in]
            )
        )
        return fixed.withColumn(col_out, self.normalize_str(F.col(col_out)))

    # -------------------- source reading & preparation --------------------
    def read_source(self) -> DF:
        """Join detail with header to exclude Anulado; select required fields."""
        cat = self.catalog

        d = spark.table(f"{cat}.silver.detalle_facturas").select(
            F.col("IdFactura").cast("string").alias("IdFactura"),
            F.col("NombreProducto").cast("string").alias("NombreProducto"),
            F.col("Cantidad").cast("double").alias("Cantidad"),
            F.col("ValorUnitario").cast("double").alias("ValorUnitario"),
            F.col("ValorDescuento").cast("double").alias("ValorDescuento"),
            F.col("PorcentajeIva").cast("double").alias("PorcentajeIva"),
            F.col("ValorIva").cast("double").alias("ValorIva"),
            F.col("PorcentajeImpuestoSaludable").cast("double").alias("PorcentajeImpuestoSaludable"),
            F.col("ValorImpuestoSaludable").cast("double").alias("ValorImpuestoSaludable"),
            F.col("Subtotal").cast("double").alias("Subtotal"),
            F.col("Total").cast("double").alias("Total"),
        )

        f = spark.table(f"{cat}.silver.facturas").select(
            F.col("IdFactura").cast("string").alias("IdFactura"),
            F.col("Anulado").cast("boolean").alias("Anulado"),
        )

        return (d.join(f, on="IdFactura", how="inner")
                 .where(F.col("Anulado") == F.lit(False))
                 .drop("Anulado"))

    def add_product_normalization(self, src: DF) -> DF:
        """Add NombreProductoNorm with the same normalization pipeline as the dimension."""
        norm = src.withColumn("NombreProductoNorm", self.normalize_str(F.col("NombreProducto")))
        return self._map_name_fixes(norm, col_in="NombreProductoNorm", col_out="NombreProductoNorm")

    def map_to_dim_product(self, df: DF) -> DF:
        """Left join against gold.dim_productos by normalized name."""
        dim = spark.table(f"{self.catalog}.gold.dim_productos").select(
            F.col("IdProducto").cast("int").alias("IdProducto"),
            F.col("NombreProducto").alias("NombreProductoDim")
        )
        return (
            df.join(dim, df["NombreProductoNorm"] == dim["NombreProductoDim"], how="left")
              .drop("NombreProductoDim")
        )

    def add_row_hash(self, df: DF) -> DF:
        """
        Deterministic hash over business fields of the detail line.
        It deliberately excludes IdProducto or any normalization-only column.
        """
        return df.withColumn(
            "RowHash",
            F.sha2(F.concat_ws(
                "||",
                F.col("IdFactura").cast("string"),
                F.col("NombreProducto").cast("string"),
                F.col("Cantidad").cast("string"),
                F.col("ValorUnitario").cast("string"),
                F.col("ValorDescuento").cast("string"),
                F.col("PorcentajeIva").cast("string"),
                F.col("ValorIva").cast("string"),
                F.col("PorcentajeImpuestoSaludable").cast("string"),
                F.col("ValorImpuestoSaludable").cast("string"),
                F.col("Subtotal").cast("string"),
                F.col("Total").cast("string"),
            ), 256)
        )

    def build_fact_df(self, src: DF) -> DF:
        """Full pipeline to final GOLD shape (includes RowHash)."""
        with_norm = self.add_product_normalization(src)
        with_pid  = self.map_to_dim_product(with_norm)
        with_hash = self.add_row_hash(with_pid)

        cols = [
            "RowHash", "IdFactura", "IdProducto",
            "Cantidad", "ValorUnitario", "ValorDescuento",
            "PorcentajeIva", "ValorIva",
            "PorcentajeImpuestoSaludable", "ValorImpuestoSaludable",
            "Subtotal", "Total",
        ]
        return with_hash.select(*cols).dropDuplicates(["RowHash"])

    # -------------------- table creation --------------------
    def create_external_table(self, df_initial: DF) -> None:
        """Register the external table and append initial batch."""
        logger.info(f"Creating/registering external Delta table at LOCATION = {self.target_path}")
        super().create_external_table(
            df_initial=df_initial,
            ddl_columns_sql=self.DDL_COLUMNS_SQL,
            table_comment=self.TABLE_COMMENT,
            select_cols=[
                "RowHash", "IdFactura", "IdProducto",
                "Cantidad", "ValorUnitario", "ValorDescuento",
                "PorcentajeIva", "ValorIva",
                "PorcentajeImpuestoSaludable", "ValorImpuestoSaludable",
                "Subtotal", "Total",
            ],
        )

    # -------------------- merge incremental --------------------
    def merge_upsert(self, df_new: DF) -> None:
        """
        Insert-only by RowHash + late-binding of IdProducto:
          - when NOT MATCHED: INSERT
          - when MATCHED: UPDATE IdProducto = coalesce(t.IdProducto, s.IdProducto)
        """
        logger.info("Running MERGE (insert + late-binding IdProducto) into GOLD.fact_detalle_facturas")
        delta_tgt = DeltaTable.forName(spark, self.target_fullname)

        (
            delta_tgt.alias("t")
            .merge(df_new.alias("s"), "t.RowHash = s.RowHash")
            .whenMatchedUpdate(set={
                "IdProducto": "coalesce(t.IdProducto, s.IdProducto)"
            })
            .whenNotMatchedInsert(values={
                "RowHash": "s.RowHash",
                "IdFactura": "s.IdFactura",
                "IdProducto": "s.IdProducto",
                "Cantidad": "s.Cantidad",
                "ValorUnitario": "s.ValorUnitario",
                "ValorDescuento": "s.ValorDescuento",
                "PorcentajeIva": "s.PorcentajeIva",
                "ValorIva": "s.ValorIva",
                "PorcentajeImpuestoSaludable": "s.PorcentajeImpuestoSaludable",
                "ValorImpuestoSaludable": "s.ValorImpuestoSaludable",
                "Subtotal": "s.Subtotal",
                "Total": "s.Total",
            })
            .execute()
        )

    # -------------------- main --------------------
    def process(self) -> None:
        """Build initial load or perform incremental insert + late-binding update."""
        logger.info(f"Starting GOLD process for {self.target_fullname}")
        self.ensure_schema()

        src = self.read_source()
        fact_df = self.build_fact_df(src)

        if not self.table_exists():
            logger.info("Target table does not exist. Performing initial build...")
            self.create_external_table(fact_df)
            self.set_table_properties(self.table_properties)
            logger.info(f"Created and loaded table {self.target_fullname} at {self.target_path}")
            return

        logger.info("Target table exists. Performing incremental upsert...")
        tgt = spark.table(self.target_fullname).select("RowHash")
        pending = (fact_df.join(tgt, on="RowHash", how="left_anti").distinct())

        if self._df_is_empty(pending):
            logger.info("No new detail rows to insert. Fact is up to date.")
            return

        self.merge_upsert(pending)
        self.set_table_properties(self.table_properties)
        logger.info("Incremental merge completed successfully.")
