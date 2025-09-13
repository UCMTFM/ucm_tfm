# fact_detalle_facturas.py
"""
Builds <catalog>.gold.fact_detalle_facturas from silver.detalle_facturas
"""

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class GoldFactDetalleFacturasProcessor(BaseProcessor):
    """
    GOLD fact table for invoice line items sourced from `silver.detalle_facturas`,
    joined to `silver.facturas` to exclude voided invoices (Anulado = false). It
    performs late-binding of `IdProducto` by normalized product name against
    `gold.dim_productos`, and guarantees idempotence via a deterministic `RowHash`.

    Workflow:
        1) Read invoice detail lines and join with invoice headers to filter `Anulado = false`.
        2) Normalize product names (same pipeline as the product dimension) and apply targeted fixes.
        3) Map lines to `gold.dim_productos` by normalized name (late-binding of `IdProducto`).
        4) Compute `RowHash` over the business fields to make the pipeline idempotent.
        5) If table does not exist, create/register external Delta and append the initial batch.
        6) Otherwise, MERGE:
             - NOT MATCHED → INSERT full row
             - MATCHED     → UPDATE `IdProducto = coalesce(t.IdProducto, s.IdProducto)`
        7) Apply optional table properties.

    Inherits from:
        BaseProcessor: configuration handling, storage connectivity, Delta helpers,
        and common utility functions (normalization, table existence checks, etc.).
    """

    TABLE_COMMENT = (
        "Invoice line items (Anulado=false). Resolves IdProducto by normalized name; "
        "insert-only with idempotence via RowHash and late-binding updates."
    )
    DDL_COLUMNS_SQL = (
        "RowHash STRING COMMENT 'Deterministic hash over source business fields', "
        "IdFactura STRING COMMENT 'Invoice id from silver.facturas', "
        "IdProducto INT COMMENT 'Product surrogate id from gold.dim_productos (nullable if "
        "no match)', Cantidad DOUBLE COMMENT 'Quantity', "
        "ValorUnitario DOUBLE COMMENT 'Unit price', "
        "ValorDescuento DOUBLE COMMENT 'Discount amount', "
        "PorcentajeIva DOUBLE COMMENT 'VAT percentage', "
        "ValorIva DOUBLE COMMENT 'VAT amount', "
        "PorcentajeImpuestoSaludable DOUBLE COMMENT 'Health tax percentage', "
        "ValorImpuestoSaludable DOUBLE COMMENT 'Health tax amount', "
        "Subtotal DOUBLE COMMENT 'Line subtotal (pre-tax/discount as per source definition)', "
        "Total DOUBLE COMMENT 'Line total amount'"
    )

    def _map_name_fixes(self, df: DF, col_in: str, col_out: str) -> DF:
        """
        Apply targeted product-name fixes (aligned with the product dimension) and re-normalize.

        Notes:
            Unlike the dimension, this fact does not drop marketing-like keywords; it keeps
            all detail lines intact.

        Args:
            df (DF): Input DataFrame containing a normalized product name column.
            col_in (str): Name of the input normalized column to fix.
            col_out (str): Name of the output normalized column after fixes.

        Returns:
            DF: DataFrame with the corrected and re-normalized product name in `col_out`.
        """
        c = F.col(col_in)
        fixed = df.select(
            F.when(c == F.lit("GELATINA BALNCA"), F.lit("GELATINA BLANCA"))
            .when(c.rlike(r"PAQUETON\s*X\s*50"), F.lit("PAQUETON X 50"))
            .when(c == F.lit("LAMINA ROSUQILLA X 12"), F.lit("LAMINA ROSQUILLA X 12"))
            .when(c == F.lit("DOCENERA"), F.lit("BOLSA DOCENERA"))
            .otherwise(c)
            .alias(col_out),
            *[x for x in df.columns if x != col_in],
        )
        return fixed.withColumn(col_out, self.normalize_str(F.col(col_out))) 

    def read_source(self) -> DF:
        """
        Read `silver.detalle_facturas` and join with `silver.facturas` to exclude voided invoices.

        Returns:
            DF: Detail lines with required numeric fields casted and only rows where
                `Anulado = false`. Columns include:
                IdFactura, NombreProducto, Cantidad, ValorUnitario, ValorDescuento,
                PorcentajeIva, ValorIva, PorcentajeImpuestoSaludable, ValorImpuestoSaludable,
                Subtotal, Total.
        """
        cat = self.catalog

        d = self.spark.table(f"{cat}.silver.detalle_facturas").select(
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

        f = self.spark.table(f"{cat}.silver.facturas").select(
            F.col("IdFactura").cast("string").alias("IdFactura"),
            F.col("Anulado").cast("boolean").alias("Anulado"),
        )

        return (
            d.join(f, on="IdFactura", how="inner").where(F.col("Anulado") == F.lit(False)).drop("Anulado")
        )  

    def add_product_normalization(self, src: DF) -> DF:
        """
        Add a normalized product name column using the same baseline normalization as the dimension.

        Args:
            src (DF): Source DataFrame returned by :meth:`read_source`.

        Returns:
            DF: DataFrame with an extra `NombreProductoNorm` column normalized and fixed.
        """
        norm = src.withColumn("NombreProductoNorm", self.normalize_str(F.col("NombreProducto")))
        return self._map_name_fixes(norm, col_in="NombreProductoNorm", col_out="NombreProductoNorm")  

    def map_to_dim_product(self, df: DF) -> DF:
        """
        Resolve `IdProducto` by joining to `gold.dim_productos` on normalized product name.

        Args:
            df (DF): DataFrame with `NombreProductoNorm` (from :meth:`add_product_normalization`).

        Returns:
            DF: DataFrame with `IdProducto` (nullable when no match in the dimension).
        """
        dim = self.spark.table(f"{self.catalog}.gold.dim_productos").select(
            F.col("IdProducto").cast("int").alias("IdProducto"),
            F.col("NombreProducto").alias("NombreProductoDim"),
        )
        return df.join(
            dim, df["NombreProductoNorm"] == dim["NombreProductoDim"], how="left"
        ).drop("NombreProductoDim")  

    def add_row_hash(self, df: DF) -> DF:
        """
        Compute a deterministic hash (`RowHash`) over the business fields of each line.

        Design:
            The hash excludes `IdProducto` and any normalization-only columns to ensure
            idempotence regardless of late-binding or normalization changes.

        Args:
            df (DF): DataFrame containing all business fields to be hashed.

        Returns:
            DF: DataFrame with an additional `RowHash` column.
        """
        return df.withColumn(
            "RowHash",
            F.sha2(
                F.concat_ws(
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
                ),
                256,
            ),
        )  

    def build_fact_df(self, src: DF) -> DF:
        """
        Full pipeline from raw detail lines to the final GOLD schema.

        Steps:
            1) Normalize and fix product names.
            2) Map to product dimension for `IdProducto`.
            3) Add `RowHash`.
            4) Project final columns and drop duplicates by `RowHash`.

        Args:
            src (DF): Source DataFrame as returned by :meth:`read_source`.

        Returns:
            DF: DataFrame with columns:
                RowHash, IdFactura, IdProducto, Cantidad, ValorUnitario, ValorDescuento,
                PorcentajeIva, ValorIva, PorcentajeImpuestoSaludable, ValorImpuestoSaludable,
                Subtotal, Total.
        """
        with_norm = self.add_product_normalization(src)
        with_pid = self.map_to_dim_product(with_norm)
        with_hash = self.add_row_hash(with_pid)

        cols = [
            "RowHash",
            "IdFactura",
            "IdProducto",
            "Cantidad",
            "ValorUnitario",
            "ValorDescuento",
            "PorcentajeIva",
            "ValorIva",
            "PorcentajeImpuestoSaludable",
            "ValorImpuestoSaludable",
            "Subtotal",
            "Total",
        ]
        return with_hash.select(*cols).dropDuplicates(["RowHash"])  

    def create_external_table(self, df_initial: DF) -> None:
        """
        Register the external Delta table (if missing) and append the initial batch.

        Args:
            df_initial (DF): Fully shaped DataFrame from :meth:`build_fact_df`.
        """
        logger.info(f"Creating/registering external Delta table at LOCATION = {self.target_path}")
        super().create_external_table(
            df_initial=df_initial,
            ddl_columns_sql=self.DDL_COLUMNS_SQL,
            table_comment=self.TABLE_COMMENT,
            select_cols=[
                "RowHash",
                "IdFactura",
                "IdProducto",
                "Cantidad",
                "ValorUnitario",
                "ValorDescuento",
                "PorcentajeIva",
                "ValorIva",
                "PorcentajeImpuestoSaludable",
                "ValorImpuestoSaludable",
                "Subtotal",
                "Total",
            ],
        )  

    def merge_upsert(self, df_new: DF) -> None:
        """
        Idempotent merge with late-binding of `IdProducto`.

        Behavior:
            - NOT MATCHED → INSERT full row.
            - MATCHED     → UPDATE `IdProducto = coalesce(t.IdProducto, s.IdProducto)`.

        Args:
            df_new (DF): New/pending rows to upsert, already shaped like the target table.
        """
        logger.info("Running MERGE (insert + late-binding IdProducto) into GOLD.fact_detalle_facturas")
        delta_tgt = DeltaTable.forName(self.spark, self.target_fullname)

        (
            delta_tgt.alias("t")
            .merge(df_new.alias("s"), "t.RowHash = s.RowHash")
            .whenMatchedUpdate(set={"IdProducto": "coalesce(t.IdProducto, s.IdProducto)"})
            .whenNotMatchedInsert(
                values={
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
                }
            )
            .execute()
        )  

    def process(self) -> None:
        """
        Orchestrate initial build or incremental upsert with late-binding.

        Steps:
            1) Ensure schema exists.
            2) Read/prepare source and build the target-shaped DataFrame.
            3) If target table is missing → create and append initial batch.
            4) Else → compute pending `RowHash` rows and MERGE them.
            5) Apply table properties if configured.

        Returns:
            None
        """
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
        tgt = self.spark.table(self.target_fullname).select("RowHash")
        pending = fact_df.join(tgt, on="RowHash", how="left_anti").distinct()

        if self._df_is_empty(pending):
            logger.info("No new detail rows to insert. Fact is up to date.")
            return

        self.merge_upsert(pending)
        self.set_table_properties(self.table_properties)
        logger.info("Incremental merge completed successfully.")  
