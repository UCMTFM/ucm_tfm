# dim_productos.py
"""
Builds <catalog>.gold.dim_productos from silver.detalle_facturas
"""

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .base import BaseProcessor


class GoldDimProductosProcessor(BaseProcessor):
    """
    Products dimension (gold). Built from silver.facturas + silver.detalle_facturas with a
    stable surrogate id.

    Workflow:
        1) Read `silver.detalle_facturas` joined with `silver.facturas` to exclude annulled rows.
        2) Normalize product names and filter out marketing-like items.
        3) Apply targeted name fixes and re-normalize.
        4) Compute first-seen timestamp per normalized name and assign `IdProducto` by dense rank.
        5) Create the table on first run and seed rows; otherwise insert only new products.
    """

    TABLE_COMMENT = (
        "Products dimension (gold). Built from silver.facturas + "
        "silver.detalle_facturas with stable surrogate id."
    )

    COLUMNS_COMMENT = {
        "IdProducto": "Surrogate product id (stable, incremental)",
        "NombreProducto": "Normalized product name",
    }

    def read_joined_source(self) -> DF:
        """
        Read and join detail with header to filter out annulled facts.

        Returns:
            DF: DataFrame with columns:
                - NombreProducto (string)
                - FechaCreacion_det (timestamp)
        """
        cat = self.catalog
        d = self.spark.table(f"{cat}.silver.detalle_facturas").select(
            F.col("IdFactura").alias("IdFactura"),
            F.col("NombreProducto").cast("string").alias("NombreProducto"),
            F.col("FechaCreacion").alias("FechaCreacion_det"),
        )
        f = self.spark.table(f"{cat}.silver.facturas").select(
            F.col("IdFactura").alias("IdFactura"),
            F.col("Anulado").cast("boolean").alias("Anulado"),
        )

        return (
            d.join(f, on="IdFactura", how="inner")
            .where(F.col("NombreProducto").isNotNull() & (F.col("Anulado") == F.lit(False)))
            .select("NombreProducto", "FechaCreacion_det")
        )

    def normalize_names(self, src: DF) -> DF:
        """
        Apply baseline normalization to product names.

        Args:
            src (DF): Input with `NombreProducto` and `FechaCreacion_det`.

        Returns:
            DF: DataFrame with
                - NombreProductoNorm (normalized name)
                - FechaCreacion     (timestamp)
        """
        return src.select(
            self.normalize_str(F.col("NombreProducto")).alias("NombreProductoNorm"),
            F.col("FechaCreacion_det").alias("FechaCreacion"),
        )

    def filter_base(self, norm: DF) -> DF:
        """
        Remove marketing-like items for dimension hygiene.

        Args:
            norm (DF): DataFrame with `NombreProductoNorm`.

        Returns:
            DF: Filtered DataFrame excluding terms such as OFERTA, PROMO, FIDELIZ, BONO, REGALO.
        """
        return (
            norm.where(~F.col("NombreProductoNorm").contains("OFERTA"))
            .where(~F.col("NombreProductoNorm").contains("PROMO"))
            .where(~F.col("NombreProductoNorm").contains("FIDELIZ"))
            .where(~F.col("NombreProductoNorm").contains("BONO"))
            .where(~F.col("NombreProductoNorm").contains("REGALO"))
        )

    def map_name_fixes(self, base: DF) -> DF:
        """
        Apply targeted name fixes and re-normalize to final form.

        Args:
            base (DF): DataFrame with `NombreProductoNorm` and `FechaCreacion`.

        Returns:
            DF: DataFrame with canonicalized
                - NombreProductoNorm
                - FechaCreacion
        """
        c = F.col("NombreProductoNorm")
        mapped = base.select(
            F.when(c == F.lit("GELATINA BALNCA"), F.lit("GELATINA BLANCA"))
            .when(c.rlike(r"PAQUETON\s*X\s*50"), F.lit("PAQUETON X 50"))
            .when(c == F.lit("LAMINA ROSUQILLA X 12"), F.lit("LAMINA ROSQUILLA X 12"))
            .when(c == F.lit("DOCENERA"), F.lit("BOLSA DOCENERA"))
            .otherwise(c)
            .alias("NombreProductoNorm"),
            F.col("FechaCreacion"),
        )
        return mapped.select(
            self.normalize_str(F.col("NombreProductoNorm")).alias("NombreProductoNorm"),
            F.col("FechaCreacion"),
        )

    def compute_first_seen(self, mapped: DF) -> DF:
        """
        Compute first-seen timestamp per normalized product name.

        Args:
            mapped (DF): DataFrame with `NombreProductoNorm` and `FechaCreacion`.

        Returns:
            DF: DataFrame with
                - NombreProductoNorm
                - first_seen_ts (timestamp)
        """
        return mapped.groupBy("NombreProductoNorm").agg(
            F.min(F.to_timestamp(F.col("FechaCreacion"))).alias("first_seen_ts")
        )

    def compute_initial_dim(self, first_seen: DF) -> DF:
        """
        Assign `IdProducto` by dense rank over (first_seen_ts, NombreProductoNorm).

        Args:
            first_seen (DF): DataFrame with `NombreProductoNorm` and `first_seen_ts`.

        Returns:
            DF: Dimension seed with
                - IdProducto (int)
                - NombreProducto (string)
        """
        w_dense = Window.orderBy(
            F.col("first_seen_ts").asc_nulls_last(), F.col("NombreProductoNorm").asc()
        )
        return first_seen.withColumn("IdProducto", F.dense_rank().over(w_dense)).select(
            F.col("IdProducto").cast("int").alias("IdProducto"),
            F.col("NombreProductoNorm").alias("NombreProducto"),
        )

    def create_external_table(self, df_initial: DF) -> None:
        """
        Create/register the external Delta table and seed initial rows
        (ordered by IdProducto, NombreProducto) if empty.

        Args:
            df_initial (DF): Seed rows with `IdProducto` and `NombreProducto`.
        """
        logger.info(f"Creating external Delta table at LOCATION = {self.target_path}")

        cols_comment_sql = (
            "IdProducto INT COMMENT 'Surrogate product id (stable, incremental)', "
            "NombreProducto STRING COMMENT 'Normalized product name'"
        )

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.target_fullname} (
                {cols_comment_sql}
            )
            USING DELTA
            LOCATION '{self.target_path}'
            COMMENT '{self.TABLE_COMMENT}'
        """
        )
        self.set_table_properties(self.table_properties)

        if self._table_is_empty():
            (
                df_initial.select("IdProducto", "NombreProducto")
                .orderBy(F.col("IdProducto").asc(), F.col("NombreProducto").asc())
                .writeTo(self.target_fullname)
                .append()
            )
            logger.info("Initial load appended.")
        else:
            logger.info("Table already has data. Skipping initial load.")

    def merge_upsert_insert_only(self, df_new: DF) -> None:
        """
        Insert-only MERGE for new product names.

        Args:
            df_new (DF): New rows with `IdProducto` and `NombreProducto`.
        """
        logger.info("Running MERGE (insert-only) into GOLD.dim_productos")
        delta_tgt = DeltaTable.forName(self.spark, self.target_fullname)
        (
            delta_tgt.alias("t")
            .merge(df_new.alias("s"), "t.NombreProducto = s.NombreProducto")
            .whenNotMatchedInsert(
                values={"IdProducto": "s.IdProducto", "NombreProducto": "s.NombreProducto"}
            )
            .execute()
        )

    def process(self) -> None:
        """
        Orchestrate the GOLD build for `dim_productos`.

        Workflow:
            - Ensure schema, then read/join/normalize/filter/fix names.
            - Compute first-seen and derive seed dimension.
            - If table doesn't exist: create and load initial snapshot.
            - Else: insert only newly observed products and set table properties.
        """
        logger.info(f"Starting GOLD process for {self.target_fullname}")
        self.ensure_schema()

        src = self.read_joined_source()
        norm = self.normalize_names(src)
        base = self.filter_base(norm)
        mapped = self.map_name_fixes(base)
        first_seen = self.compute_first_seen(mapped)

        if not self.table_exists():
            logger.info("Target table does not exist. Performing initial build...")
            dim0 = self.compute_initial_dim(first_seen)
            self.create_external_table(dim0)
            logger.info(f"Created and loaded table {self.target_fullname} at {self.target_path}")
        else:
            logger.info("Target table exists. Performing incremental insert...")
            tgt = self.spark.table(self.target_fullname).select("NombreProducto", "IdProducto")

            dim_current = self.compute_initial_dim(first_seen)
            new_names = dim_current.join(
                tgt.select("NombreProducto"), on="NombreProducto", how="left_anti"
            ).distinct()

            if new_names.rdd.isEmpty():
                logger.info("No new products to insert. Dimension is up to date.")
                return

            max_n = tgt.select(F.max("IdProducto").alias("mx")).collect()[0]["mx"]
            max_n = int(max_n) if max_n is not None else 0

            w_new = Window.orderBy(F.col("IdProducto").asc(), F.col("NombreProducto").asc())
            to_insert = new_names.withColumn(
                "rn", F.row_number().over(w_new) + F.lit(max_n)
            ).select(F.col("rn").cast("int").alias("IdProducto"), "NombreProducto")

            self.merge_upsert_insert_only(to_insert)
            self.set_table_properties(self.table_properties)
            logger.info("Incremental insert completed successfully.")
