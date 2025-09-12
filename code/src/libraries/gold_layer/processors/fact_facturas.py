# fact_facturas.py
"""
Builds <catalog>.gold.fact_facturas from silver.facturas (Anulado=false).
"""

from loguru import logger
from delta.tables import DeltaTable
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base_gold import BaseGoldProcessor, spark


class GoldFactFacturasProcessor(BaseGoldProcessor):
    """Invoice fact from silver.facturas. Resolves NumeroCliente (ID → name); MERGE by IdFactura."""

    TABLE_COMMENT = (
        "Invoice fact from silver.facturas (Anulado=false). "
        "Includes NumeroCliente, IdRuta, IdFormaPago, and measures; upsert by IdFactura."
    )

    DDL_COLUMNS_SQL = (
        "IdFactura STRING COMMENT 'Invoice id from source', "
        "Fecha DATE COMMENT 'Invoice date (from Fecha or FechaCreacion)', "
        "NumeroCliente INT COMMENT 'FK to gold.dim_clientes', "
        "IdRuta STRING COMMENT 'Route id (FK to gold.dim_rutas via IdRuta)', "
        "IdFormaPago STRING COMMENT 'Payment form (degenerate dim)', "
        "Subtotal DECIMAL(18,2) COMMENT 'Subtotal', "
        "Descuento DECIMAL(18,2) COMMENT 'Discount', "
        "Iva DECIMAL(18,2) COMMENT 'VAT', "
        "ImpuestoSaludable DECIMAL(18,2) COMMENT 'Health tax', "
        "PorcentajeRetencion DECIMAL(18,4) COMMENT 'Retention percent', "
        "Total DECIMAL(18,2) COMMENT 'Total amount', "
        "Saldo DECIMAL(18,2) COMMENT 'Outstanding balance', "
        "Cantidad DECIMAL(18,2) COMMENT 'Quantity (if present on header; else NULL)'"
    )

    # -------- Util: safe column (NULL if missing) --------
    @staticmethod
    def _safe(df, name: str, cast: str = None):
        """Return column if present; otherwise a typed NULL literal."""
        cols = set(df.columns)
        if name in cols:
            c = F.col(name)
            return c.cast(cast) if cast else c
        return F.lit(None).cast(cast if cast else "string")

    # --------- Read source + prepare keys/measures ----------
    def build_source_fact(self) -> DF:
        """
        Read silver.facturas and:
          * build robust Fecha (Fecha | FechaCreacion)
          * normalize identifiers and name
          * cast measures defensively
          * deduplicate by IdFactura, keeping latest by creation/Fecha
        """
        src = spark.table(self.source_fullname)

        fecha_expr = F.to_date(
            F.coalesce(
                F.to_timestamp(self._safe(src, "Fecha")),
                F.to_timestamp(self._safe(src, "FechaCreacion"))
            )
        ).alias("Fecha")

        ident_raw  = self._safe(src, "Identificacion", "string").alias("Identificacion_raw")
        ident_cln  = self._canon_ident_for_tiebreak(F.col("Identificacion_raw")).alias("Ident_clean")
        nombre_raw = self._safe(src, "RazonSocialCliente").alias("RazonSocialCliente_raw")
        nombre_nrm = self.normalize_str_core(F.col("RazonSocialCliente_raw")).alias("NombreCliente_norm")

        def d182(name): return self._safe(src, name, "decimal(18,2)").alias(name)
        def d184(name): return self._safe(src, name, "decimal(18,4)").alias(name)

        df = (
            src.select(
                self._safe(src, "IdFactura", "string").alias("IdFactura"),
                fecha_expr,
                ident_raw, ident_cln, nombre_raw, nombre_nrm,
                self._safe(src, "IdRuta", "string").alias("IdRuta"),
                self._safe(src, "IdFormaPago", "string").alias("IdFormaPago"),
                d182("Subtotal"),
                d182("Descuento"),
                d182("Iva"),
                d182("ImpuestoSaludable"),
                d184("PorcentajeRetencion"),
                d182("Total"),
                d182("Saldo"),
                d182("Cantidad"),
                self._safe(src, "Anulado", "boolean").alias("Anulado"),
                F.coalesce(
                    F.to_timestamp(self._safe(src, "FechaCreacion")),
                    F.to_timestamp(self._safe(src, "Fecha"))
                ).alias("_ts_order")
            )
            .where(F.col("Anulado") == F.lit(False))
        )

        # Deduplicate by IdFactura: keep the latest by _ts_order
        from pyspark.sql.window import Window
        w = Window.partitionBy("IdFactura").orderBy(F.col("_ts_order").desc_nulls_last(), F.col("IdFactura").desc())
        df = (df.withColumn("_rn", F.row_number().over(w))
                .where(F.col("_rn") == 1)
                .drop("_rn"))

        return df

    # --------- Resolve NumeroCliente against dim_clientes ----------
    def attach_numero_cliente(self, base_df: DF) -> DF:
        """
        Resolve FK NumeroCliente using:
          - by identification (canonical)
          - fallback by normalized name
        """
        dim = spark.table(f"{self.catalog}.gold.dim_clientes") \
                   .select(
                       F.col("NumeroCliente").cast("int").alias("NumeroCliente"),
                       F.col("Identificacion").cast("string").alias("Ident_dim"),
                       F.col("NombreCliente").cast("string").alias("Nombre_dim")
                   )
        dim = dim.withColumn("Ident_clean_dim", self._canon_ident_for_tiebreak(F.col("Ident_dim"))) \
                 .withColumn("Nombre_norm_dim", self.normalize_str_core(F.col("Nombre_dim")))

        by_ident = dim.select(
            F.col("Ident_clean_dim").alias("Ident_clean"),
            F.col("NumeroCliente").alias("NumeroCliente_by_ident")
        )
        by_name = dim.select(
            F.col("Nombre_norm_dim").alias("NombreCliente_norm"),
            F.col("NumeroCliente").alias("NumeroCliente_by_name")
        )

        joined = (base_df
                  .join(by_ident, on="Ident_clean", how="left")
                  .join(by_name, on="NombreCliente_norm", how="left"))

        return joined.select(
            "IdFactura", "Fecha",
            F.coalesce(F.col("NumeroCliente_by_ident"), F.col("NumeroCliente_by_name")).cast("int").alias("NumeroCliente"),
            "IdRuta", "IdFormaPago",
            "Subtotal", "Descuento", "Iva", "ImpuestoSaludable", "PorcentajeRetencion",
            "Total", "Saldo", "Cantidad"
        )

    # ------------------- CREATE TABLE -------------------
    def create_external_table(self, df_initial: DF) -> None:
        """Create table and append initial rows (only those with FK resolved)."""
        df_initial = df_initial.where(F.col("NumeroCliente").isNotNull())
        super().create_external_table(
            df_initial=df_initial,
            ddl_columns_sql=self.DDL_COLUMNS_SQL,
            table_comment=self.TABLE_COMMENT,
            select_cols=[
                "IdFactura", "Fecha", "NumeroCliente", "IdRuta", "IdFormaPago",
                "Subtotal", "Descuento", "Iva", "ImpuestoSaludable", "PorcentajeRetencion",
                "Total", "Saldo", "Cantidad"
            ],
        )

    # ------------------- MERGE (upsert by IdFactura) -------------------
    def merge_upsert(self, df_upsert: DF) -> None:
        """Upsert by IdFactura; prefer non-null incoming values via COALESCE."""
        df_upsert = df_upsert.where(F.col("NumeroCliente").isNotNull())

        delta_tgt = DeltaTable.forName(spark, self.target_fullname)
        (
            delta_tgt.alias("t")
            .merge(df_upsert.alias("s"), "t.IdFactura = s.IdFactura")
            .whenMatchedUpdate(set={
                "Fecha":                "coalesce(s.Fecha, t.Fecha)",
                "NumeroCliente":        "coalesce(s.NumeroCliente, t.NumeroCliente)",
                "IdRuta":               "coalesce(s.IdRuta, t.IdRuta)",
                "IdFormaPago":          "coalesce(s.IdFormaPago, t.IdFormaPago)",
                "Subtotal":             "coalesce(s.Subtotal, t.Subtotal)",
                "Descuento":            "coalesce(s.Descuento, t.Descuento)",
                "Iva":                  "coalesce(s.Iva, t.Iva)",
                "ImpuestoSaludable":    "coalesce(s.ImpuestoSaludable, t.ImpuestoSaludable)",
                "PorcentajeRetencion":  "coalesce(s.PorcentajeRetencion, t.PorcentajeRetencion)",
                "Total":                "coalesce(s.Total, t.Total)",
                "Saldo":                "coalesce(s.Saldo, t.Saldo)",
                "Cantidad":             "coalesce(s.Cantidad, t.Cantidad)",
            })
            .whenNotMatchedInsert(values={
                "IdFactura":            "s.IdFactura",
                "Fecha":                "s.Fecha",
                "NumeroCliente":        "s.NumeroCliente",
                "IdRuta":               "s.IdRuta",
                "IdFormaPago":          "s.IdFormaPago",
                "Subtotal":             "s.Subtotal",
                "Descuento":            "s.Descuento",
                "Iva":                  "s.Iva",
                "ImpuestoSaludable":    "s.ImpuestoSaludable",
                "PorcentajeRetencion":  "s.PorcentajeRetencion",
                "Total":                "s.Total",
                "Saldo":                "s.Saldo",
                "Cantidad":             "s.Cantidad",
            })
            .execute()
        )

    # ------------------- MAIN -------------------
    def process(self) -> None:
        """Validate dependency on dim_clientes, then build or upsert."""
        logger.info(f"Starting GOLD process for {self.target_fullname}")
        self.ensure_schema()

        # Fast validation: dim_clientes must exist
        try:
            spark.table(f"{self.catalog}.gold.dim_clientes").limit(1).collect()
        except Exception as e:
            raise RuntimeError(
                f"gold.dim_clientes not available or readable in catalog '{self.catalog}'. "
                f"Run the dim_clientes processor first. Details: {e}"
            )

        base_df   = self.build_source_fact()
        resolved  = self.attach_numero_cliente(base_df)

        if not self.table_exists():
            logger.info("Target table does not exist. Performing initial build…")
            self.create_external_table(resolved)
            self.set_table_properties(self.table_properties)
            logger.info(f"Created and loaded table {self.target_fullname} at {self.target_path}")
            return

        logger.info("Target table exists. Performing upsert by IdFactura…")
        self.merge_upsert(resolved)
        self.set_table_properties(self.table_properties)
        logger.info("Upsert completed successfully.")
