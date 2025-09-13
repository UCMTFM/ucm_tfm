# dim_clientes.py
"""
Builds <catalog>.gold.dim_clientes from silver.facturas.
"""

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .base import BaseProcessor


class GoldDimClientesProcessor(BaseProcessor):
    """
    Customer dimension (gold) built from `silver.facturas`.

    Workflow:
        1. Read the minimal fields from the Silver facts required to form groups
           (normalized customer name and raw ID) and compute a grouping key.
        2. Determine, per group, the first-seen timestamp and a deterministic
           representative row.
        3. Assign a stable `NumeroCliente` using the order of first appearance
           (dense ranking over first_seen_ts, group_key).
        4. If the table does not exist, create it and seed the initial snapshot.
        5. If the table exists, append only newly seen representative names
           while preserving previously assigned numbers.

    Inherits from:
        BaseProcessor: utilities for Spark/Delta operations, configuration, and I/O.

    Table semantics:
        - One row per “ID-group” (canon of identification or normalized name).
        - `NumeroCliente` is append-only and stable once assigned.
    """

    TABLE_COMMENT = (
        "Customer dimension from silver.facturas. Stable NumeroCliente per ID-group "
        "(append-only)."
    )
    DDL_COLUMNS_SQL = (
        "NumeroCliente INT COMMENT 'Stable surrogate number (first-seen group order)', "
        "Identificacion STRING COMMENT 'First ID observed for the group (raw)', "
        "NombreCliente STRING COMMENT 'Representative name (first observed; normalized core)'"
    )

    def read_base(self) -> DF:
        """
        Read minimal fields from source and build `group_key`.

        Source:
            `<catalog>.<sources.main_schema>.<sources.main_table>` (from config).

        Filtering:
            - Keep only non-annulled rows and non-null identifiers / names.

        Returns:
            DF: Columns
                - Identificacion (string, raw)
                - Identificacion_clean (string, canonical for tie-breaks)
                - NombreCliente (string, normalized core)
                - ts_creacion (timestamp, first-seen reference)
                - group_key (string, grouping key from ID or normalized name)
        """
        f = self.spark.table(self.source_fullname).select(
            F.col("Identificacion").cast("string").alias("Identificacion_raw"),
            self.normalize_str_core(F.col("RazonSocialCliente")).alias("NombreCliente"),
            F.to_timestamp(F.col("FechaCreacion")).alias("ts_creacion"),
            F.col("Anulado").cast("boolean").alias("Anulado"),
        )

        base = (
            f.where(
                (F.col("Anulado") == F.lit(False))
                & F.col("Identificacion_raw").isNotNull()
                & F.col("NombreCliente").isNotNull()
            )
            .withColumn(
                "Identificacion_clean", self._canon_ident_for_tiebreak(F.col("Identificacion_raw"))
            )
            .select(
                F.col("Identificacion_raw").alias("Identificacion"),
                F.col("Identificacion_clean"),
                F.col("NombreCliente"),
                F.col("ts_creacion"),
            )
        )

        return base.withColumn(
            "group_key", self._group_key("Identificacion_clean", "NombreCliente")
        )

    def compute_first_seen_per_group(self, base: DF) -> DF:
        """
        Compute first appearance timestamp per group.

        Args:
            base (DF): Output of `read_base()` including `group_key` and `ts_creacion`.

        Returns:
            DF: Columns
                - group_key
                - first_seen_ts (min(ts_creacion) per group_key)
        """
        return base.groupBy("group_key").agg(F.min("ts_creacion").alias("first_seen_ts"))

    def compute_rep_row_per_group(self, base: DF) -> DF:
        """
        Pick a deterministic representative row per `group_key`.

        Ordering (ascending):
            ts_creacion, Identificacion_clean, Identificacion, NombreCliente.

        Args:
            base (DF): Output of `read_base()`.

        Returns:
            DF: Columns
                - group_key
                - Identificacion_rep
                - NombreCliente_rep
        """
        w = Window.partitionBy("group_key").orderBy(
            F.col("ts_creacion").asc(),
            F.col("Identificacion_clean").asc(),
            F.col("Identificacion").asc(),
            F.col("NombreCliente").asc(),
        )
        return (
            base.withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") == 1)
            .select(
                "group_key",
                F.col("Identificacion").alias("Identificacion_rep"),
                F.col("NombreCliente").alias("NombreCliente_rep"),
            )
        )

    def compute_initial_dim(self, first_seen_grp: DF, rep_grp: DF) -> DF:
        """
        Assign `NumeroCliente` by first-seen order and project representative fields.

        Args:
            first_seen_grp (DF): Output of `compute_first_seen_per_group`.
            rep_grp (DF): Output of `compute_rep_row_per_group`.

        Returns:
            DF: Columns
                - NumeroCliente (int, dense_rank over first_seen_ts + group_key)
                - Identificacion (from representative row)
                - NombreCliente (from representative row)
                - first_seen_ts
                - group_key
        """
        w_dense = Window.orderBy(F.col("first_seen_ts").asc(), F.col("group_key").asc())
        numbered = first_seen_grp.withColumn("NumeroCliente", F.dense_rank().over(w_dense)).select(
            "group_key", "NumeroCliente", "first_seen_ts"
        )

        return numbered.join(rep_grp, on="group_key", how="inner").select(
            F.col("NumeroCliente").cast("int").alias("NumeroCliente"),
            F.col("Identificacion_rep").alias("Identificacion"),
            F.col("NombreCliente_rep").alias("NombreCliente"),
            "first_seen_ts",
            "group_key",
        )

    def create_external_table(self, df_initial: DF) -> None:
        """
        Create/register the Delta table (if missing) and perform initial load.

        Behavior:
            - Publishes final columns (NumeroCliente, Identificacion, NombreCliente).
            - Extra deduplication by `NombreCliente` for safety before seeding.

        Args:
            df_initial (DF): Output of `compute_initial_dim` to seed the table.

        Returns:
            None
        """
        df_initial = df_initial.select(
            "NumeroCliente", "Identificacion", "NombreCliente"
        ).dropDuplicates(["NombreCliente"])
        super().create_external_table(
            df_initial=df_initial,
            ddl_columns_sql=self.DDL_COLUMNS_SQL,
            table_comment=self.TABLE_COMMENT,
            select_cols=["NumeroCliente", "Identificacion", "NombreCliente"],
        )

    def merge_insert_new_names(self, df_new: DF) -> None:
        """
        Insert-only MERGE for newly seen representative names.

        Notes:
            - Within each `group_key` only the first (earliest) candidate is kept.
            - Deduplicates by `NombreCliente` before merging into target.

        Args:
            df_new (DF): Candidate rows with columns
                NumeroCliente, Identificacion, NombreCliente, first_seen_ts, group_key.

        Returns:
            None
        """
        logger.info(
            "Running MERGE (insert-only by representative NombreCliente) into GOLD.dim_clientes"
        )

        df_new = (
            df_new.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("group_key").orderBy(
                        F.col("first_seen_ts").asc(), F.col("NumeroCliente").asc()
                    )
                ),
            )
            .where(F.col("rn") == 1)
            .drop("rn")
            .dropDuplicates(["NombreCliente"])
        )

        delta_tgt = DeltaTable.forName(self.spark, self.target_fullname)
        (
            delta_tgt.alias("t")
            .merge(df_new.alias("s"), "t.NombreCliente = s.NombreCliente")
            .whenNotMatchedInsert(
                values={
                    "NumeroCliente": "s.NumeroCliente",
                    "Identificacion": "s.Identificacion",
                    "NombreCliente": "s.NombreCliente",
                }
            )
            .execute()
        )

    def process(self) -> None:
        """
        Orchestrate the GOLD build for `dim_clientes`.

        Flow:
            - Ensure schema exists.
            - Read base, compute first-seen timestamps and representative rows.
            - If table is missing/stale: create and seed the initial snapshot.
            - Else: append only newly seen representative names (insert-only MERGE),
                    preserving existing numbers.

        Returns:
            None
        """
        logger.info(f"Starting GOLD build for {self.target_fullname}")
        self.ensure_schema()

        base = self.read_base()
        first_seen_grp = self.compute_first_seen_per_group(base)
        rep_grp = self.compute_rep_row_per_group(base)

        if not self.table_exists():
            logger.info("Table not found or stale. Creating initial snapshot...")
            dim0_full = self.compute_initial_dim(first_seen_grp, rep_grp)
            self.create_external_table(dim0_full)
            logger.info(f"Created {self.target_fullname} at {self.target_path}")
            return

        logger.info("Table found. Appending new groups (insert-only by representative name)...")
        dim_current_all = self.compute_initial_dim(first_seen_grp, rep_grp)

        tgt = self.spark.table(self.target_fullname).select("NombreCliente", "NumeroCliente")
        new_names = dim_current_all.join(
            tgt.select("NombreCliente"), on="NombreCliente", how="left_anti"
        ).distinct()

        if self._df_is_empty(new_names):
            logger.info("No new customer groups. Already up to date.")
            return

        max_n = tgt.select(F.max("NumeroCliente").alias("mx")).collect()[0]["mx"]
        max_n = int(max_n) if max_n is not None else 0

        w_new = Window.orderBy(F.col("first_seen_ts").asc(), F.col("group_key").asc())
        to_insert = new_names.withColumn(
            "rn_tmp", F.row_number().over(w_new) + F.lit(max_n)
        ).select(
            F.col("rn_tmp").cast("int").alias("NumeroCliente"),
            "Identificacion",
            "NombreCliente",
            "first_seen_ts",
            "group_key",
        )

        to_insert = (
            to_insert.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("group_key").orderBy(
                        F.col("first_seen_ts").asc(), F.col("NumeroCliente").asc()
                    )
                ),
            )
            .where(F.col("rn") == 1)
            .drop("rn")
            .dropDuplicates(["NombreCliente"])
        )

        self.merge_insert_new_names(to_insert)
        self.set_table_properties(self.table_properties)
        logger.info("Incremental insert completed.")
