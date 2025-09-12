# dim_forma_pago.py
"""
Seeds <catalog>.gold.dim_forma_pago (static payment methods).
"""

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import Row

from .base import BaseProcessor


class GoldDimFormaPagoProcessor(BaseProcessor):
    """Payment method dimension (seeded/static)."""

    TABLE_COMMENT = "Payment method dimension (seeded/static)."
    DDL_COLUMNS_SQL = (
        "IdFormaPago STRING COMMENT 'Payment method code', "
        "FormaPago STRING COMMENT 'Payment method name'"
    )

    def _seed_df(self) -> DF:
        """Small in-memory DataFrame with the required codes."""
        rows = [
            Row(IdFormaPago="000", FormaPago="Efectivo"),
            Row(IdFormaPago="008", FormaPago="Transferencia"),
        ]
        return self.spark.createDataFrame(rows).select("IdFormaPago", "FormaPago")

    def _merge_seed(self, df_seed: DF) -> None:
        """Upsert the seed into the Delta target."""
        delta_tgt = DeltaTable.forName(self.spark, self.target_fullname)
        (
            delta_tgt.alias("t")
            .merge(df_seed.alias("s"), "t.IdFormaPago = s.IdFormaPago")
            .whenNotMatchedInsert(
                values={"IdFormaPago": "s.IdFormaPago", "FormaPago": "s.FormaPago"}
            )
            .whenMatchedUpdate(set={"FormaPago": "s.FormaPago"})
            .execute()
        )

    def process(self) -> None:
        """Create if missing; otherwise ensure the two codes exist (idempotent)."""
        logger.info(f"Starting GOLD process for {self.target_fullname}")
        self.ensure_schema()

        seed = self._seed_df()

        if not self.table_exists():
            logger.info("Target table does not exist. Performing initial build...")
            self.create_external_table(
                df_initial=seed,
                ddl_columns_sql=self.DDL_COLUMNS_SQL,
                table_comment=self.TABLE_COMMENT,
                select_cols=["IdFormaPago", "FormaPago"],
            )
            logger.info(f"Created and loaded table {self.target_fullname} at {self.target_path}")
            return

        logger.info("Target table exists. Ensuring required codes via idempotent upsert…")
        self._merge_seed(seed)
        self.set_table_properties(self.table_properties)
        logger.info("Upsert completed; table is up to date.")
