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
    """
    GOLD payment method dimension seeded with a fixed, small set of codes.

    Workflow:
        1. Build a tiny in-memory seed with the required payment method codes.
        2. If the target table is missing, create the external Delta table and load the seed.
        3. If the table exists, upsert the seed to ensure codes/names are present and up to date.
        4. Apply optional table properties from configuration.

    Inherits from:
        BaseProcessor: Provides Spark handles, configuration loading, schema management,
        and shared Delta utilities used by this processor.
    """  

    TABLE_COMMENT = "Payment method dimension (seeded/static)."
    DDL_COLUMNS_SQL = (
        "IdFormaPago STRING COMMENT 'Payment method code', "
        "FormaPago STRING COMMENT 'Payment method name'"
    )

    def _seed_df(self) -> DF:
        """
        Build the in-memory seed for the dimension.

        Returns:
            DF: DataFrame with the canonical set of payment methods,
                including columns `IdFormaPago` and `FormaPago`.
        """  
        rows = [
            Row(IdFormaPago="000", FormaPago="Efectivo"),
            Row(IdFormaPago="008", FormaPago="Transferencia"),
        ]
        return self.spark.createDataFrame(rows).select("IdFormaPago", "FormaPago")

    def _merge_seed(self, df_seed: DF) -> None:
        """
        Upsert the seed rows into the Delta target (idempotent).

        Args:
            df_seed (DF): Seed DataFrame with `IdFormaPago` and `FormaPago`.

        Returns:
            None
        """  
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
        """
        Create the table on first run and ensure the seed codes exist on subsequent runs.

        Steps:
            1) Ensure the target schema exists.
            2) Build the in-memory seed DataFrame.
            3) If the table does not exist: create/register the external Delta table and seed it.
            4) Otherwise: upsert the seed (insert new codes / refresh names).
            5) Apply table properties if configured.

        Returns:
            None
        """  
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
