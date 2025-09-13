# dim_rutas.py
"""
Builds <catalog>.gold.dim_rutas from silver.facturas.
"""

from loguru import logger

from .base import BaseProcessor

class GoldDimRutasProcessor(BaseProcessor):
    """
    Processor for building the GOLD route dimension (`<catalog>.gold.dim_rutas`)
    from the Silver source `silver.facturas`.

    Workflow:
        1. Reads distinct `idRuta` values from the Silver layer.
        2. If the target table is missing or stale, creates the external Delta table
           at the configured LOCATION and seeds human-friendly names as Ruta <N>
           ordered deterministically by `idRuta`.
        3. If the table exists, detects newly seen `idRuta`, computes the next sequence
           starting from the current maximum, and appends them via an insert-only MERGE.
        4. Applies optional table properties specified in the configuration.

    Inherits from:
        BaseProcessor: Provides Spark/DBFS handles and shared helpers for schema
        management and Delta operations used by this processor.
    """ 

    TABLE_COMMENT = "Routes dimension (gold). Built from silver.facturas with stable numbering by idRuta."

    DDL_COLUMNS_SQL = (
        "idRuta STRING COMMENT 'Route identifier (string) sourced from silver.facturas', "
        "nombreRuta STRING COMMENT 'Human-friendly name generated as Ruta <N>'"
    )

    def process(self) -> None:
        """
        Orchestrate the build/refresh of `<catalog>.gold.dim_rutas`.

        Steps:
            1) Ensure target schema exists.
            2) Read distinct route identifiers from the Silver source.
            3) If the table does not exist (or is stale):
                - Compute initial numbering and create/register the external table.
                - Seed rows with `(idRuta, nombreRuta)`.
            4) Else (table exists):
                - Find new `idRuta` not present in the target.
                - Compute next sequence from current max and build Ruta <N>.
                - MERGE insert-only the new routes.
            5) Apply optional table properties from configuration.

        Returns:
            None
        """  
        logger.info(f"Starting GOLD build for {self.target_fullname}")
        self.ensure_schema()

        src = self.read_source_distinct_routes()

        if not self.table_exists():
            logger.info("Table not found. Creating initial snapshot...")
            dim0 = self.compute_initial_dimension(src)
            self.create_external_table(
                df_initial=dim0,
                ddl_columns_sql=self.DDL_COLUMNS_SQL,
                table_comment=self.TABLE_COMMENT,
                select_cols=["idRuta", "nombreRuta"],
            )
            logger.info(f"Created {self.target_fullname} at {self.target_path}")
            return

        logger.info("Table found. Appending new routes (insert-only)...")
        tgt = self.read_target()
        new_ids = self.find_new_routes(src, tgt)

        if self._df_is_empty(new_ids):
            logger.info("No new routes. Already up to date.")
            return

        max_n = self.get_existing_max_sequence(tgt)
        to_insert = self.build_inserts_for_new_routes(new_ids, start_from=max_n)
        self.merge_upsert_new_routes(to_insert)
        self.set_table_properties(self.table_properties)
        logger.info("Append completed successfully.")
