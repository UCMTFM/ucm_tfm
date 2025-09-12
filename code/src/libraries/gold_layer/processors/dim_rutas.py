# dim_rutas.py
"""
Builds <catalog>.gold.dim_rutas from silver.facturas.
"""

from loguru import logger

from .base import BaseProcessor


class GoldDimRutasProcessor(BaseProcessor):
    """Route dimension built from silver.facturas with stable 'Ruta <N>' labels by idRuta."""

    TABLE_COMMENT = (
        "Route dimension from silver.facturas. Stable 'Ruta <N>' by idRuta (append-only)."
    )
    DDL_COLUMNS_SQL = (
        "idRuta STRING COMMENT 'Route ID from silver.facturas', "
        "nombreRuta STRING COMMENT 'Generated label: Ruta <N>'"
    )

    def process(self) -> None:
        """
        Build/refresh flow:
          1) Read distinct idRuta
          2) If table is missing, create and seed numbering
          3) Otherwise, append new idRuta with the next sequence
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
