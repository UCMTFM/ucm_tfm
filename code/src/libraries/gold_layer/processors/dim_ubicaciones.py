# dim_ubicaciones.py
"""
Builds <catalog>.gold.dim_ubicaciones from silver.facturas.
(Phase A: NumeroCliente + Lat/Long; Phase B: geo enrichment from local shapefiles.)
"""


from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .base import BaseProcessor


class GoldDimUbicacionesProcessor(BaseProcessor):
    """Location dimension from silver.facturas; Phase A (Lat/Long) + Phase B (admin geo fields)"""

    TABLE_COMMENT = (
        "Locations dimension (gold). Same NumeroCliente logic; "
        "includes Latitud/Longitud from representative row; "
        "geo enrichment (Departamento/Municipio/Cod_DANE/Distrito/Barrio) via local shapefiles."
    )
    DDL_COLUMNS_SQL = (
        "NumeroCliente INT COMMENT 'Surrogate customer number (stable, incremental "
        "by first-seen group)', Latitud DOUBLE COMMENT 'Latitude from representative "
        "first-seen row', Longitud DOUBLE COMMENT 'Longitude from representative first-seen row', "
        "Cod_DANE STRING COMMENT 'Código DANE (from MGN)', "
        "Departamento STRING COMMENT 'Departamento (from ADM1)', "
        "Municipio STRING COMMENT 'Municipio (from ADM2)', "
        "Distrito STRING COMMENT 'Distrito/Comuna/Localidad (local layers)', "
        "Barrio STRING COMMENT 'Barrio/Neighborhood (local layers)'"
    )

    def read_base(self) -> DF:
        """Read minimal fields required to compute group_key and representative coordinates."""
        f = self.spark.table(self.source_fullname).select(
            F.col("Identificacion").cast("string").alias("Identificacion_raw"),
            self.normalize_str_core(F.col("RazonSocialCliente")).alias("NombreCliente"),
            F.to_timestamp(F.col("FechaCreacion")).alias("ts_creacion"),
            F.col("Anulado").cast("boolean").alias("Anulado"),
            F.col("Latitud").cast("double").alias("Latitud"),
            F.col("Longitud").cast("double").alias("Longitud"),
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
                F.col("Latitud"),
                F.col("Longitud"),
            )
        )

        return base.withColumn(
            "group_key", self._group_key("Identificacion_clean", "NombreCliente")
        )

    def compute_first_seen_per_group(self, base: DF) -> DF:
        """First appearance timestamp per group_key."""
        return base.groupBy("group_key").agg(F.min("ts_creacion").alias("first_seen_ts"))

    def compute_rep_row_per_group(self, base: DF) -> DF:
        """Representative row per group_key with deterministic order; carry Lat/Long."""
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
                F.col("Latitud").alias("Latitud_rep"),
                F.col("Longitud").alias("Longitud_rep"),
            )
        )

    def compute_initial_dim(self, first_seen_grp: DF, rep_grp: DF) -> DF:
        """Assign NumeroCliente and project representative coordinates."""
        w_dense = Window.orderBy(F.col("first_seen_ts").asc(), F.col("group_key").asc())
        numbered = first_seen_grp.withColumn("NumeroCliente", F.dense_rank().over(w_dense)).select(
            "group_key", "NumeroCliente", "first_seen_ts"
        )

        return numbered.join(rep_grp, on="group_key", how="inner").select(
            F.col("NumeroCliente").cast("int").alias("NumeroCliente"),
            F.col("Identificacion_rep").alias("Identificacion"),
            F.col("NombreCliente_rep").alias("NombreCliente"),
            F.col("Latitud_rep").alias("Latitud"),
            F.col("Longitud_rep").alias("Longitud"),
            "first_seen_ts",
            "group_key",
        )

    # -------------------- TABLE CREATE (Phase A) --------------------
    def create_external_table(self, df_initial: DF) -> None:
        """
        Create table and seed rows with admin columns set to NULL; drop rows without coordinates.
        """
        df_initial = df_initial.dropDuplicates(["NombreCliente"])
        df_initial = (
            df_initial.withColumn("Cod_DANE", F.lit(None).cast("string"))
            .withColumn("Departamento", F.lit(None).cast("string"))
            .withColumn("Municipio", F.lit(None).cast("string"))
            .withColumn("Distrito", F.lit(None).cast("string"))
            .withColumn("Barrio", F.lit(None).cast("string"))
            .where(F.col("Latitud").isNotNull() & F.col("Longitud").isNotNull())
        )

        super().create_external_table(
            df_initial=df_initial.select(
                "NumeroCliente",
                "Latitud",
                "Longitud",
                "Cod_DANE",
                "Departamento",
                "Municipio",
                "Distrito",
                "Barrio",
            ),
            ddl_columns_sql=self.DDL_COLUMNS_SQL,
            table_comment=self.TABLE_COMMENT,
            select_cols=[
                "NumeroCliente",
                "Latitud",
                "Longitud",
                "Cod_DANE",
                "Departamento",
                "Municipio",
                "Distrito",
                "Barrio",
            ],
        )

    # -------------------- MERGE (insert-only) Phase A --------------------
    def merge_insert_new(self, df_new: DF) -> None:
        """Insert new NumeroCliente rows (admin columns set to NULL)."""
        logger.info("Running MERGE (insert-only by NumeroCliente) into GOLD.dim_ubicaciones")

        from pyspark.sql.window import Window

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
            .withColumn("Cod_DANE", F.lit(None).cast("string"))
            .withColumn("Departamento", F.lit(None).cast("string"))
            .withColumn("Municipio", F.lit(None).cast("string"))
            .withColumn("Distrito", F.lit(None).cast("string"))
            .withColumn("Barrio", F.lit(None).cast("string"))
            .where(F.col("Latitud").isNotNull() & F.col("Longitud").isNotNull())
            .select(
                "NumeroCliente",
                "Latitud",
                "Longitud",
                "Cod_DANE",
                "Departamento",
                "Municipio",
                "Distrito",
                "Barrio",
            )
        )

        delta_tgt = DeltaTable.forName(self.spark, self.target_fullname)
        (
            delta_tgt.alias("t")
            .merge(df_new.alias("s"), "t.NumeroCliente = s.NumeroCliente")
            .whenNotMatchedInsert(
                values={
                    "NumeroCliente": "s.NumeroCliente",
                    "Latitud": "s.Latitud",
                    "Longitud": "s.Longitud",
                    "Cod_DANE": "s.Cod_DANE",
                    "Departamento": "s.Departamento",
                    "Municipio": "s.Municipio",
                    "Distrito": "s.Distrito",
                    "Barrio": "s.Barrio",
                }
            )
            .execute()
        )

    def _read_json_dbfs(self, path: str) -> dict:
        """Small helper to read JSON config from DBFS."""
        raw = self.dbutils.fs.head(path, 1024 * 1024)
        import json as _json

        return _json.loads(raw)

    def _workspace_geo_base(self) -> str:
        return "/Workspace/Shared/geospatial_stage"

    @staticmethod
    def _zfill_or_none(x, width):
        """Zero-pad codes while being robust to NaNs and 'float-like' strings."""
        import pandas as _pd

        if _pd.isna(x):
            return None
        s = str(x).strip()
        if "." in s:
            try:
                s = str(int(float(s)))
            except Exception:
                pass
        return s.zfill(width)

    def _load_layer_local(self, rel_path: str):
        """Load a local (workspace) vector layer with EPSG:4326 geometry."""
        import os

        import geopandas as gpd

        p = os.path.join(self._workspace_geo_base(), rel_path)
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing layer: {p}")
        gdf = gpd.read_file(p)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        return gdf

    def _apply_local_layer(self, pts, layer_conf: dict, out_col: str):
        """
        Fill a textual admin attribute (Distrito, Barrio) from a local polygon layer,
        restricted by candidate city names or Cod_DANE when provided in config.
        """
        import geopandas as gpd
        import pandas as pd

        path = layer_conf["path"]
        name_field = layer_conf.get("name_field") or layer_conf.get("district_field")
        muni_patterns = [p.upper() for p in layer_conf.get("municipio_match", [])]
        dane_list = set(layer_conf.get("cod_dane_match", []))

        if out_col not in pts.columns:
            pts[out_col] = pd.NA

        mask = pd.Series(False, index=pts.index)
        if muni_patterns and "Municipio" in pts.columns:
            muni_upper = pts["Municipio"].astype("string").str.upper()
            sub = pd.Series(False, index=pts.index)
            for pat in muni_patterns:
                sub = sub | muni_upper.str.contains(pat, na=False)
            mask = mask | sub
        if dane_list and "Cod_DANE" in pts.columns:
            mask = mask | pts["Cod_DANE"].astype("string").isin(dane_list)

        mask = mask & pts[out_col].isna()
        if not mask.any():
            logger.info(f"(info) {layer_conf.get('id')} → 0 candidate rows for '{out_col}'.")
            return pts

        gdf_poly = self._load_layer_local(path)
        if name_field not in gdf_poly.columns:
            logger.warning(
                f"(warning) {layer_conf.get('id')} missing '{name_field}'. "
                f"Columns: {list(gdf_poly.columns)[:6]} ... → skipping."
            )
            return pts

        gdf_poly = gdf_poly[[name_field, "geometry"]].copy()

        pts_city = pts.loc[mask].copy().reset_index(names="_idx")
        joined = gpd.sjoin(pts_city, gdf_poly, how="left", predicate="intersects").drop(
            columns=["index_right"]
        )

        first = joined.sort_values("_idx").drop_duplicates("_idx", keep="first").set_index("_idx")
        first = first.reindex(pts_city["_idx"])

        pts.loc[mask, out_col] = first[name_field].astype("string").values
        logger.info(
            f"(ok) {layer_conf.get('id')}: '{out_col}' <- '{name_field}' "
            f"assigned to {first[name_field].notna().sum()} points."
        )
        return pts

    def _enrich_geospatial_pdf(self, pdf):
        """
        Enrich a pandas DataFrame [NumeroCliente, Latitud, Longitud] with:
        Departamento, Municipio, Cod_DANE, Distrito, Barrio.
        """
        import os

        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import Point

        # 1) Load configs
        CFG = self._read_json_dbfs("dbfs:/FileStore/config/gold/dim_ubicaciones_global_config.json")
        DIST = self._read_json_dbfs(
            "dbfs:/FileStore/config/gold/dim_ubicaciones_distritos_config.json"
        )
        try:
            BAR = self._read_json_dbfs(
                "dbfs:/FileStore/config/gold/dim_ubicaciones_barrios_config.json"
            )
        except Exception:
            BAR = {"layers": []}

        # 2) Points GeoDataFrame
        gpts = gpd.GeoDataFrame(
            pdf.copy(),
            geometry=[Point(xy) for xy in zip(pdf["Longitud"], pdf["Latitud"])],
            crs="EPSG:4326",
        )

        # 3) ADM1 / ADM2 join
        adm1_conf = CFG["adm_layers"]["adm1"]
        adm2_conf = CFG["adm_layers"]["adm2"]

        adm1 = self._load_layer_local(adm1_conf["path"])
        adm2 = self._load_layer_local(adm2_conf["path"])
        name1 = adm1_conf["name_field"]
        name2 = adm2_conf["name_field"]

        adm1 = adm1[[name1, "geometry"]].rename(columns={name1: "Departamento"})
        adm2 = adm2[[name2, "geometry"]].rename(columns={name2: "Municipio"})

        gpts = gpd.sjoin(gpts, adm1, how="left", predicate="intersects").drop(
            columns=["index_right"]
        )
        gpts = gpd.sjoin(gpts, adm2, how="left", predicate="intersects").drop(
            columns=["index_right"]
        )

        # 4) MGN (Cod_DANE)
        mgn = CFG["mgn"]
        urb = self._load_layer_local(mgn["urbano"]["path"])
        dcol = mgn["urbano"]["dept_code_field"]
        mcol = mgn["urbano"]["muni_code_field"]

        urb = urb[[dcol, mcol, "geometry"]]
        gpts = gpd.sjoin(gpts, urb, how="left", predicate="intersects").drop(
            columns=["index_right"]
        )

        rur_path = (mgn.get("rural") or {}).get("path")
        if rur_path and os.path.exists(os.path.join(self._workspace_geo_base(), rur_path)):
            rur = self._load_layer_local(rur_path)
            if dcol in rur.columns and mcol in rur.columns:
                rur = rur[[dcol, mcol, "geometry"]]
                mask = gpts[dcol].isna() | gpts[mcol].isna()
                if mask.any():
                    fill = gpd.sjoin(gpts.loc[mask], rur, how="left", predicate="intersects")
                    gpts.loc[mask, dcol] = fill.get(f"{dcol}_right", fill.get(dcol)).values
                    gpts.loc[mask, mcol] = fill.get(f"{mcol}_right", fill.get(mcol)).values

        gpts["cod_dpto"] = gpts[dcol].apply(lambda x: self._zfill_or_none(x, 2))
        gpts["cod_mpio_3d"] = gpts[mcol].apply(lambda x: self._zfill_or_none(x, 3))
        gpts["Cod_DANE"] = [
            (a or "") + (b or "") if (a and b) else None
            for a, b in zip(gpts["cod_dpto"], gpts["cod_mpio_3d"])
        ]

        # 5) Local layers: Distrito / Barrio
        for layer in DIST.get("layers", []):
            gpts = self._apply_local_layer(gpts, layer_conf=layer, out_col="Distrito")
        for layer in BAR.get("layers", []) or []:
            gpts = self._apply_local_layer(gpts, layer_conf=layer, out_col="Barrio")

        # 6) Output pandas frame with required columns
        out_cols = ["NumeroCliente", "Departamento", "Municipio", "Cod_DANE", "Distrito", "Barrio"]
        for c in out_cols:
            if c not in gpts.columns:
                gpts[c] = pd.NA
        return gpts[out_cols].copy()

    def _phase_b_update(self, sdf_scope: DF) -> None:
        """
        Run geo enrichment for provided scope (NumeroCliente, Latitud, Longitud) and
        upsert enriched attributes into the Delta target using COALESCE updates.
        """
        if self._df_is_empty(sdf_scope):
            logger.info("Phase B: nothing to enrich (empty scope).")
            return

        sdf_scope = (
            sdf_scope.where(F.col("Latitud").isNotNull() & F.col("Longitud").isNotNull())
            .select("NumeroCliente", "Latitud", "Longitud")
            .dropDuplicates(["NumeroCliente"])
        )

        if self._df_is_empty(sdf_scope):
            logger.info("Phase B: scope has no valid coordinates.")
            return

        pdf = sdf_scope.toPandas()
        logger.info(f"Phase B: enriching {len(pdf)} points…")
        pdf_enriched = self._enrich_geospatial_pdf(pdf)

        non_null_mask = (
            pdf_enriched["Departamento"].notna()
            | pdf_enriched["Municipio"].notna()
            | pdf_enriched["Cod_DANE"].notna()
            | pdf_enriched["Distrito"].notna()
            | pdf_enriched["Barrio"].notna()
        )
        pdf_enriched = pdf_enriched.loc[non_null_mask].copy()
        if len(pdf_enriched) == 0:
            logger.info("Phase B: no new admin attributes to apply.")
            return

        sdf_upd = self.spark.createDataFrame(pdf_enriched)

        delta_tgt = DeltaTable.forName(self.spark, self.target_fullname)
        (
            delta_tgt.alias("t")
            .merge(sdf_upd.alias("s"), "t.NumeroCliente = s.NumeroCliente")
            .whenMatchedUpdate(
                set={
                    "Cod_DANE": "coalesce(s.Cod_DANE, t.Cod_DANE)",
                    "Departamento": "coalesce(s.Departamento, t.Departamento)",
                    "Municipio": "coalesce(s.Municipio, t.Municipio)",
                    "Distrito": "coalesce(s.Distrito, t.Distrito)",
                    "Barrio": "coalesce(s.Barrio, t.Barrio)",
                }
            )
            .execute()
        )
        logger.info(f"Phase B: updated {sdf_upd.count()} rows with geo attributes.")

    def process(self) -> None:
        """Build initial table (Phase A + B) or perform incremental inserts + enrichment."""
        logger.info(f"Starting GOLD process for {self.target_fullname}")
        self.ensure_schema()

        base = self.read_base()
        first_seen_grp = self.compute_first_seen_per_group(base)
        rep_grp = self.compute_rep_row_per_group(base)

        if not self.table_exists():
            logger.info("Target table does not exist or is stale. Performing initial build...")
            dim0_full = self.compute_initial_dim(first_seen_grp, rep_grp)
            self.create_external_table(dim0_full)
            self.set_table_properties(self.table_properties)
            logger.info(f"Created and loaded table {self.target_fullname} at {self.target_path}")

            # Phase B over any row with coords and missing admin attributes
            scope = (
                self.spark.table(self.target_fullname)
                .select(
                    "NumeroCliente",
                    "Latitud",
                    "Longitud",
                    "Departamento",
                    "Municipio",
                    "Cod_DANE",
                    "Distrito",
                    "Barrio",
                )
                .where(
                    (F.col("Latitud").isNotNull() & F.col("Longitud").isNotNull())
                    & (
                        F.col("Departamento").isNull()
                        | F.col("Municipio").isNull()
                        | F.col("Cod_DANE").isNull()
                        | F.col("Distrito").isNull()
                        | F.col("Barrio").isNull()
                    )
                )
            )
            self._phase_b_update(scope)
            return

        logger.info("Target table exists. Performing incremental insert (mirror logic)...")
        dim_current_all = self.compute_initial_dim(first_seen_grp, rep_grp)

        tgt = self.spark.table(self.target_fullname).select("NumeroCliente")
        new_rows = dim_current_all.join(tgt, on="NumeroCliente", how="left_anti").distinct()

        if self._df_is_empty(new_rows):
            logger.info("No new groups to insert. Dimension is up to date.")
            return

        max_n = tgt.select(F.max("NumeroCliente").alias("mx")).collect()[0]["mx"]
        max_n = int(max_n) if max_n is not None else 0

        w_new = Window.orderBy(F.col("first_seen_ts").asc(), F.col("group_key").asc())
        to_insert = new_rows.withColumn("rn_tmp", F.row_number().over(w_new) + F.lit(max_n)).select(
            F.col("rn_tmp").cast("int").alias("NumeroCliente"),
            "first_seen_ts",
            "group_key",
            "NombreCliente",
            "Latitud",
            "Longitud",
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
            .withColumn("Cod_DANE", F.lit(None).cast("string"))
            .withColumn("Departamento", F.lit(None).cast("string"))
            .withColumn("Municipio", F.lit(None).cast("string"))
            .withColumn("Distrito", F.lit(None).cast("string"))
            .withColumn("Barrio", F.lit(None).cast("string"))
            .where(F.col("Latitud").isNotNull() & F.col("Longitud").isNotNull())
            .select(
                "NumeroCliente",
                "Latitud",
                "Longitud",
                "Cod_DANE",
                "Departamento",
                "Municipio",
                "Distrito",
                "Barrio",
            )
        )

        # Phase A: insert
        self.merge_insert_new(to_insert)

        # Phase B: enrich only the freshly inserted rows
        self._phase_b_update(to_insert.select("NumeroCliente", "Latitud", "Longitud"))

        self.set_table_properties(self.table_properties)
        logger.info("Incremental insert + geo enrichment completed successfully.")
