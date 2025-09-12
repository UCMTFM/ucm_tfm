# base_gold.py
"""
Gold Layer — Base Processor
---------------------------
Core class used as a foundation for all GOLD processors in Unity Catalog.

Main responsibilities:
- Load configuration from DBFS or local JSON
- Resolve catalog/schema/table names and corresponding ADLS paths
- Provide utility functions for Spark and Delta (existence checks, DDL helpers, etc.)
"""

from typing import Dict, Any, List
import json
from loguru import logger

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

# Initialize Spark session and DBUtils
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class BaseGoldProcessor:
    """
    Shared base class for all GOLD processors.

    This class handles common logic like:
    - Loading configuration
    - Building fully qualified names for source/target tables
    - Generating external Delta table paths in ADLS
    - Providing reusable helpers for working with the metastore and Delta Lake
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config: Dict[str, Any] = self._load_config(config_path)

        # Extract catalog and table details from config
        self.catalog: str    = self.config["catalog"]
        self.src_schema: str = self.config["sources"]["main_schema"]
        self.src_table: str  = self.config["sources"]["main_table"]
        self.tgt_schema: str = self.config["sink"]["schema"]
        self.tgt_table: str  = self.config["sink"]["table"]

        # ADLS configuration
        self.account: str   = self.config["lakehouse_storage_account_name"]
        self.container: str = self.config.get("lakehouse_container_name", "lakehouse")

        # Optional: custom table properties (TBLPROPERTIES)
        self.table_properties: Dict[str, str] = self.config.get("table_properties", {})

    # -------------------- Configuration --------------------
    def _load_config(self, path: str) -> Dict[str, Any]:
        """
        Load a JSON configuration from DBFS or local disk.

        Args:
            path: Can be a DBFS path (starts with dbfs:/) or a local file path.

        Returns:
            Parsed config as a Python dictionary.
        """
        logger.info(f"Loading configuration from: {path}")
        if path.startswith("dbfs:/"):
            raw = dbutils.fs.head(path, 1024 * 1024)
            return json.loads(raw)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # -------------------- String Helpers --------------------
    @staticmethod
    def normalize_str_basic(col):
        """Trim whitespace, convert to uppercase, and collapse multiple spaces."""
        return F.regexp_replace(F.upper(F.trim(col)), r"\s+", " ")

    normalize_str = normalize_str_basic  # Alias for backward compatibility

    @staticmethod
    def normalize_str_core(col):
        """
        Normalize customer names by:
        - Removing non-alphanumeric noise
        - Stripping legal suffixes (e.g., S.A., LTDA.)
        - Collapsing multiple spaces and converting to uppercase
        """
        s = F.upper(F.trim(col))
        s = F.regexp_replace(s, r"[\u00A0]+", " ")  # Replace non-breaking space
        s = F.regexp_replace(s, r"[^A-Z0-9ÁÉÍÓÚÜÑ& ]+", " ")
        s = F.regexp_replace(s, r"\bS\s*\.?\s*A\s*\.?\s*S\b", "")
        s = F.regexp_replace(s, r"\bL\s*\.?\s*T\s*\.?\s*D\s*\.?\s*A\b", "")
        s = F.regexp_replace(s, r"\bS\s*\.?\s*A\b", "")
        s = F.regexp_replace(s, r"\bS\s*\.?\s*E\s*\.?\s*N\s*\.?\s*C\b", "")
        s = F.regexp_replace(s, r"\bY\s+CIA\b", "Y")
        s = F.regexp_replace(s, r"\s+", " ")
        return F.trim(s)

    @staticmethod
    def _canon_ident_for_tiebreak(col):
        """Simplify identifier by removing all non-alphanumeric characters."""
        return F.regexp_replace(col.cast("string"), r"[^0-9A-Za-z]", "")

    @staticmethod
    def _group_key(ident_clean_col: str, nombre_col: str):
        """
        Create a grouping key based on:
        - Cleaned identifier if available
        - Otherwise, use normalized name
        """
        return F.when(
            (F.col(ident_clean_col).isNotNull()) & (F.col(ident_clean_col) != ""),
            F.col(ident_clean_col)
        ).otherwise(F.col(nombre_col))

    # -------------------- DataFrame & Delta Helpers --------------------
    @staticmethod
    def _df_is_empty(df: DF) -> bool:
        """Check if a DataFrame is empty without accessing the underlying RDD (Spark Connect compatible)."""
        return df.limit(1).count() == 0

    def _path_has_delta(self) -> bool:
        """Return True if the target path points to a valid Delta table."""
        try:
            return DeltaTable.isDeltaTable(spark, self.target_path)
        except Exception:
            return False

    # -------------------- Naming & Paths --------------------
    @property
    def source_fullname(self) -> str:
        """Build the fully qualified name for the source table."""
        return f"{self.catalog}.{self.src_schema}.{self.src_table}"

    @property
    def target_fullname(self) -> str:
        """Build the fully qualified name for the target table."""
        return f"{self.catalog}.{self.tgt_schema}.{self.tgt_table}"

    @property
    def target_path(self) -> str:
        """Construct ADLS path for the external Delta table."""
        return f"abfss://{self.container}@{self.account}.dfs.core.windows.net/{self.tgt_schema}/{self.tgt_table}"

    # -------------------- Metastore Operations --------------------
    def ensure_schema(self) -> None:
        """Create the schema if it doesn't exist and attach a meaningful comment."""
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.tgt_schema}")
        spark.sql(f"COMMENT ON SCHEMA {self.catalog}.{self.tgt_schema} IS 'Gold layer - analytics ready'")

    def _metastore_has_table(self) -> bool:
        """Check if the table is registered in the metastore (and is not temporary)."""
        try:
            df = spark.sql(f"SHOW TABLES IN {self.catalog}.{self.tgt_schema} LIKE '{self.tgt_table}'")
            return df.filter(F.col("isTemporary") == False).limit(1).count() > 0
        except Exception as e:
            logger.warning(f"_metastore_has_table() failed; assuming no. Reason: {e}")
            return False

    def table_exists(self) -> bool:
        """
        Check if the table exists in both the metastore and at the storage path as a Delta table.

        If there's a stale metastore entry pointing to a non-Delta path, remove it.
        """
        in_metastore = self._metastore_has_table()
        path_is_delta = self._path_has_delta()

        if in_metastore and not path_is_delta:
            logger.warning(
                f"Found metastore entry for {self.target_fullname}, but the path is not a Delta table. "
                f"Dropping the stale entry."
            )
            try:
                spark.sql(f"DROP TABLE IF EXISTS {self.target_fullname}")
            except Exception as e:
                logger.warning(f"Failed to drop stale table: {e}")
            return False

        return in_metastore and path_is_delta

    def _table_is_empty(self) -> bool:
        """Return True if the target table has no records."""
        return self._df_is_empty(spark.table(self.target_fullname).limit(1))

    # -------------------- Table Creation --------------------
    def _spark_sql_type(self, dt):
        """Map Spark data types to SQL-compatible types for DDL generation."""
        name = dt.simpleString().lower()
        if name.startswith("int"):     return "INT"
        if name.startswith("bigint"):  return "BIGINT"
        if name.startswith("double"):  return "DOUBLE"
        if name.startswith("float"):   return "FLOAT"
        if name.startswith("decimal"): return name.upper()
        if name.startswith("date"):    return "DATE"
        if name.startswith("timestamp"): return "TIMESTAMP"
        if name.startswith("boolean"): return "BOOLEAN"
        return "STRING"

    def create_external_table(
        self,
        df_initial: DF,
        ddl_columns_sql,  # Can be a DDL string or a dict with {col: comment}
        table_comment: str,
        select_cols: List[str]
    ) -> None:
        """
        Create or register an external Delta table at the specified location.

        If the table is empty, insert the initial data (filtered to select_cols).
        """
        logger.info(f"Creating or registering external Delta table at: {self.target_path}")

        if isinstance(ddl_columns_sql, str):
            ddl_text = ddl_columns_sql
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.target_fullname} (
                    {ddl_text}
                )
                USING DELTA
                LOCATION '{self.target_path}'
                COMMENT '{table_comment}'
            """)
        else:
            # Generate column definitions dynamically with optional comments
            comments: Dict[str, str] = dict(ddl_columns_sql)
            fields = []
            for f in df_initial.schema.fields:
                col_name = f.name
                col_type = self._spark_sql_type(f.dataType)
                col_comment = comments.get(col_name, "")
                if col_comment:
                    fields.append(f"{col_name} {col_type} COMMENT '{col_comment}'")
                else:
                    fields.append(f"{col_name} {col_type}")
            ddl_text = ", ".join(fields).replace("'", "''")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.target_fullname} (
                    {ddl_text}
                )
                USING DELTA
                LOCATION '{self.target_path}'
                COMMENT '{table_comment}'
            """)

        self.set_table_properties(self.table_properties)

        if self._table_is_empty():
            (df_initial.select(*select_cols).writeTo(self.target_fullname).append())
            logger.info("Inserted initial data into the table.")
        else:
            logger.info("Table already contains data. Skipping initial load.")

    def set_table_properties(self, props: Dict[str, str]) -> None:
        """Apply custom TBLPROPERTIES to the Delta table (if provided)."""
        if not props:
            return
        props_sql = ", ".join([f"'{k}'='{v}'" for k, v in props.items()])
        spark.sql(f"ALTER TABLE {self.target_fullname} SET TBLPROPERTIES ({props_sql})")

    # -------------------- Specific Logic for Route Dimension --------------------
    def read_source_distinct_routes(self) -> DF:
        """Fetch distinct non-null `idRuta` values from the Silver source table."""
        logger.info(f"Fetching distinct routes from source: {self.source_fullname}")
        return (
            spark.table(self.source_fullname)
                 .select(F.col("idRuta").cast("string").alias("idRuta"))
                 .where(F.col("idRuta").isNotNull())
                 .distinct()
        )

    def read_target(self) -> DF:
        """Read minimal columns from the existing target dimension."""
        return spark.table(self.target_fullname).select("idRuta", "nombreRuta")

    @staticmethod
    def compute_initial_dimension(df_src: DF) -> DF:
        """Assign default route names (e.g., 'Ruta 1', 'Ruta 2', ...) ordered by idRuta."""
        from pyspark.sql.window import Window
        w = Window.orderBy(F.col("idRuta").asc())
        return (
            df_src.withColumn("numero_ruta", F.row_number().over(w))
                  .select(
                      F.col("idRuta"),
                      F.concat(F.lit("Ruta "), F.col("numero_ruta").cast("string")).alias("nombreRuta")
                  )
        )

    def find_new_routes(self, df_src: DF, df_tgt: DF) -> DF:
        """Identify new route IDs that are in the source but not yet in the target."""
        return df_src.join(df_tgt.select("idRuta"), on="idRuta", how="left_anti").distinct()

    def get_existing_max_sequence(self, df_tgt: DF) -> int:
        """Find the highest numeric suffix from existing 'Ruta N' entries."""
        row = (
            df_tgt
            .select(F.max(F.regexp_extract(F.col("nombreRuta"), r"(\d+)$", 1).cast("int")).alias("max_n"))
            .collect()[0]
        )
        return int(row["max_n"] or 0)

    def build_inserts_for_new_routes(self, df_new_ids: DF, start_from: int) -> DF:
        """Assign new route numbers starting from the next available sequence."""
        from pyspark.sql.window import Window
        w = Window.orderBy(F.col("idRuta").asc())
        return (
            df_new_ids
            .withColumn("seq", F.row_number().over(w) + F.lit(start_from))
            .select(
                F.col("idRuta"),
                F.concat(F.lit("Ruta "), F.col("seq").cast("string")).alias("nombreRuta")
            )
        )

    def merge_upsert_new_routes(self, df_to_insert: DF) -> None:
        """Insert only the new routes into the target Delta table via MERGE."""
        logger.info("Merging new routes into GOLD dimension table")
        delta_tgt = DeltaTable.forName(spark, self.target_fullname)
        (delta_tgt.alias("t")
                 .merge(df_to_insert.alias("s"), "t.idRuta = s.idRuta")
                 .whenNotMatchedInsert(values={
                     "idRuta": "s.idRuta",
                     "nombreRuta": "s.nombreRuta"
                 })
                 .execute())