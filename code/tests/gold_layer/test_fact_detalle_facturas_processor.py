import json
from datetime import datetime
from unittest.mock import Mock
import types
import pytest

from databricks.connect import DatabricksSession as SparkSession
import libraries.gold_layer.processors.base as base_module
from libraries.gold_layer.processors.fact_detalle_facturas import GoldFactDetalleFacturasProcessor
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, BooleanType, IntegerType,
    StructType, StructField, TimestampType
)

# ----------------------------- Fixtures -----------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Provide a SparkSession backed by databricks_connect for the whole test session.
    Keep shuffle partitions low to speed up local runs.
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    yield spark
    spark.stop()


@pytest.fixture
def config_dict():
    """
    Minimal config expected by the BaseProcessor + this processor.
    """
    return {
        "catalog": "testcat",
        "sources": {"main_schema": "silver", "main_table": "facturas"},
        "sink": {"schema": "gold", "table": "fact_detalle_facturas"},
        "lakehouse_storage_account_name": "dummyacct",
        "lakehouse_container_name": "lakehouse",
        "table_properties": {},
    }


@pytest.fixture
def config_file(tmp_path, config_dict):
    """
    Write the config to a temporary JSON file and return its path.
    """
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)


@pytest.fixture
def processor(config_file, spark, monkeypatch):
    """
    Instantiate the processor with a monkeypatched BaseProcessor.__init__
    to avoid DBFS/secrets and to inject the provided SparkSession.
    """
    def fake_base_init(self, config_path: str):
        import json as _json
        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark
        # set common attrs used by BaseProcessor helpers
        self.catalog = self.config.get("catalog")
        self.tgt_schema = self.config["sink"]["schema"]
        self.tgt_table  = self.config["sink"]["table"]
        self.table_properties = self.config.get("table_properties", {})

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return GoldFactDetalleFacturasProcessor(config_file)

# ----------------------------- Synthetic Sources -----------------------------

def _schemas():
    detalle_schema = StructType([
        StructField("IdFactura",                   StringType(),  True),
        StructField("NombreProducto",              StringType(),  True),
        StructField("Cantidad",                    DoubleType(),  True),
        StructField("ValorUnitario",               DoubleType(),  True),
        StructField("ValorDescuento",              DoubleType(),  True),
        StructField("PorcentajeIva",               DoubleType(),  True),
        StructField("ValorIva",                    DoubleType(),  True),
        StructField("PorcentajeImpuestoSaludable", DoubleType(),  True),
        StructField("ValorImpuestoSaludable",      DoubleType(),  True),
        StructField("Subtotal",                    DoubleType(),  True),
        StructField("Total",                       DoubleType(),  True),
        StructField("FechaCreacion",               TimestampType(), True),  # not used but realistic
    ])
    facturas_schema = StructType([
        StructField("IdFactura",     StringType(), True),
        StructField("Fecha",         TimestampType(), True),
        StructField("FechaCreacion", TimestampType(), True),
        StructField("Anulado",       BooleanType(), True),
    ])
    dim_prod_schema = StructType([
        StructField("IdProducto",     IntegerType(), True),
        StructField("NombreProducto", StringType(),  True),
    ])
    return detalle_schema, facturas_schema, dim_prod_schema


def _data_initial(spark):
    """
    Initial load:
      - F-1 appears twice (exact duplicate) -> must deduplicate by RowHash.
      - F-2 appears once but is annulled at header level -> must be filtered out.
    """
    detalle_schema, facturas_schema, _ = _schemas()

    src_det_initial = spark.createDataFrame([
        ("F-1", "GELATINA BALNCA", 2.0, 10.0, 0.0, 19.0, 3.8,  0.0, 0.0, 20.0, 23.8, datetime(2024,1,10,9,0,0)),
        ("F-1", "GELATINA BALNCA", 2.0, 10.0, 0.0, 19.0, 3.8,  0.0, 0.0, 20.0, 23.8, datetime(2024,1,10,9,0,0)),  # duplicate
        ("F-2", "OFERTA PACK",     1.0, 15.0, 0.0, 19.0, 2.85, 0.0, 0.0, 15.0, 17.85, datetime(2024,1,10,10,0,0)),
    ], schema=detalle_schema)

    src_fact_initial = spark.createDataFrame([
        ("F-1", datetime(2024,1,10,10,0,0), None, False),
        ("F-2", datetime(2024,1,10,11,0,0), None, True),   # annulled
    ], schema=facturas_schema)

    return src_det_initial, src_fact_initial


def _data_incremental(spark):
    """
    Incremental load:
      - Re-sends the F-1 duplicate row (must be dropped by anti-join on RowHash).
      - Adds new F-3 with a name that needs fix: LAMINA ROSUQILLA X 12 → LAMINA ROSQUILLA X 12.
    """
    detalle_schema, facturas_schema, _ = _schemas()

    src_det_incremental = spark.createDataFrame([
        ("F-1", "GELATINA BALNCA",        2.0, 10.0, 0.0, 19.0, 3.8,  0.0, 0.0, 20.0, 23.8, datetime(2024,1,10,9,0,0)),
        ("F-3", "LAMINA ROSUQILLA X 12",  4.0,  5.0, 0.0, 19.0, 3.8,  0.0, 0.0, 20.0, 23.8, datetime(2024,1,12,9,0,0)),
    ], schema=detalle_schema)

    src_fact_incremental = spark.createDataFrame([
        ("F-3", datetime(2024,1,12,9,0,0), None, False),
    ], schema=facturas_schema)

    return src_det_incremental, src_fact_incremental


def _dim_productos(spark):
    _, _, dim_prod_schema = _schemas()
    return spark.createDataFrame([
        (1, "GELATINA BLANCA"),
        (2, "PAQUETON X 50"),
        (3, "LAMINA ROSQUILLA X 12"),
    ], schema=dim_prod_schema)

# ----------------------------- Tests -----------------------------

class TestFactDetalleFacturasInitialAndIncremental:
    """
    Covers:
      - Initial build: filters annulled headers, normalizes+fixes names, maps IdProducto, de-duplicates by RowHash.
      - Incremental: insert-only of truly new lines via RowHash anti-join; late-binding remains possible.
    """

    def test_initial_build_and_incremental_insert_only(self, spark, processor, monkeypatch):
        # -- Holders and flags
        created_holder = {"df": None}
        merged_holder  = {"df": None}
        table_exists   = {"val": False}
        phase          = {"mode": "initial"}
        target_snapshot = {"rowhash_only": None}

        # -- No-op schema/props
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(processor, "set_table_properties", Mock(return_value=None))
        processor.table_exists = lambda: table_exists["val"]

        # -- Capture Base.create_external_table (initial)
        def _capture_super_create(self, df_initial, ddl_columns_sql, table_comment, select_cols):
            created_holder["df"] = df_initial.select(*select_cols)
        monkeypatch.setattr(base_module.BaseProcessor, "create_external_table", _capture_super_create)

        # -- Capture processor.merge_upsert (incremental)
        def _capture_merge(self, df_new):
            merged_holder["df"] = df_new
        processor.merge_upsert = types.MethodType(_capture_merge, processor)

        # -- Monkeypatch spark.table to serve synthetic sources + target
        dim = _dim_productos(spark)
        det_init, fact_init = _data_initial(spark)
        det_inc,  fact_inc  = _data_incremental(spark)

        original_spark_table = spark.table
        def _fake_table(self, fullname: str):
            cat = processor.catalog
            if fullname == f"{cat}.silver.detalle_facturas":
                return det_init if phase["mode"] == "initial" else det_inc
            if fullname == f"{cat}.silver.facturas":
                return fact_init if phase["mode"] == "initial" else fact_inc
            if fullname == f"{cat}.gold.dim_productos":
                return dim
            if fullname == processor.target_fullname:
                # For the incremental anti-join by RowHash
                return (target_snapshot["rowhash_only"]
                        if target_snapshot["rowhash_only"] is not None
                        else spark.createDataFrame([], StructType([StructField("RowHash", StringType(), True)])))
            return original_spark_table(fullname)
        spark.table = types.MethodType(_fake_table, spark)

        # ---------------- Initial build ----------------
        table_exists["val"] = False
        phase["mode"] = "initial"
        processor.process()

        created_df = created_holder["df"]
        assert created_df is not None

        # Expect exactly one row (F-1) after: filter annulled header F-2 + drop exact duplicate by RowHash.
        rows_init = created_df.collect()
        assert len(rows_init) == 1
        r0 = rows_init[0]
        assert r0["IdFactura"] == "F-1"
        assert r0["IdProducto"] == 1  # GELATINA BALNCA -> fixed/norm -> GELATINA BLANCA
        # Golden schema columns must be present
        assert {"RowHash","Cantidad","ValorUnitario","ValorDescuento","PorcentajeIva","ValorIva",
                "PorcentajeImpuestoSaludable","ValorImpuestoSaludable","Subtotal","Total"} <= set(created_df.columns)

        target_snapshot["rowhash_only"] = created_df.select("RowHash")

        # ---------------- Incremental (insert-only) ----------------
        table_exists["val"] = True
        phase["mode"] = "incremental"
        processor.process()

        merged_df = merged_holder["df"]
        assert merged_df is not None
        rows_inc = merged_df.collect()
        assert len(rows_inc) == 1  # only the new line F-3
        ri = rows_inc[0]
        assert ri["IdFactura"] == "F-3"
        assert ri["IdProducto"] == 3   # LAMINA ROSUQILLA... -> LAMINA ROSQUILLA... (fixed), matched in dim
        assert float(ri["Cantidad"]) == 4.0 and float(ri["Subtotal"]) == 20.0 and float(ri["Total"]) == 23.8

        # -- Cleanup spark.table
        spark.table = original_spark_table
