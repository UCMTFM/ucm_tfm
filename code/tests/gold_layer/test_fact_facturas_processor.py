# test_fact_facturas_processor.py
import json
from datetime import datetime
from unittest.mock import Mock
import types

import pytest
from databricks.connect import DatabricksSession as SparkSession
import libraries.gold_layer.processors.base as base_module
from libraries.gold_layer.processors.fact_facturas import GoldFactFacturasProcessor
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, TimestampType, DoubleType, BooleanType, StructType, StructField
)

# ---------------- Fixtures ----------------

@pytest.fixture(scope="session")
def spark():
    """
    Provides a SparkSession for the whole test session.
    Keeps shuffle partitions small for faster unit tests.
    """
    s = SparkSession.builder.getOrCreate()
    s.conf.set("spark.sql.shuffle.partitions", "2")
    yield s
    s.stop()

@pytest.fixture
def config_dict():
    """
    Minimal configuration mirroring the processor's expectations.
    """
    return {
        "catalog": "testcat",
        "sources": {"main_schema": "silver", "main_table": "facturas"},
        "sink": {"schema": "gold", "table": "fact_facturas"},
        "lakehouse_storage_account_name": "dummyacct",
        "lakehouse_container_name": "lakehouse",
        "table_properties": {},
    }

@pytest.fixture
def config_file(tmp_path, config_dict):
    """
    Writes the config JSON to a temporary file and returns its path.
    """
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)

@pytest.fixture
def processor(config_file, spark, monkeypatch):
    """
    Returns a GoldFactFacturasProcessor with a monkeypatched BaseProcessor.__init__
    to avoid DBUtils/secrets and any external I/O in unit tests.
    """
    def fake_base_init(self, config_path: str):
        import json as _json
        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark
        self.catalog   = self.config.get("catalog")
        self.src_schema = self.config["sources"].get("main_schema", "")
        self.src_table  = self.config["sources"].get("main_table", "")
        self.tgt_schema = self.config["sink"].get("schema", "")
        self.tgt_table  = self.config["sink"].get("table", "")
        self.account  = self.config.get("lakehouse_storage_account_name")
        self.container = self.config.get("lakehouse_container_name", "lakehouse")
        self.table_properties = self.config.get("table_properties", {})

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return GoldFactFacturasProcessor(config_file)

# ---------------- Synthetic sources ----------------

def _silver_fact_schema():
    return StructType([
        StructField("IdFactura",            StringType(),   True),
        StructField("Fecha",                TimestampType(),True),
        StructField("FechaCreacion",        TimestampType(),True),
        StructField("Identificacion",       StringType(),   True),
        StructField("RazonSocialCliente",   StringType(),   True),
        StructField("IdRuta",               StringType(),   True),
        StructField("IdFormaPago",          StringType(),   True),
        StructField("Subtotal",             DoubleType(),   True),
        StructField("Descuento",            DoubleType(),   True),
        StructField("Iva",                  DoubleType(),   True),
        StructField("ImpuestoSaludable",    DoubleType(),   True),
        StructField("PorcentajeRetencion",  DoubleType(),   True),
        StructField("Total",                DoubleType(),   True),
        StructField("Saldo",                DoubleType(),   True),
        StructField("Cantidad",             DoubleType(),   True),
        StructField("Anulado",              BooleanType(),  True),
    ])

def build_src_initial(spark):
    return spark.createDataFrame([
        ("F-1",   datetime(2024,1,10,10), datetime(2024,1,10,9,55), "CC-111", "Cliente A SAS",  "R10", "000", 50.0, 5.0,  9.0,  0.0, 10.0, 54.0, 0.0, 5.0,  False),
        ("F-DUP", datetime(2024,1,6,12),  datetime(2024,1,5,8,0),   "CC-111", "Cliente A SAS",  "R11", "008", 100.0,0.0, 19.0, 0.0, 0.0, 119.0,10.0,10.0, False),
        ("F-DUP", datetime(2024,1,6,12),  datetime(2024,1,6,9,0),   "CC-111", "Cliente A SAS",  "R11", "008", 110.0,0.0, 20.9, 0.0, 0.0, 130.9, 5.0,11.0, False),
        ("F-2",   datetime(2024,1,11,9),  None,                     None,     "Cliente B LTDA", "R20", "008", 20.0, 0.0,  3.8,  0.2, 5.0,  24.0, 0.0, None, False),
        ("F-X",   datetime(2024,1,12,9),  None,                     "CC-999", "Cliente X",      "R30", "000", 10.0, 0.0,  1.9,  0.0, 0.0,  11.9, 0.0, 1.0,  True),
    ], schema=_silver_fact_schema())

def build_src_incremental(spark):
    return spark.createDataFrame([
        ("F-3",   datetime(2024,1,13,9),  datetime(2024,1,13,8,30), None,     "Cliente B LTDA", "R21", "000", 40.0, 2.0, 7.22, 0.0, 2.5, 45.22, 0.0, 4.0,  False),
    ], schema=_silver_fact_schema())

def build_dim_clientes(spark):
    # NumeroCliente resolves by cleaned ID first, otherwise by normalized name
    return spark.createDataFrame([
        (1, "CC111",  "CLIENTE A"),
        (2, None,     "CLIENTE B"),
    ], ["NumeroCliente","Identificacion","NombreCliente"])

# ---------------- Test suites ----------------

class TestFactFacturasInitial:
    """
    Initial build path (table does not exist): ensures filtering, dedup, type casting,
    NumeroCliente resolution, and published columns.
    """

    def test_initial_build_publishes_expected_rows_and_columns(self, spark, processor, monkeypatch):
        created_holder = {"df": None}

        # Force initial branch and avoid side-effects
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=False))
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", Mock(return_value=None))

        # Patch spark.table to return our synthetic sources
        src_initial = build_src_initial(spark)
        src_incremental = build_src_incremental(spark)  # not used in this test
        dim_clientes = build_dim_clientes(spark)

        original_table = processor.spark.table
        def fake_table(name: str):
            if name == processor.source_fullname:
                return src_initial
            if name == f"{processor.catalog}.gold.dim_clientes":
                return dim_clientes
            return original_table(name)
        monkeypatch.setattr(processor.spark, "table", fake_table)

        # Capture the DataFrame passed to table creation
        def _create_external_table_stub(df_initial, ddl_columns_sql=None, table_comment=None, select_cols=None):
            created_holder["df"] = df_initial.select(*select_cols)

        monkeypatch.setattr(base_module.BaseProcessor, "create_external_table", _create_external_table_stub)

        # Act
        processor.process()

        # Assert
        created = created_holder["df"]
        assert created is not None
        expected_cols = {
            "IdFactura","Fecha","NumeroCliente","IdRuta","IdFormaPago",
            "Subtotal","Descuento","Iva","ImpuestoSaludable","PorcentajeRetencion",
            "Total","Saldo","Cantidad"
        }
        assert set(created.columns) == expected_cols  # matches DDL in the processor

        rows = {r["IdFactura"]: r for r in created.collect()}
        assert set(rows.keys()) == {"F-1", "F-DUP", "F-2"}  # filters annulled and keeps latest per IdFactura

        # F-1 → NumeroCliente by ID (CC-111 → CC111) => 1
        r = rows["F-1"]
        assert r["NumeroCliente"] == 1
        assert r["Fecha"].isoformat() == "2024-01-10"

        # F-DUP → keeps the newer version by _ts_order (Total=130.9, Cantidad=11.0)
        r = rows["F-DUP"]
        assert r["NumeroCliente"] == 1
        assert float(r["Total"]) == 130.9 and float(r["Cantidad"]) == 11.0

        # F-2 → fallback by normalized name 'CLIENTE B' => 2; Cantidad can be NULL
        r = rows["F-2"]
        assert r["NumeroCliente"] == 2
        assert r["Fecha"].isoformat() == "2024-01-11"
        assert r["Cantidad"] is None

class TestFactFacturasIncremental:
    """
    Incremental path (table exists): ensures upsert by IdFactura uses a correctly
    resolved and typed DataFrame with only the new rows.
    """

    def test_incremental_upsert_builds_expected_df(self, spark, processor, monkeypatch):
        merged_holder = {"df": None}

        # Force existing table branch and avoid side-effects
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=True))
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", Mock(return_value=None))

        # Patch spark.table for the new extraction and dim table
        src_incremental = build_src_incremental(spark)
        dim_clientes = build_dim_clientes(spark)

        original_table = processor.spark.table
        def fake_table(name: str):
            if name == processor.source_fullname:
                return src_incremental
            if name == f"{processor.catalog}.gold.dim_clientes":
                return dim_clientes
            return original_table(name)
        monkeypatch.setattr(processor.spark, "table", fake_table)

        # Capture what would be passed to the upsert (merge)
        def _merge_stub(df_upsert):
            merged_holder["df"] = df_upsert

        monkeypatch.setattr(processor, "merge_upsert", _merge_stub)

        # Act
        processor.process()

        # Assert — only F-3 goes to the merge, resolved by name → NumeroCliente=2
        out = merged_holder["df"]
        assert out is not None
        rows = {r["IdFactura"]: r for r in out.collect()}
        assert set(rows.keys()) == {"F-3"}
        r = rows["F-3"]
        assert r["NumeroCliente"] == 2
        assert r["Fecha"].isoformat() == "2024-01-13"
        assert r["IdRuta"] == "R21" and r["IdFormaPago"] == "000"
        assert float(r["Subtotal"]) == 40.0 and float(r["Descuento"]) == 2.0 and float(r["Iva"]) == 7.22
