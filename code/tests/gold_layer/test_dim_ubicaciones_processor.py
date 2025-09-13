# test_dim_ubicaciones_processor.py

import json
from datetime import datetime
from types import MethodType
from unittest.mock import Mock

import pytest
from databricks.connect import DatabricksSession as SparkSession
import libraries.gold_layer.processors.base as base_module
from libraries.gold_layer.processors.dim_ubicaciones import GoldDimUbicacionesProcessor
from pyspark.sql import Row


# ---------------- Fixtures ----------------

@pytest.fixture(scope="session")
def spark():
    """
    Provides a SparkSession (Databricks Connect) with a low shuffle partition count
    to speed up tests.
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    yield spark
    spark.stop()


@pytest.fixture
def config_dict():
    """
    Minimal config structure expected by the Gold base class and this processor.
    """
    return {
        "catalog": "testcat",
        "sources": {"main_schema": "silver", "main_table": "facturas"},
        "sink": {"schema": "gold", "table": "dim_ubicaciones"},
        "lakehouse_storage_account_name": "dummyacct",
        "lakehouse_container_name": "lakehouse",
        "table_properties": {},
    }


@pytest.fixture
def config_file(tmp_path, config_dict):
    """
    Writes the config to a temporary local JSON file.
    The BaseProcessor.__init__ is monkeypatched, so we do NOT need dbfs:/ here.
    """
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)


@pytest.fixture
def processor(config_file, spark, monkeypatch):
    """
    Instantiates GoldDimUbicacionesProcessor with a monkeypatched BaseProcessor.__init__
    to avoid DBUtils/secrets and provide a Spark handle plus config fields.
    """
    def fake_base_init(self, config_path: str):
        import json as _json
        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark
        self.catalog = self.config["catalog"]
        self.src_schema = self.config["sources"]["main_schema"]
        self.src_table = self.config["sources"]["main_table"]
        self.tgt_schema = self.config["sink"]["schema"]
        self.tgt_table = self.config["sink"]["table"]
        self.account = self.config["lakehouse_storage_account_name"]
        self.container = self.config.get("lakehouse_container_name", "lakehouse")
        self.table_properties = self.config.get("table_properties", {})

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return GoldDimUbicacionesProcessor(config_file)


# ---------------- Helpers: synthetic sources/targets ----------------

def _src_initial(spark):
    """
    Initial snapshot for silver.facturas.
    New processor logic filters: Anulado=false AND Identificacion NOT NULL.
    Hence, only Cliente A passes Phase A.
    """
    return spark.createDataFrame([
        Row(Identificacion="CC-111", RazonSocialCliente="Cliente A S.A.S",
            FechaCreacion=datetime(2024,1,1,8,0,0), Anulado=False, Latitud=4.60, Longitud=-74.08),
        Row(Identificacion=None,     RazonSocialCliente="Cliente B LTDA",
            FechaCreacion=datetime(2024,1,2,9,0,0), Anulado=False, Latitud=6.25, Longitud=-75.57),  # dropped now
        Row(Identificacion="ZZ-999", RazonSocialCliente="X ANULADO",
            FechaCreacion=datetime(2024,1,3,9,0,0), Anulado=True,  Latitud=1.0,  Longitud=1.0),
    ])

def _src_incremental(spark):
    """
    Incremental snapshot: re-sees A, keeps B with null ID (still dropped), adds D with valid ID.
    This should produce exactly one new group (NumeroCliente = max + 1).
    """
    return spark.createDataFrame([
        Row(Identificacion="CC-111", RazonSocialCliente="Cliente A S.A.S",
            FechaCreacion=datetime(2024,1,1,8,0,0), Anulado=False, Latitud=4.60, Longitud=-74.08),
        Row(Identificacion=None,     RazonSocialCliente="Cliente B LTDA",
            FechaCreacion=datetime(2024,1,2,9,0,0), Anulado=False, Latitud=6.25, Longitud=-75.57),
        Row(Identificacion="CC-222", RazonSocialCliente="Cliente D",
            FechaCreacion=datetime(2024,1,3,10,0,0), Anulado=False, Latitud=7.11, Longitud=-73.00),
    ])


# ---------------- Test Suites ----------------

class TestDimUbicacionesPhaseA:
    """
    Phase A tests (no geospatial enrichment). We validate that:
      - Initial build filters annulled and null-ID rows, keeps representative coordinates,
        sets admin columns to NULL and publishes exactly the expected columns.
      - Incremental run inserts only truly new groups (insert-only MERGE semantics) and
        continues NumeroCliente from the current maximum.
    """

    def test_initial_build_creates_one_row_with_null_admin_columns(self, spark, processor, monkeypatch):
        # Capture holder for the DF passed to BaseProcessor.create_external_table
        created = {"df": None}

        # No-op schema/properties and disable Phase B enrichment
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(processor, "_phase_b_update", lambda sdf: None)
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", Mock(return_value=None))

        # Table existence: False -> initial branch
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=False))

        # Feed synthetic source through spark.table
        def _fake_table(self, fullname: str):
            if fullname == processor.source_fullname:
                return _src_initial(spark)
            if fullname == processor.target_fullname:
                # Processor may read target for Phase B scope; return empty shape if read.
                return spark.createDataFrame(
                    [], "NumeroCliente int, Latitud double, Longitud double, "
                        "Departamento string, Municipio string, Cod_DANE string, Distrito string, Barrio string"
                )
            return spark.table(fullname)
        monkeypatch.setattr(type(spark), "table", MethodType(_fake_table, spark))

        # Capture BaseProcessor.create_external_table arguments and short-circuit real writes
        def _capture_super_create(self, df_initial, ddl_columns_sql, table_comment, select_cols):
            created["df"] = df_initial.select(*select_cols)
        monkeypatch.setattr(base_module.BaseProcessor, "create_external_table", _capture_super_create)

        # Act
        processor.process()

        # Assert: exactly one row (Cliente A), admin columns are NULL, coordinates preserved
        out = created["df"]
        rows = out.orderBy("NumeroCliente").collect()
        assert len(rows) == 1
        r = rows[0]
        assert r["NumeroCliente"] == 1
        assert abs(r["Latitud"] - 4.60) < 1e-9 and abs(r["Longitud"] + 74.08) < 1e-9
        for c in ["Departamento", "Municipio", "Cod_DANE", "Distrito", "Barrio"]:
            assert r[c] is None, f"{c} must be NULL in Phase A initial"

    def test_incremental_inserts_only_new_group_and_continues_sequence(self, spark, processor, monkeypatch):
        # Capture holder for the DF passed to processor.merge_insert_new
        captured = {"to_insert": None}

        # No-op schema/properties and disable Phase B
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(processor, "_phase_b_update", lambda sdf: None)
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", Mock(return_value=None))

        # Table existence: True -> incremental branch
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=True))

        # Fake existing target keys: only NumeroCliente=1 (Cliente A already seeded)
        tgt_existing = spark.createDataFrame([(1,)], ["NumeroCliente"])

        # Feed synthetic source/target through spark.table
        def _fake_table(self, fullname: str):
            if fullname == processor.source_fullname:
                return _src_incremental(spark)
            if fullname == processor.target_fullname:
                return tgt_existing
            return spark.table(fullname)
        monkeypatch.setattr(type(spark), "table", MethodType(_fake_table, spark))

        # Capture merge_insert_new input and short-circuit Delta merge
        def _capture_merge(self, df_new):
            captured["to_insert"] = df_new
        monkeypatch.setattr(processor, "merge_insert_new", MethodType(_capture_merge, processor))

        # Act
        processor.process()

        # Assert: exactly one new group (Cliente D) with NumeroCliente = max + 1 = 2; admin columns NULL
        df_ins = captured["to_insert"]
        rows = df_ins.orderBy("NumeroCliente").collect()
        assert len(rows) == 1
        r = rows[0]
        assert r["NumeroCliente"] == 2
        assert abs(r["Latitud"] - 7.11) < 1e-9 and abs(r["Longitud"] + 73.00) < 1e-9
        for c in ["Departamento", "Municipio", "Cod_DANE", "Distrito", "Barrio"]:
            assert r[c] is None, f"{c} must be NULL in Phase A incremental"
