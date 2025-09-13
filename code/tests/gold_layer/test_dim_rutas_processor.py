import json
from unittest.mock import Mock

import pytest
from databricks.connect import DatabricksSession as SparkSession
import libraries.gold_layer.processors.base as base_module
from libraries.gold_layer.processors.dim_rutas import GoldDimRutasProcessor
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

# ---------------- Fixtures ----------------

@pytest.fixture(scope="session")
def spark():
    """
    Provides a SparkSession for the entire test session.
    It configures the session with a small shuffle partition number for faster test execution.
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    yield spark
    spark.stop()

@pytest.fixture
def config_dict():
    """
    Provides a minimal configuration dictionary used by the processor,
    simulating the expected structure in real configs.
    """
    return {
        "catalog": "testcat",
        "sources": {"main_schema": "silver", "main_table": "facturas"},
        "sink": {"schema": "gold", "table": "dim_rutas"},
        "lakehouse_storage_account_name": "dummyacct",
        "lakehouse_container_name": "lakehouse",
        "table_properties": {},
    }

@pytest.fixture
def config_file(tmp_path, config_dict):
    """
    Writes the config dictionary to a temporary JSON file.
    Returns the file path to be used as input for the processor.
    """
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)

@pytest.fixture
def processor(config_file, spark, monkeypatch, config_dict):
    """
    Returns an instance of `GoldDimRutasProcessor` with a monkeypatched base constructor
    to avoid dependencies like DBUtils or secrets during unit tests.
    It initializes attributes manually from the provided config file.
    """
    def fake_base_init(self, config_path: str):
        import json as _json
        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark
        self.catalog = self.config.get("catalog")
        self.src_schema = self.config["sources"].get("main_schema", "")
        self.src_table = self.config["sources"].get("main_table", "")
        self.tgt_schema = self.config["sink"].get("schema", "")
        self.tgt_table = self.config["sink"].get("table", "")
        self.account = self.config.get("lakehouse_storage_account_name")
        self.container = self.config.get("lakehouse_container_name", "lakehouse")
        self.table_properties = self.config.get("table_properties", {})

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return GoldDimRutasProcessor(config_file)

# ---------------- Helpers for Synthetic DataFrames ----------------

def _df_idruta(spark, values):
    """
    Helper function to create a DataFrame containing only `idRuta` values.
    """
    return spark.createDataFrame([Row(idRuta=v) for v in values]).select("idRuta")

def _df_target_existing(spark):
    """
    Creates a mock DataFrame representing the existing target table with two predefined routes.
    """
    schema = StructType([
        StructField("idRuta", StringType(), True),
        StructField("nombreRuta", StringType(), True),
    ])
    data = [("A10", "Ruta 1"), ("B01", "Ruta 2")]
    return spark.createDataFrame(data, schema=schema)

# ---------------- Test Suites ----------------

class TestDimRutasInitialBuild:
    """
    Test scenarios for when the target table does not yet exist — i.e., the initial build.
    """

    def test_creates_external_table_with_numbering(self, spark, processor, monkeypatch):
        """
        Ensures that during the initial build, the processor:
        - Creates an external table.
        - Assigns incremented names ("Ruta N") to each unique route.
        - Publishes the correct columns to the external table.
        """
        created_holder = {"df": None, "select_cols": None}

        # Avoid actual schema creation logic
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))

        # Mock reading from the source to return three distinct route IDs
        monkeypatch.setattr(
            processor, "read_source_distinct_routes",
            Mock(side_effect=lambda: _df_idruta(spark, ["A10", "A20", "B01"]))
        )

        # Simulate that the table does not exist
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=False))

        # Stub for create_external_table to capture the published DataFrame
        def _create_external_table_stub(df_initial, ddl_columns_sql=None, table_comment=None, select_cols=None):
            created_holder["df"] = df_initial.select(*select_cols)
            created_holder["select_cols"] = list(select_cols)

        monkeypatch.setattr(base_module.BaseProcessor, "create_external_table", _create_external_table_stub)

        # Avoid setting table properties for this test
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", Mock(return_value=None))

        # Act
        processor.process()

        # Assert
        created = created_holder["df"]
        assert created is not None
        assert set(created.columns) == {"idRuta", "nombreRuta"}

        # Validate route names are correctly assigned
        rows = created.orderBy("idRuta").collect()
        expected = {"A10": "Ruta 1", "A20": "Ruta 2", "B01": "Ruta 3"}
        for r in rows:
            assert r["nombreRuta"] == expected[r["idRuta"]]

class TestDimRutasIncremental:
    """
    Test scenarios for when the target table already exists — i.e., incremental builds.
    """

    def test_inserts_only_new_routes_and_continues_sequence(self, spark, processor, monkeypatch):
        """
        Verifies that during an incremental run:
        - Only new routes (not already in target) are inserted.
        - The `nombreRuta` continues numbering from the existing maximum.
        """
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=True))

        # Source contains a new route "C99"
        monkeypatch.setattr(
            processor, "read_source_distinct_routes",
            Mock(side_effect=lambda: _df_idruta(spark, ["A10", "B01", "C99"]))
        )
        # Existing target contains A10 and B01 only
        monkeypatch.setattr(
            processor, "read_target",
            Mock(side_effect=lambda: _df_target_existing(spark))
        )

        captured = {"to_insert": None}
        def _merge_stub(df_to_insert):
            captured["to_insert"] = df_to_insert

        monkeypatch.setattr(processor, "merge_upsert_new_routes", _merge_stub)
        set_props = Mock(return_value=None)
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", set_props)

        # Act
        processor.process()

        # Assert
        to_insert = captured["to_insert"]
        assert to_insert is not None
        rows = to_insert.collect()
        assert len(rows) == 1 and rows[0]["idRuta"] == "C99"
        assert rows[0]["nombreRuta"] == "Ruta 3"  # max in existing = 2, next should be 3
        set_props.assert_called_once()  # Should set properties after upsert

    def test_noop_when_no_new_routes(self, spark, processor, monkeypatch):
        """
        Ensures that if there are no new routes in the source (i.e., all exist in the target),
        then:
        - No merge/upsert is performed.
        - No table properties are set.
        """
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=True))

        # Source contains only routes already present in the target
        monkeypatch.setattr(
            processor, "read_source_distinct_routes",
            Mock(side_effect=lambda: _df_idruta(spark, ["A10", "B01"]))
        )
        monkeypatch.setattr(
            processor, "read_target",
            Mock(side_effect=lambda: _df_target_existing(spark))
        )

        merge_mock = Mock()
        monkeypatch.setattr(processor, "merge_upsert_new_routes", merge_mock)
        set_props = Mock()
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", set_props)

        # Act
        processor.process()

        # Assert
        merge_mock.assert_not_called()
        set_props.assert_not_called()
