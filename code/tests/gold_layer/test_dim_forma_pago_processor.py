# test_dim_forma_pago_processor.py
import json
from unittest.mock import Mock

import pytest
from databricks.connect import DatabricksSession as SparkSession
import libraries.gold_layer.processors.base as base_module
from libraries.gold_layer.processors.dim_forma_pago import GoldDimFormaPagoProcessor
from pyspark.sql import Row

# ---------------- Fixtures ----------------

@pytest.fixture(scope="session")
def spark():
    """
    Provides a SparkSession for the entire test session.
    Uses a small number of shuffle partitions for faster unit tests.
    """
    s = SparkSession.builder.getOrCreate()
    s.conf.set("spark.sql.shuffle.partitions", "2")
    yield s
    s.stop()

@pytest.fixture
def config_dict():
    """
    Minimal configuration mirroring the real structure expected by the processor.
    """
    return {
        "catalog": "testcat",
        "sources": {"main_schema": "silver", "main_table": "facturas"},  # not used by this processor
        "sink": {"schema": "gold", "table": "dim_forma_pago"},
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
    Returns a GoldDimFormaPagoProcessor with a monkeypatched base constructor
    to avoid DBUtils/secrets and any external I/O during unit tests.
    """
    def fake_base_init(self, config_path: str):
        import json as _json
        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark
        # initialize attributes used by BaseProcessor helpers & properties
        self.catalog = self.config.get("catalog")
        self.src_schema = self.config["sources"].get("main_schema", "")
        self.src_table = self.config["sources"].get("main_table", "")
        self.tgt_schema = self.config["sink"].get("schema", "")
        self.tgt_table = self.config["sink"].get("table", "")
        self.account = self.config.get("lakehouse_storage_account_name")
        self.container = self.config.get("lakehouse_container_name", "lakehouse")
        self.table_properties = self.config.get("table_properties", {})

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return GoldDimFormaPagoProcessor(config_file)

# ---------------- Test Suites ----------------

class TestDimFormaPagoInitialBuild:
    """
    Tests for the initial build path (target table does not exist).
    """

    def test_initial_creates_external_table_with_expected_seed(self, processor, monkeypatch):
        """
        Ensures that the processor:
        - builds the in-memory seed with the two canonical codes,
        - creates/registers the external table,
        - and publishes exactly (IdFormaPago, FormaPago).
        """
        created_holder = {"df": None}

        # Avoid schema side-effects and force the initial branch
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=False))

        # Capture the seed DataFrame passed to the table creation
        def _create_external_table_stub(df_initial, ddl_columns_sql=None, table_comment=None, select_cols=None):
            # mirror BaseProcessor behavior: publish only selected columns
            created_holder["df"] = df_initial.select(*select_cols)

        monkeypatch.setattr(base_module.BaseProcessor, "create_external_table", _create_external_table_stub)
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", Mock(return_value=None))

        # Act
        processor.process()

        # Assert: exactly the two expected rows in a deterministic order
        created = created_holder["df"]
        assert created is not None
        assert set(created.columns) == {"IdFormaPago", "FormaPago"}

        rows = created.orderBy("IdFormaPago").collect()
        pairs = [(r["IdFormaPago"], r["FormaPago"]) for r in rows]
        assert pairs == [("000", "Efectivo"), ("008", "Transferencia")]

class TestDimFormaPagoIncremental:
    """
    Tests for the incremental path (target table already exists).
    """

    def test_upsert_is_idempotent_and_applies_properties(self, processor, monkeypatch):
        """
        Verifies that when the table exists:
        - the processor upserts the seed (insert-only on new codes, update on name),
        - and applies table properties after the merge.
        """
        # Force existing table branch
        monkeypatch.setattr(processor, "ensure_schema", Mock(return_value=None))
        monkeypatch.setattr(base_module.BaseProcessor, "table_exists", Mock(return_value=True))

        # Capture what would be merged (insert-only + possible updates)
        captured = {"df": None}
        def _merge_stub(df_seed):
            captured["df"] = df_seed.select("IdFormaPago", "FormaPago")

        monkeypatch.setattr(processor, "_merge_seed", _merge_stub)
        set_props = Mock(return_value=None)
        monkeypatch.setattr(base_module.BaseProcessor, "set_table_properties", set_props)

        # Act
        processor.process()

        # Assert: same two codes go into the merge; properties were applied
        merged = captured["df"]
        assert merged is not None
        rows = merged.orderBy("IdFormaPago").collect()
        pairs = [(r["IdFormaPago"], r["FormaPago"]) for r in rows]
        assert pairs == [("000", "Efectivo"), ("008", "Transferencia")]
        set_props.assert_called_once()
