import json
from unittest.mock import Mock, call

import pytest
from libraries.bronze_layer.ingestors import base as base_module
from libraries.bronze_layer.ingestors.batch_ingestor import BatchIngestor


@pytest.fixture
def config_dict():
    return {
        "lakehouse_storage_account_name": "lkhsacct",
        "lakehouse_container_name": "lakehouse",
        "landing_storage_account_name": "ldgacct",
        "landing_container_name": "landing",
        "datasource": "erp",
        "dataset": "invoices",
        "source": {
            "format": "cloudFiles",
            "options": {
                "cloudFiles.format": "json",
                "cloudFiles.inferColumnTypes": "true",
            },
        },
        "sink": {
            "format": "delta",
            "options": {
                "mergeSchema": "true",
                "maxFilesPerTrigger": "1",
            },
        },
    }


@pytest.fixture
def config_file(tmp_path, config_dict):
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)


@pytest.fixture
def spark_chain_mocks(monkeypatch):
    """
    Monta una cadena de mocks para readStream/writeStream y parchea BaseIngestor.__init__
    para inyectar spark y config.
    Devuelve: (spark_mock, read_stream_mock, df_mock, write_stream_mock, query_mock)
    """
    spark_mock = Mock(name="SparkSessionMock")

    read_stream = Mock(name="readStream")
    spark_mock.readStream = read_stream
    df_mock = Mock(name="DataFrameMock")

    read_stream.format.return_value = read_stream
    read_stream.options.return_value = read_stream
    read_stream.option.return_value = read_stream
    read_stream.load.return_value = df_mock

    df_mock.withColumn.return_value = df_mock

    write_stream = Mock(name="writeStream")
    df_mock.writeStream = write_stream
    write_stream.format.return_value = write_stream
    write_stream.options.return_value = write_stream
    write_stream.option.return_value = write_stream
    write_stream.trigger.return_value = write_stream
    write_stream.queryName.return_value = write_stream

    query_mock = Mock(name="StreamingQueryMock")
    write_stream.start.return_value = query_mock

    # Parches BaseIngestor.__init__ to not touch real IO
    def fake_base_init(self, config_path: str):
        with open(config_path, "r") as f:
            self.config = json.load(f)
        self.spark = spark_mock

    monkeypatch.setattr(base_module.BaseIngestor, "__init__", fake_base_init)

    return spark_mock, read_stream, df_mock, write_stream, query_mock


@pytest.fixture
def ingestor(config_file, spark_chain_mocks, monkeypatch):
    obj = BatchIngestor(config_file)
    monkeypatch.setattr(obj, "create_table", Mock(return_value=None))
    return obj


# ---------- Test suites ----------


class TestBatchIngestorInitialization:
    def test_initializes_with_valid_config(self, config_dict, config_file, spark_chain_mocks):
        # Given
        spark_mock, *_ = spark_chain_mocks

        # When
        ingestor = BatchIngestor(config_file)

        # Then
        assert ingestor.config["datasource"] == config_dict["datasource"]
        assert ingestor.config["dataset"] == config_dict["dataset"]
        assert ingestor.spark is spark_mock


class TestBatchIngestorIngestReading:
    def test_builds_paths_and_reads_with_expected_options(
        self, config_dict, ingestor, spark_chain_mocks
    ):
        # Given
        _, read_stream, df_mock, _, _ = spark_chain_mocks

        lkh_acc = config_dict["lakehouse_storage_account_name"]
        lkh_cont = config_dict["lakehouse_container_name"]
        ldg_acc = config_dict["landing_storage_account_name"]
        ldg_cont = config_dict["landing_container_name"]
        datasource = config_dict["datasource"]
        dataset = config_dict["dataset"]

        lakehouse_container = f"abfss://{lkh_cont}@{lkh_acc}.dfs.core.windows.net"
        landing_container = f"abfss://{ldg_cont}@{ldg_acc}.dfs.core.windows.net"
        bronze_path = f"{lakehouse_container}/bronze"
        landing_path = f"{landing_container}/{datasource}/{dataset}"
        schema_loc = f"{bronze_path}/{datasource}/{dataset}_schema"

        # When
        ingestor.ingest()

        # Then
        read_stream.format.assert_called_once_with(config_dict["source"]["format"])
        read_stream.options.assert_any_call(**config_dict["source"]["options"])
        assert call("cloudFiles.schemaLocation", schema_loc) in read_stream.option.call_args_list
        read_stream.load.assert_called_once_with(landing_path)

        # Columnas agregadas
        expected_cols = {
            "_ingested_file_name",
            "_ingested_file_size",
            "_ingested_file_modification_time",
            "_ingestion_time",
        }
        added_cols = {c.args[0] for c in df_mock.withColumn.call_args_list}
        assert expected_cols.issubset(added_cols)


class TestBatchIngestorIngestWriting:
    def test_writes_with_sink_checkpoint_trigger_and_path(
        self, config_dict, ingestor, spark_chain_mocks
    ):
        # Given
        _, _, _, write_stream, _ = spark_chain_mocks

        lkh_acc = config_dict["lakehouse_storage_account_name"]
        lkh_cont = config_dict["lakehouse_container_name"]
        datasource = config_dict["datasource"]
        dataset = config_dict["dataset"]
        lakehouse_container = f"abfss://{lkh_cont}@{lkh_acc}.dfs.core.windows.net"
        bronze_path = f"{lakehouse_container}/bronze"
        final_output_path = f"{bronze_path}/{datasource}/{dataset}"
        checkpoint_loc = f"{bronze_path}/{datasource}/{dataset}_checkpoint/"

        # When
        ingestor.ingest()

        # Then
        write_stream.format.assert_called_once_with(config_dict["sink"]["format"])
        write_stream.options.assert_any_call(**config_dict["sink"]["options"])
        assert call("checkpointLocation", checkpoint_loc) in write_stream.option.call_args_list
        write_stream.trigger.assert_called_once_with(availableNow=True)
        write_stream.queryName.assert_called_once_with(f"{datasource} {dataset}")
        write_stream.start.assert_called_once_with(final_output_path)


class TestBatchIngestorIngestFlow:
    def test_awaits_termination_and_creates_table(self, config_dict, ingestor, spark_chain_mocks):
        # Given
        _, _, _, _, query_mock = spark_chain_mocks

        lkh_acc = config_dict["lakehouse_storage_account_name"]
        lkh_cont = config_dict["lakehouse_container_name"]
        datasource = config_dict["datasource"]
        dataset = config_dict["dataset"]
        lakehouse_container = f"abfss://{lkh_cont}@{lkh_acc}.dfs.core.windows.net"
        final_output_path = f"{lakehouse_container}/bronze/{datasource}/{dataset}"

        # When
        ingestor.ingest()

        # Then
        query_mock.awaitTermination.assert_called_once()
        ingestor.create_table.assert_called_once_with(dataset, final_output_path)


class TestBatchIngestorErrors:
    def test_propagates_errors_from_start(self, ingestor, spark_chain_mocks):
        # Given
        _, _, _, write_stream, _ = spark_chain_mocks
        write_stream.start.side_effect = RuntimeError("boom")

        # When & Then
        with pytest.raises(RuntimeError, match="boom"):
            ingestor.ingest()
