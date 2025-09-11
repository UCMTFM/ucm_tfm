# code/tests/bronze_layer/test_streaming_ingestor.py
import json
from unittest.mock import Mock, call

import libraries.bronze_layer.ingestors.base as base_module
import pytest

# 🔧 Ajusta estos imports al path real de tu proyecto
from libraries.bronze_layer.ingestors.streaming_ingestor import StreamingIngestor


@pytest.fixture
def config_dict():
    return {
        "lakehouse_storage_account_name": "lkhsacct",
        "lakehouse_container_name": "lakehouse",
        "datasource": "erp",
        "dataset": "events",
        "kafka_config_path": "dbfs:/configs/kafka.properties",
        "source": {
            "format": "kafka",
            "json_schema": "field1 double, field2 double, field3 string",
            "options": {
                "subscribe": "topic.erp.events",
                "startingOffsets": "latest",
            },
        },
        "sink": {
            "format": "delta",
            "options": {
                "mergeSchema": "true",
            },
        },
    }


@pytest.fixture
def config_file(tmp_path, config_dict):
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)


@pytest.fixture
def spark_mocks(monkeypatch):
    """
    Crea mocks encadenados para spark.readStream, df.toDF, withColumn y writeStream.
    Devuelve: (spark_mock, read_stream, df_in, df_renamed, df_formatted, write_stream, query_mock)
    """
    spark_mock = Mock(name="SparkSessionMock")

    read_stream = Mock(name="readStream")
    spark_mock.readStream = read_stream

    df_in = Mock(name="KafkaRawDF")
    # Common kafka cols: key, value, topic
    df_in.columns = ["key", "value", "topic"]

    read_stream.format.return_value = read_stream
    read_stream.options.return_value = read_stream
    read_stream.load.return_value = df_in

    df_renamed = Mock(name="RenamedDF")
    df_in.toDF.return_value = df_renamed

    df_renamed.withColumn.return_value = df_renamed

    df_formatted = Mock(name="FormattedDF")
    df_renamed.select.return_value = df_renamed
    df_renamed.drop.return_value = df_formatted

    write_stream = Mock(name="writeStream")
    df_formatted.writeStream = write_stream
    write_stream.format.return_value = write_stream
    write_stream.options.return_value = write_stream
    write_stream.option.return_value = write_stream
    write_stream.trigger.return_value = write_stream
    write_stream.queryName.return_value = write_stream

    query_mock = Mock(name="StreamingQueryMock")
    write_stream.start.return_value = query_mock

    # Parch BaseIngestor.__init__ to inject spark, config and dbutils
    def fake_base_init(self, config_path: str):
        with open(config_path, "r") as f:
            self.config = json.load(f)
        self.spark = spark_mock

        dbutils = Mock(name="dbutils")
        dbutils.fs = Mock()
        self.dbutils = dbutils

    monkeypatch.setattr(base_module.BaseIngestor, "__init__", fake_base_init)

    return spark_mock, read_stream, df_in, df_renamed, df_formatted, write_stream, query_mock


@pytest.fixture
def ingestor(config_file, spark_mocks, monkeypatch):
    obj = StreamingIngestor(config_file)
    monkeypatch.setattr(obj, "create_table", Mock(return_value=None))
    return obj


# ---------- Test suites ----------


class TestStreamingIngestorInitialization:
    def test_initializes_with_valid_config(self, config_dict, config_file, spark_mocks):
        # Given
        spark_mock, *_ = spark_mocks

        # When
        ing = StreamingIngestor(config_file)

        # Then
        assert ing.config["datasource"] == config_dict["datasource"]
        assert ing.config["dataset"] == config_dict["dataset"]
        assert ing.spark is spark_mock
        assert hasattr(ing, "dbutils")


class TestStreamingIngestorKafkaConfig:
    def test_read_kafka_config_parses_properties_and_builds_jaas(self, ingestor, spark_mocks):
        # Given
        _, _, _, _, _, _, _ = spark_mocks
        props = """
        # comment
        bootstrap.servers = broker1:9093,broker2:9093
        security.protocol = SASL_SSL
        sasl.mechanisms = PLAIN
        sasl.username = my_user
        sasl.password = my_pass
        """
        ingestor.dbutils.fs.head.return_value = props

        # When
        cfg = ingestor.read_kafka_config()

        # Then
        assert cfg["kafka.bootstrap.servers"] == "broker1:9093,broker2:9093"
        assert cfg["kafka.security.protocol"] == "SASL_SSL"
        assert cfg["kafka.sasl.mechanism"] == "PLAIN"
        assert 'username="my_user"' in cfg["kafka.sasl.jaas.config"]
        assert 'password="my_pass"' in cfg["kafka.sasl.jaas.config"]

    def test_read_kafka_config_ignores_comments_and_blank_lines(self, ingestor):
        # Given
        props = "\n# hello\n\nsasl.username = u\nsasl.password = p\n"
        ingestor.dbutils.fs.head.return_value = props

        # When
        cfg = ingestor.read_kafka_config()

        # Then
        assert 'username="u"' in cfg["kafka.sasl.jaas.config"]
        assert 'password="p"' in cfg["kafka.sasl.jaas.config"]


class TestStreamingIngestorJsonIngest:
    def test_json_ingest_reads_from_kafka_and_formats_dataframe(
        self, config_dict, ingestor, spark_mocks, monkeypatch
    ):
        # Given
        spark_mock, read_stream, df_in, df_renamed, df_formatted, _, _ = spark_mocks

        # Forzar opciones de kafka desde read_kafka_config
        kafka_opts = {
            "kafka.bootstrap.servers": "b1:9093",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": 'kafkashaded... username="u" password="p";',
        }
        monkeypatch.setattr(StreamingIngestor, "read_kafka_config", lambda self: kafka_opts)

        expected_options = dict(kafka_opts)
        expected_options.update(config_dict["source"]["options"])

        # When
        df_out = ingestor.json_ingest()

        # Then: Kafka read
        read_stream.format.assert_called_once_with(config_dict["source"]["format"])
        read_stream.options.assert_called_once()

        passed_kwargs = read_stream.options.call_args.kwargs
        for k, v in expected_options.items():
            assert passed_kwargs.get(k) == v

        read_stream.load.assert_called_once_with()

        # Then: renamed with toDF
        df_in.toDF.assert_called_once_with("_key", "_value", "_topic")

        # Then: transformations
        # cols "key", "value" (JSON parse), "_ingestion_time" are added
        added_cols = [c.args[0] for c in df_renamed.withColumn.call_args_list]
        assert "key" in added_cols
        assert "value" in added_cols
        assert "_ingestion_time" in added_cols

        # select and drop were called
        assert df_renamed.select.called
        assert df_renamed.drop.called

        # returns the formatted final df
        assert df_out is df_formatted


class TestStreamingIngestorIngestFlow:
    def test_ingest_writes_with_sink_and_creates_table(
        self, config_dict, ingestor, spark_mocks, monkeypatch
    ):
        # Given
        _, _, _, _, df_formatted, write_stream, query_mock = spark_mocks

        monkeypatch.setattr(StreamingIngestor, "json_ingest", lambda self: df_formatted)

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

        # Then: writing
        write_stream.format.assert_called_once_with(config_dict["sink"]["format"])
        write_stream.options.assert_any_call(**config_dict["sink"]["options"])
        assert call("checkpointLocation", checkpoint_loc) in write_stream.option.call_args_list
        write_stream.trigger.assert_called_once_with(availableNow=True)
        write_stream.queryName.assert_called_once_with(f"{datasource} {dataset}")
        write_stream.start.assert_called_once_with(final_output_path)

        # awaitTermination and create_table
        query_mock.awaitTermination.assert_called_once()
        ingestor.create_table.assert_called_once_with(dataset, final_output_path)

    def test_ingest_propagates_start_errors(self, ingestor, spark_mocks, monkeypatch):
        # Given
        _, _, _, _, df_formatted, write_stream, _ = spark_mocks
        monkeypatch.setattr(StreamingIngestor, "json_ingest", lambda self: df_formatted)
        write_stream.start.side_effect = RuntimeError("boom")

        # When & Then
        with pytest.raises(RuntimeError, match="boom"):
            ingestor.ingest()
