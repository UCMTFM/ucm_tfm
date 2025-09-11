import json
from unittest.mock import Mock

import libraries.gold_layer.processors.base as base_module
import pytest
from databricks.connect import DatabricksSession as SparkSession
from libraries.gold_layer.processors.sales_by_route import SalesByRouteProcessor
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    yield spark
    spark.stop()


@pytest.fixture
def config_dict():
    return {
        "catalog": "mycat",
        "data_since": 2023,
        "sources": {
            "detalle_facturas_schema": "silver",
            "detalle_facturas_table": "detalle_facturas",
            "facturas_schema": "silver",
            "facturas_table": "facturas",
        },
    }


@pytest.fixture
def config_file(tmp_path, config_dict):
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_dict))
    return str(p)


@pytest.fixture
def processor(config_file, spark, monkeypatch):
    def fake_base_init(self, config_path: str):
        import json as _json

        self.spark = spark
        with open(config_path, "r") as f:
            self.config = _json.load(f)

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)

    return SalesByRouteProcessor(config_file)


# ------------------- Test Suites -------------------


class TestSalesByRouteGetModelVars:
    def test_builds_daily_aggregates_and_lag(self, spark, processor):
        # Given:
        fact_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("IdRuta", IntegerType(), True),
            ]
        )
        df_fact = spark.createDataFrame(
            [
                (1, 10),
                (2, 10),
            ],
            fact_schema,
        )

        det_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("Dia", IntegerType(), True),
                StructField("Mes", IntegerType(), True),
                StructField("Anio", IntegerType(), True),
            ]
        )
        df_det = spark.createDataFrame(
            [
                (1, 100, "Agua", 5.0, 1, 1, 2024),
                (2, 100, "Agua", 7.0, 2, 1, 2024),
            ],
            det_schema,
        )

        # When
        out = SalesByRouteProcessor.get_model_vars(df_fact, df_det)

        # Then: ordered by (IdRuta, IdProducto, Anio, Mes, Dia)
        rows = out.orderBy("IdRuta", "IdProducto", "Anio", "Mes", "Dia").collect()
        # Row 1 -> CantidadTotal = 5.0, lag NULL
        # Row 2 -> CantidadTotal = 7.0, lag 5.0
        assert len(rows) == 2
        assert rows[0]["IdRuta"] == 10
        assert rows[0]["IdProducto"] == 100
        assert rows[0]["NombreProducto"] == "Agua"
        assert rows[0]["Anio"] == 2024 and rows[0]["Mes"] == 1 and rows[0]["Dia"] == 1
        assert rows[0]["CantidadTotal"] == 5.0
        assert rows[0]["CantidadTotalPrevia"] is None

        assert rows[1]["Anio"] == 2024 and rows[1]["Mes"] == 1 and rows[1]["Dia"] == 2
        assert rows[1]["CantidadTotal"] == 7.0
        assert rows[1]["CantidadTotalPrevia"] == 5.0


class TestSalesByRouteAddOHEVars:
    def test_encodes_and_filters_rows(self, spark, processor, config_dict, monkeypatch):
        import libraries.gold_layer.processors.sales_by_route as sr_mod
        from pyspark.sql import Row  # si no lo tienes importado arriba
        from pyspark.sql import functions as F

        # Fakes ligeros que simulan el pipeline de ML sin JVM
        class FakeStringIndexer:
            def __init__(self, inputCol, outputCol, handleInvalid=None):
                self.inputCol = inputCol
                self.outputCol = outputCol
                self.handleInvalid = handleInvalid

        class FakeOneHotEncoder:
            def __init__(self, inputCol, outputCol):
                self.inputCol = inputCol
                self.outputCol = outputCol

        class FakePipeline:
            def __init__(self, stages):
                self.stages = stages

            def fit(self, df):
                idx_input_cols = [
                    s.inputCol for s in self.stages if isinstance(s, FakeStringIndexer)
                ]

                class _Model:
                    def transform(self, df2):
                        out = df2
                        for c in idx_input_cols:
                            out = out.withColumn(f"{c}_idx", F.lit(0))
                            out = out.withColumn(f"{c}_ohe", F.array(F.lit(1.0)))
                        return out

                return _Model()

        # Parcheo en el módulo del procesador
        monkeypatch.setattr(sr_mod, "StringIndexer", FakeStringIndexer, raising=True)
        monkeypatch.setattr(sr_mod, "OneHotEncoder", FakeOneHotEncoder, raising=True)
        monkeypatch.setattr(sr_mod, "Pipeline", FakePipeline, raising=True)

        # Given
        rows = [
            Row(
                IdRuta=10,
                IdProducto=100,
                NombreProducto="Agua",
                Anio=2022,
                Mes=12,
                Dia=31,
                CantidadTotalPrevia=None,
                CantidadTotal=5.0,
            ),
            Row(
                IdRuta=10,
                IdProducto=100,
                NombreProducto="Agua",
                Anio=2024,
                Mes=1,
                Dia=1,
                CantidadTotalPrevia=5.0,
                CantidadTotal=7.0,
            ),
        ]
        df_agg_model = spark.createDataFrame(rows)
        categorical_cols = ["IdRuta", "IdProducto", "NombreProducto", "Anio", "Mes", "Dia"]

        # When
        out = processor.add_ohe_vars(df_agg_model, categorical_cols)

        # Then
        collected = out.select(
            "IdRuta", "IdProducto", "NombreProducto", "Anio", "Mes", "Dia"
        ).collect()
        assert len(collected) == 1
        assert collected[0]["Anio"] == 2024

        expected_idx = [c + "_idx" for c in categorical_cols]
        expected_ohe = [c + "_ohe" for c in categorical_cols]
        for c in expected_idx + expected_ohe:
            assert c in out.columns


class TestSalesByRouteProcess:
    def test_process_reads_builds_encodes_and_writes(
        self, spark, processor, config_dict, monkeypatch
    ):
        # Given:
        det_tbl = config_dict["sources"]["detalle_facturas_table"]
        fact_tbl = config_dict["sources"]["facturas_table"]

        # DF dummy that will return read_silver_table
        df_det = spark.createDataFrame(
            [
                Row(
                    IdFactura=1,
                    IdProducto=100,
                    NombreProducto="Agua",
                    Cantidad=5.0,
                    Dia=1,
                    Mes=1,
                    Anio=2024,
                ),
                Row(
                    IdFactura=2,
                    IdProducto=100,
                    NombreProducto="Agua",
                    Cantidad=7.0,
                    Dia=2,
                    Mes=1,
                    Anio=2024,
                ),
            ]
        )
        df_fact = spark.createDataFrame(
            [
                Row(IdFactura=1, IdRuta=10),
                Row(IdFactura=2, IdRuta=10),
            ]
        )

        def fake_read(table_name: str):
            if table_name == det_tbl:
                return df_det
            elif table_name == fact_tbl:
                return df_fact
            raise ValueError("tabla inesperada")

        monkeypatch.setattr(processor, "read_silver_table", Mock(side_effect=fake_read))

        df_agg_model = SalesByRouteProcessor.get_model_vars(df_fact, df_det)
        monkeypatch.setattr(
            SalesByRouteProcessor, "get_model_vars", lambda _df_fact, _df_det: df_agg_model
        )

        df_encoded = df_agg_model.withColumn("foo", F.lit(1))
        monkeypatch.setattr(processor, "add_ohe_vars", lambda _df, cols: df_encoded)

        monkeypatch.setattr(processor, "write_delta_table", Mock(return_value=None))

        # When
        processor.process()

        # Then: both tables read and final DF written
        assert processor.read_silver_table.call_count == 2
        processor.write_delta_table.assert_called_once()
        passed_df = processor.write_delta_table.call_args.args[0]

        assert set(df_encoded.columns) == set(passed_df.columns)
