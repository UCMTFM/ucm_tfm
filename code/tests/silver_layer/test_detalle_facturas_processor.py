import json
from datetime import datetime
from unittest.mock import Mock

import libraries.silver_layer.processors.base as base_module
import pytest
from databricks.connect import DatabricksSession as SparkSession
from libraries.silver_layer.processors.detalle_facturas import DetalleFacturasProcessor
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


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
        "dataset": "detalle_facturas",
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

        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark

        self.read_bronze_table = Mock()
        self.imputations = Mock()
        self.write_delta_table = Mock()
        self.update_last_processed = Mock()

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return DetalleFacturasProcessor(config_file)


# ----------------------- Test Suites -------------------


class TestDetalleFacturasTransformations:
    def test_transforms_and_derives_financial_fields(self, spark, processor):
        # Given
        schema = StructType(
            [
                StructField("IdDetalle", IntegerType(), False),
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("ValorUnitario", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("PorcentajeIva", DoubleType(), True),
                StructField("Iva", DoubleType(), True),
                StructField("ISPPorcentaje", DoubleType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("_ingestion_time", TimestampType(), True),
            ]
        )

        df_in = spark.createDataFrame(
            [
                (
                    1,
                    101,
                    1001,
                    "Agua",
                    2.0,
                    10.0,
                    2.0,
                    19.0,
                    3.8,
                    5.0,
                    datetime(2024, 1, 15, 8, 30, 0),
                    datetime(2024, 1, 15, 8, 31, 0),
                ),
            ],
            schema=schema,
        )

        # When
        out = processor.transformations(df_in)

        # Then
        row = out.collect()[0]
        assert row["IdDetalle"] == 1
        assert row["IdFactura"] == 101
        assert row["IdProducto"] == 1001
        assert row["NombreProducto"] == "Agua"

        assert "ValorDescuento" in out.columns
        assert "ValorIva" in out.columns
        assert "PorcentajeImpuestoSaludable" in out.columns

        assert row["Subtotal"] == 20.0
        assert row["PorcentajeDescuento"] == pytest.approx(10.0)
        assert row["Anio"] == 2024 and row["Mes"] == 1 and row["Dia"] == 15
        assert row["ValorImpuestoSaludable"] == 1.0
        assert row["Total"] == pytest.approx(22.8, rel=1e-6)

    def test_handles_zero_subtotal_returns_null_discount_percent(self, spark, processor):
        # Given: Subtotal = 0 (Cantidad=0) -> div by 0 -> Spark returns null
        schema = StructType(
            [
                StructField("IdDetalle", IntegerType(), False),
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("ValorUnitario", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("PorcentajeIva", DoubleType(), True),
                StructField("Iva", DoubleType(), True),
                StructField("ISPPorcentaje", DoubleType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("_ingestion_time", TimestampType(), True),
            ]
        )

        df_in = spark.createDataFrame(
            [
                (
                    1,
                    200,
                    300,
                    "X",
                    0.0,
                    10.0,
                    5.0,
                    19.0,
                    0.0,
                    0.0,
                    datetime(2023, 5, 1, 12, 0, 0),
                    datetime(2023, 5, 1, 12, 0, 1),
                )
            ],
            schema=schema,
        )

        # When
        out = processor.transformations(df_in)
        row = out.collect()[0]

        # Then: Subtotal 0 -> PorcentajeDescuento will be null
        assert row["Subtotal"] == 0.0
        assert row["PorcentajeDescuento"] is None


class TestDetalleFacturasProcess:
    def test_process_reads_imputes_transforms_writes_and_updates(self, spark, processor):
        # Given:
        schema = StructType(
            [
                StructField("IdDetalle", IntegerType(), False),
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("ValorUnitario", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("PorcentajeIva", DoubleType(), True),
                StructField("Iva", DoubleType(), True),
                StructField("ISPPorcentaje", DoubleType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("_ingestion_time", TimestampType(), True),
            ]
        )

        df_bronze = spark.createDataFrame(
            [
                (
                    1,
                    10,
                    100,
                    "Agua",
                    2.0,
                    10.0,
                    2.0,
                    19.0,
                    3.8,
                    5.0,
                    datetime(2024, 1, 1, 0, 0, 0),
                    datetime(2024, 1, 1, 0, 0, 1),
                )
            ],
            schema=schema,
        )

        processor.read_bronze_table.return_value = df_bronze
        processor.imputations.side_effect = lambda df: df

        processor.write_delta_table.return_value = None
        processor.update_last_processed.return_value = None

        # When
        processor.process()

        # Then
        processor.read_bronze_table.assert_called_once()
        processor.imputations.assert_called_once()
        processor.write_delta_table.assert_called_once()

        written_df = processor.write_delta_table.call_args.args[0]
        assert {
            "Subtotal",
            "Total",
            "ValorDescuento",
            "ValorIva",
            "PorcentajeImpuestoSaludable",
        } <= set(written_df.columns)

        processor.update_last_processed.assert_called_once()
