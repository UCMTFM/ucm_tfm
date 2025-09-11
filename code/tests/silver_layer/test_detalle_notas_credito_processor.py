import json
from datetime import datetime
from unittest.mock import Mock

import libraries.silver_layer.processors.base as base_module
import pytest
from databricks.connect import DatabricksSession as SparkSession
from libraries.silver_layer.processors.detalle_notas_credito import DetalleNotasCreditoProcessor
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
        "dataset": "detalle_notas_credito",
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
    return DetalleNotasCreditoProcessor(config_file)


# --------------------------- Test Suites ---------------------------


class TestDetalleNotasCreditoTransformations:
    def test_derives_fields_and_totals_with_positive_subtotal(self, spark, processor):
        # Given
        # Case: Devolucion=1, Rotacion=2 -> DevolucionesRotaciones=3
        # Subtotal (of factura) = 50, Descuento=5 -> PorcentajeDescuento=10
        # SubtotalDevoluciones = 10 * 3 = 30.00
        # ValorDescuentoDevoluciones = 30 * 0.10 = 3.00
        # ValorIvaDevoluciones = 3 * 19% = 0.57
        # ValorImpuestoSaludableDevoluciones = round(30 * 5%, 2) = 1.50
        # TotalDevoluciones = round(30 - 3 + 0.57 + 1.5, 2) = 29.07
        schema = StructType(
            [
                StructField("IdDetalle", IntegerType(), False),
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
                StructField("ValorUnitario", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("PorcentajeIva", DoubleType(), True),
                StructField("ISPPorcentaje", DoubleType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("Subtotal", DoubleType(), True),
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
                    1.0,
                    2.0,
                    10.0,
                    5.0,
                    19.0,
                    5.0,
                    datetime(2024, 2, 10, 10, 15, 0),
                    50.0,
                    datetime(2024, 2, 10, 10, 16, 0),
                )
            ],
            schema=schema,
        )

        # When
        out = processor.transformations(df_in)
        row = out.collect()[0]

        # Then:
        assert "PorcentajeImpuestoSaludable" in out.columns
        assert "Descuento" not in out.columns
        assert "Subtotal" not in out.columns
        assert row["DevolucionesRotaciones"] == 3.0
        assert row["Anio"] == 2024 and row["Mes"] == 2 and row["Dia"] == 10

        assert row["SubtotalDevoluciones"] == 30.0
        assert row["PorcentajeDescuento"] == pytest.approx(10.0)
        assert row["ValorDescuentoDevoluciones"] == 3.0
        assert row["ValorIvaDevoluciones"] == pytest.approx(0.57, rel=1e-6)
        assert row["ValorImpuestoSaludableDevoluciones"] == 1.5
        assert row["TotalDevoluciones"] == pytest.approx(29.07, rel=1e-6)

    def test_porcentaje_descuento_is_zero_when_subtotal_is_zero(self, spark, processor):
        # Given: Subtotal=0 -> PorcentajeDescuento=0
        schema = StructType(
            [
                StructField("IdDetalle", IntegerType(), False),
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
                StructField("ValorUnitario", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("PorcentajeIva", DoubleType(), True),
                StructField("ISPPorcentaje", DoubleType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("Subtotal", DoubleType(), True),
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
                    2.0,
                    10.0,
                    5.0,
                    19.0,
                    0.0,
                    datetime(2023, 5, 1, 12, 0, 0),
                    0.0,
                    datetime(2023, 5, 1, 12, 0, 1),
                )
            ],
            schema=schema,
        )

        # When
        out = processor.transformations(df_in)
        row = out.collect()[0]

        # Then
        assert row["PorcentajeDescuento"] == 0
        assert row["SubtotalDevoluciones"] == 20.0
        assert row["ValorDescuentoDevoluciones"] == 0.0
        assert row["ValorIvaDevoluciones"] == pytest.approx(0.38, rel=1e-6)


class TestDetalleNotasCreditoProcess:
    def test_process_reads_imputes_logs_transforms_writes_updates(self, spark, processor):
        # Given
        schema = StructType(
            [
                StructField("IdDetalle", IntegerType(), False),
                StructField("IdFactura", IntegerType(), False),
                StructField("IdProducto", IntegerType(), False),
                StructField("NombreProducto", StringType(), True),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
                StructField("ValorUnitario", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("PorcentajeIva", DoubleType(), True),
                StructField("ISPPorcentaje", DoubleType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("Subtotal", DoubleType(), True),
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
                    1.0,
                    1.0,
                    10.0,
                    2.0,
                    19.0,
                    5.0,
                    datetime(2024, 1, 1, 0, 0, 0),
                    40.0,
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
        processor.update_last_processed.assert_called_once()
        written_df = processor.write_delta_table.call_args.args[0]
        assert {
            "DevolucionesRotaciones",
            "SubtotalDevoluciones",
            "PorcentajeDescuento",
            "ValorDescuentoDevoluciones",
            "ValorIvaDevoluciones",
            "ValorImpuestoSaludableDevoluciones",
            "TotalDevoluciones",
            "PorcentajeImpuestoSaludable",
            "Anio",
            "Mes",
            "Dia",
        }.issubset(set(written_df.columns))
