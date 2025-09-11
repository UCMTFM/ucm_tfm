import json
from datetime import datetime
from unittest.mock import Mock

import libraries.silver_layer.processors.base as base_module
import pytest
from databricks.connect import DatabricksSession as SparkSession
from libraries.silver_layer.processors.facturas import FacturasProcessor
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
        "dataset": "facturas",
        "imputations": {
            "DireccionCliente": "",
            "TelefonoCliente": "N/A",
        },
        "sources": {
            "detalle_schema": "silver",
            "detalle_table": "detalle_facturas",
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

        with open(config_path, "r") as f:
            self.config = _json.load(f)
        self.spark = spark
        self.read_bronze_table = Mock()
        self.read_table = Mock()
        self.write_delta_table = Mock()
        self.update_last_processed = Mock()
        self.get_condition = Mock(return_value=None)

    monkeypatch.setattr(base_module.BaseProcessor, "__init__", fake_base_init)
    return FacturasProcessor(config_file)


# --------------------------- Test Suites ---------------------------


class TestFacturasImputations:
    def test_filters_null_idfactura_and_applies_imputations(self, spark, processor, config_dict):
        # Given
        schema = StructType(
            [
                StructField("IdFactura", IntegerType(), True),
                StructField("DireccionCliente", StringType(), True),
                StructField("TelefonoCliente", StringType(), True),
            ]
        )
        df_in = spark.createDataFrame(
            [
                (None, None, None),
                (101, None, None),
                (102, "Calle 1", "555-123"),
            ],
            schema=schema,
        )

        # When
        out = processor.imputations(df_in)
        rows = out.orderBy("IdFactura").collect()

        # Then
        assert [r["IdFactura"] for r in rows] == [101, 102]
        assert rows[0]["DireccionCliente"] == config_dict["imputations"]["DireccionCliente"]
        assert rows[0]["TelefonoCliente"] == config_dict["imputations"]["TelefonoCliente"]
        assert rows[1]["DireccionCliente"] == "Calle 1"
        assert rows[1]["TelefonoCliente"] == "555-123"


class TestFacturasTransformations:
    def _input_schema(self):
        return StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("NumeroFactura", StringType(), True),
                StructField("Fecha", TimestampType(), True),
                StructField("IdentificacionCliente", StringType(), True),
                StructField("Latitud", DoubleType(), True),
                StructField("Longitud", DoubleType(), True),
                StructField("RazonSocialCliente", StringType(), True),
                StructField("Retefuente", DoubleType(), True),
                StructField("FechaVencimiento", TimestampType(), True),
                StructField("FormaPago", IntegerType(), True),
                StructField("IdResolucion", IntegerType(), True),
                StructField("DireccionCliente", StringType(), True),
                StructField("TelefonoCliente", StringType(), True),
                StructField("Saldo", DoubleType(), True),
                StructField("CodigoRuta", IntegerType(), True),
                StructField("ReteIva", DoubleType(), True),
                StructField("IdEmpresa", IntegerType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("Cufe", StringType(), True),
                StructField("Anulada", IntegerType(), True),
                StructField("UsuarioAnulacion", StringType(), True),
                StructField("FechaAnulacion", TimestampType(), True),
                StructField("ComentarioAnulacion", StringType(), True),
                StructField("_ingestion_time", TimestampType(), True),
            ]
        )

    def test_renames_and_derives_fields(self, spark, processor):
        # Given
        schema = self._input_schema()
        df_in = spark.createDataFrame(
            [
                (
                    101,
                    "ABC-12345",
                    datetime(2024, 1, 15, 9, 0, 0),
                    "123",
                    4.5,
                    -74.1,
                    "Mi Cliente",
                    10.0,
                    datetime(2024, 2, 15, 0, 0, 0),
                    1,
                    77,
                    "Dir X",
                    "555",
                    0.0,
                    10,
                    3.0,
                    1,
                    datetime(2024, 1, 10, 0, 0, 0),
                    "cufe",
                    0,
                    None,
                    None,
                    None,
                    datetime(2024, 1, 15, 9, 1, 0),
                )
            ],
            schema=schema,
        )

        # When
        out = processor.transformations(df_in)
        row = out.collect()[0]

        # Then
        assert {
            "Identificacion",
            "ValorRetencion",
            "IdFormaPago",
            "Direccion",
            "Telefono",
            "IdRuta",
            "ValorReteIva",
            "Anulado",
            "MotivoAnulacion",
        }.issubset(out.columns)

        assert row["NombreNegocio"] == "Mi Cliente"
        assert row["Tipo"] == 1
        assert row["Anio"] == 2024 and row["Mes"] == 1 and row["Dia"] == 15
        assert row["Prefijo"] == "ABC"
        assert row["Consecutivo"] == "12345"

    def test_prefix_and_consecutive_when_no_dash(self, spark, processor):
        # Given
        schema = self._input_schema()
        row_dict = {
            "IdFactura": 102,
            "NumeroFactura": "SINPREFIJO",
            "Fecha": datetime(2024, 3, 1, 10, 0, 0),
            "IdentificacionCliente": "999",
            "Latitud": None,
            "Longitud": None,
            "RazonSocialCliente": "Cliente 2",
            "Retefuente": 0.0,
            "FechaVencimiento": None,
            "FormaPago": None,
            "IdResolucion": None,
            "DireccionCliente": None,
            "TelefonoCliente": None,
            "Saldo": None,
            "CodigoRuta": None,
            "ReteIva": None,
            "IdEmpresa": None,
            "FechaCreacion": None,
            "Cufe": None,
            "Anulada": None,
            "UsuarioAnulacion": None,
            "FechaAnulacion": None,
            "ComentarioAnulacion": None,
            "_ingestion_time": datetime(2024, 3, 1, 10, 1, 0),
        }

        df_in = spark.createDataFrame([row_dict], schema=schema)

        # When
        out = processor.transformations(df_in)
        row = out.collect()[0]

        # Then
        assert row["Prefijo"] == "SINPREFIJO"
        assert row["Consecutivo"] is None


class TestFacturasReadDetailData:
    def test_groups_detail_by_idfactura(self, spark, processor, config_dict):
        # Given
        det_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Subtotal", DoubleType(), True),
                StructField("Total", DoubleType(), True),
                StructField("ValorDescuento", DoubleType(), True),
                StructField("ValorIva", DoubleType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("ValorImpuestoSaludable", DoubleType(), True),
            ]
        )
        df_det = spark.createDataFrame(
            [
                (1001, 10.0, 12.0, 1.0, 2.0, 3.0, 0.5),
                (1001, 20.0, 24.0, 2.0, 4.0, 2.0, 1.0),
                (1002, 15.0, 18.0, 0.0, 3.0, 1.0, 0.2),
            ],
            schema=det_schema,
        )

        processor.get_condition.return_value = "dummy"
        processor.read_table.return_value = df_det

        # When
        out = processor.read_detail_data()

        # Then
        rows = sorted(out.collect(), key=lambda r: r["IdFactura"])
        assert rows[0]["IdFactura"] == 1001
        assert rows[0]["Subtotal"] == 30.0
        assert rows[0]["Total"] == 36.0
        assert rows[0]["Descuento"] == 3.0
        assert rows[0]["Iva"] == 6.0
        assert rows[0]["Cantidad"] == 5.0
        assert rows[0]["ImpuestoSaludable"] == 1.5

        assert rows[1]["IdFactura"] == 1002
        assert rows[1]["Subtotal"] == 15.0
        assert rows[1]["Total"] == 18.0
        assert rows[1]["Descuento"] == 0.0
        assert rows[1]["Iva"] == 3.0
        assert rows[1]["Cantidad"] == 1.0
        assert rows[1]["ImpuestoSaludable"] == 0.2

        processor.get_condition.assert_called_once_with(config_dict["sources"]["detalle_table"])
        schema_arg, table_arg, _ = processor.read_table.call_args.args
        assert schema_arg == config_dict["sources"]["detalle_schema"]
        assert table_arg == config_dict["sources"]["detalle_table"]


class TestFacturasAddDetailData:
    def test_joins_and_computes_retention_percentage(self, spark):
        # Given
        main_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("NumeroFactura", StringType(), True),
                StructField("ValorRetencion", DoubleType(), True),
            ]
        )
        df_fact = spark.createDataFrame(
            [
                (1001, "ABC-1", 5.0),
                (1002, "ABC-2", 0.0),
            ],
            schema=main_schema,
        )

        # df detail aggregated
        det_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Subtotal", DoubleType(), True),
                StructField("Total", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("Iva", DoubleType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("ImpuestoSaludable", DoubleType(), True),
            ]
        )
        df_det = spark.createDataFrame(
            [
                (1001, 50.0, 60.0, 0.0, 10.0, 5.0, 0.0),
                (1002, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            ],
            schema=det_schema,
        )

        # When
        out = FacturasProcessor.add_detail_data(df_fact, df_det).orderBy("IdFactura")
        rows = out.collect()

        # Then
        assert rows[0]["IdFactura"] == 1001
        assert rows[0]["PorcentajeRetencion"] == 10.00
        assert rows[1]["IdFactura"] == 1002
        assert rows[1]["PorcentajeRetencion"] is None


class TestFacturasProcess:
    def test_process_full_orchestration(self, spark, processor):
        # Given
        bronze_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), True),
                StructField("NumeroFactura", StringType(), True),
                StructField("Fecha", TimestampType(), True),
                StructField("IdentificacionCliente", StringType(), True),
                StructField("Latitud", DoubleType(), True),
                StructField("Longitud", DoubleType(), True),
                StructField("RazonSocialCliente", StringType(), True),
                StructField("Retefuente", DoubleType(), True),
                StructField("FechaVencimiento", TimestampType(), True),
                StructField("FormaPago", IntegerType(), True),
                StructField("IdResolucion", IntegerType(), True),
                StructField("DireccionCliente", StringType(), True),
                StructField("TelefonoCliente", StringType(), True),
                StructField("Saldo", DoubleType(), True),
                StructField("CodigoRuta", IntegerType(), True),
                StructField("ReteIva", DoubleType(), True),
                StructField("IdEmpresa", IntegerType(), True),
                StructField("FechaCreacion", TimestampType(), True),
                StructField("Cufe", StringType(), True),
                StructField("Anulada", IntegerType(), True),
                StructField("UsuarioAnulacion", StringType(), True),
                StructField("FechaAnulacion", TimestampType(), True),
                StructField("ComentarioAnulacion", StringType(), True),
                StructField("_ingestion_time", TimestampType(), True),
            ]
        )
        df_bronze = spark.createDataFrame(
            [
                (
                    1001,
                    "ABC-1",
                    datetime(2024, 1, 10, 0, 0, 0),
                    "123",
                    4.5,
                    -74.1,
                    "Mi Cliente",
                    5.0,
                    None,
                    1,
                    77,
                    None,
                    None,
                    0.0,
                    10,
                    3.0,
                    1,
                    datetime(2024, 1, 10, 0, 0, 0),
                    "cufe",
                    0,
                    None,
                    None,
                    None,
                    datetime(2024, 1, 10, 0, 0, 1),
                ),
            ],
            schema=bronze_schema,
        )

        det_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Subtotal", DoubleType(), True),
                StructField("Total", DoubleType(), True),
                StructField("Descuento", DoubleType(), True),
                StructField("Iva", DoubleType(), True),
                StructField("Cantidad", DoubleType(), True),
                StructField("ImpuestoSaludable", DoubleType(), True),
            ]
        )
        df_detail = spark.createDataFrame(
            [(1001, 50.0, 60.0, 0.0, 10.0, 5.0, 0.0)],
            schema=det_schema,
        )

        processor.read_bronze_table.return_value = df_bronze
        processor.read_detail_data = Mock(return_value=df_detail)
        processor.write_delta_table.return_value = None
        processor.update_last_processed.return_value = None

        # When
        processor.process()

        # Then
        processor.read_bronze_table.assert_called_once()
        processor.read_detail_data.assert_called_once()
        processor.write_delta_table.assert_called_once()
        processor.update_last_processed.assert_called_once()

        written_df = processor.write_delta_table.call_args.args[0]
        assert {
            "IdFactura",
            "Subtotal",
            "Total",
            "Descuento",
            "Iva",
            "Cantidad",
            "ImpuestoSaludable",
            "PorcentajeRetencion",
        }.issubset(written_df.columns)
