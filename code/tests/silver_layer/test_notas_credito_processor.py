import json
from datetime import datetime
from unittest.mock import Mock

import libraries.silver_layer.processors.base as base_module
import pytest
from databricks.connect import DatabricksSession as SparkSession
from libraries.silver_layer.processors.notas_credito import NotasCreditoProcessor
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
        "dataset": "notas_credito",
        "sources": {
            "detalle_schema": "silver",
            "detalle_table": "detalle_notas_credito",
            "detalle_facturas_schema": "silver",
            "detalle_facturas_table": "detalle_facturas",
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
    return NotasCreditoProcessor(config_file)


# --------------------------- Test Suites ---------------------------


class TestNotasCreditoTransformations:
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

    def test_renames_and_derives(self, spark, processor):
        # Given
        schema = self._input_schema()
        df_in = spark.createDataFrame(
            [
                (
                    2001,
                    "NC-0007",
                    datetime(2024, 5, 2, 10, 0, 0),
                    "123",
                    4.5,
                    -74.1,
                    "Cliente NC",
                    None,
                    2,
                    99,
                    "Dir Z",
                    "555-000",
                    0.0,
                    77,
                    3.0,
                    1,
                    datetime(2024, 5, 1, 0, 0, 0),
                    "cufe",
                    0,
                    None,
                    None,
                    None,
                    datetime(2024, 5, 2, 10, 0, 1),
                )
            ],
            schema=schema,
        )

        # When
        out = processor.transformations(df_in)
        row = out.collect()[0]

        # Then
        expected_cols = {
            "Identificacion",
            "IdFormaPago",
            "Direccion",
            "Telefono",
            "IdRuta",
            "ValorReteIva",
            "Anulado",
            "MotivoAnulacion",
        }
        assert expected_cols.issubset(out.columns)

        assert row["NombreNegocio"] == "Cliente NC"
        assert row["Anio"] == 2024 and row["Mes"] == 5 and row["Dia"] == 2
        assert row["Prefijo"] == "NC"
        assert row["Consecutivo"] == "0007"

    def test_prefix_consecutive_without_dash(self, spark, processor):
        # Given
        schema = self._input_schema()
        row_dict = {
            "IdFactura": 2002,
            "NumeroFactura": "SINPREFIJO",
            "Fecha": datetime(2024, 6, 1, 0, 0, 0),
            "IdentificacionCliente": "999",
            "Latitud": None,
            "Longitud": None,
            "RazonSocialCliente": "Cliente 2",
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
            "_ingestion_time": datetime(2024, 6, 1, 0, 0, 1),
        }
        df_in = spark.createDataFrame([row_dict], schema=schema)

        # When
        out = processor.transformations(df_in)

        # Then
        row = out.collect()[0]
        assert row["Prefijo"] == "SINPREFIJO"
        assert row["Consecutivo"] is None


class TestNotasCreditoReadDetailData:
    def test_groups_detalle_notas_credito(self, spark, processor, config_dict):
        # Given
        det_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
                StructField("DevolucionesRotaciones", DoubleType(), True),
                StructField("SubtotalDevoluciones", DoubleType(), True),
                StructField("ValorDescuentoDevoluciones", DoubleType(), True),
                StructField("ValorImpuestoSaludableDevoluciones", DoubleType(), True),
                StructField("TotalDevoluciones", DoubleType(), True),
            ]
        )
        df_det = spark.createDataFrame(
            [
                (3001, 1.0, 2.0, 3.0, 30.0, 3.0, 1.5, 29.5),
                (3001, 0.0, 1.0, 1.0, 10.0, 1.0, 0.5, 9.5),
                (3002, 2.0, 0.0, 2.0, 20.0, 2.0, 1.0, 19.0),
            ],
            schema=det_schema,
        )

        processor.get_condition.return_value = "dummy"
        processor.read_table.return_value = df_det

        # When
        out = processor.read_detail_data()
        rows = sorted(out.collect(), key=lambda r: r["IdFactura"])

        # Then
        r1 = rows[0]
        assert r1["IdFactura"] == 3001
        assert r1["Devolucion"] == 1.0
        assert r1["Rotacion"] == 3.0
        assert r1["CantidadDevolucionesRotaciones"] == 4.0
        assert r1["SubtotalDevoluciones"] == 40.0
        assert r1["ValorDescuentoDevoluciones"] == 4.0
        assert r1["ValorImpuestoSaludableDevoluciones"] == 2.0
        assert r1["TotalDevoluciones"] == 39.0

        r2 = rows[1]
        assert r2["IdFactura"] == 3002
        assert r2["Devolucion"] == 2.0
        assert r2["Rotacion"] == 0.0
        assert r2["CantidadDevolucionesRotaciones"] == 2.0
        assert r2["SubtotalDevoluciones"] == 20.0
        assert r2["ValorDescuentoDevoluciones"] == 2.0
        assert r2["ValorImpuestoSaludableDevoluciones"] == 1.0
        assert r2["TotalDevoluciones"] == 19.0

        processor.get_condition.assert_called_once_with(config_dict["sources"]["detalle_table"])
        schema_arg, table_arg, _ = processor.read_table.call_args.args
        assert schema_arg == config_dict["sources"]["detalle_schema"]
        assert table_arg == config_dict["sources"]["detalle_table"]


class TestNotasCreditoGetType2:
    def test_classifies_only_when_all_rows_match_rule(self, spark, processor, config_dict):
        # Given
        det_fact_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Cantidad", DoubleType(), True),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
            ]
        )
        df_det_fact = spark.createDataFrame(
            [
                # Factura 4001: TODAS las filas cumplen (Cantidad == Devolucion + Rotacion)
                (4001, 3.0, 1.0, 2.0),
                (4001, 5.0, 2.0, 3.0),
                # Factura 4002: una fila NO cumple
                (4002, 2.0, 1.0, 1.0),  # cumple
                (4002, 4.0, 1.0, 1.0),  # NO cumple (1+1 != 4)
            ],
            schema=det_fact_schema,
        )

        # When
        processor.get_condition.return_value = "cond"
        processor.read_table.return_value = df_det_fact

        out = processor.get_type_2().orderBy("IdFactura")
        rows = out.collect()

        # Then
        assert len(rows) == 1
        assert rows[0]["IdFactura"] == 4001
        assert rows[0]["Tipo"] == 2

        processor.get_condition.assert_called_once_with(
            config_dict["sources"]["detalle_facturas_table"]
        )
        schema_arg, table_arg, _ = processor.read_table.call_args.args
        assert schema_arg == config_dict["sources"]["detalle_facturas_schema"]
        assert table_arg == config_dict["sources"]["detalle_facturas_table"]


class TestNotasCreditoAddDetailData:
    def test_left_then_inner_join_behavior_and_tipo_value(self, spark):
        # main df transformed (notas_credito)
        main_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("NumeroFactura", StringType(), True),
            ]
        )
        df_nc = spark.createDataFrame(
            [
                (5001, "NC-1"),
                (5002, "NC-2"),
            ],
            schema=main_schema,
        )

        # df detalles aggregated
        det_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
                StructField("CantidadDevolucionesRotaciones", DoubleType(), True),
                StructField("SubtotalDevoluciones", DoubleType(), True),
                StructField("ValorDescuentoDevoluciones", DoubleType(), True),
                StructField("ValorImpuestoSaludableDevoluciones", DoubleType(), True),
                StructField("TotalDevoluciones", DoubleType(), True),
            ]
        )
        df_det = spark.createDataFrame(
            [
                (5001, 1.0, 1.0, 2.0, 20.0, 2.0, 1.0, 19.0),
                (5002, 0.5, 0.5, 1.0, 10.0, 1.0, 0.5, 9.5),
            ],
            schema=det_schema,
        )

        # df_type_2 ONLY contains (Tipo=2)
        type2_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Tipo", IntegerType(), True),
            ]
        )
        df_type_2 = spark.createDataFrame(
            [
                (5001, 2),
            ],
            schema=type2_schema,
        )

        # When
        out = NotasCreditoProcessor.add_detail_data(df_nc, df_det, df_type_2).orderBy("IdFactura")
        rows = out.collect()

        # Then
        assert len(rows) == 1
        assert rows[0]["IdFactura"] == 5001
        assert rows[0]["Tipo"] == 2


class TestNotasCreditoProcess:
    def test_full_pipeline(self, spark, processor, config_dict):
        # Given: Bronze notas_credito
        bronze_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("NumeroFactura", StringType(), True),
                StructField("Fecha", TimestampType(), True),
                StructField("IdentificacionCliente", StringType(), True),
                StructField("Latitud", DoubleType(), True),
                StructField("Longitud", DoubleType(), True),
                StructField("RazonSocialCliente", StringType(), True),
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

        row_bronze = {
            "IdFactura": 6001,
            "NumeroFactura": "NC-9",
            "Fecha": datetime(2024, 7, 1, 0, 0, 0),
            "IdentificacionCliente": "C001",
            "Latitud": None,
            "Longitud": None,
            "RazonSocialCliente": "Cliente X",
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
            "_ingestion_time": datetime(2024, 7, 1, 0, 0, 1),
        }

        df_bronze = spark.createDataFrame([row_bronze], schema=bronze_schema)

        # detalle_notas_credito aggregated
        det_nc_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
                StructField("DevolucionesRotaciones", DoubleType(), True),
                StructField("SubtotalDevoluciones", DoubleType(), True),
                StructField("ValorDescuentoDevoluciones", DoubleType(), True),
                StructField("ValorImpuestoSaludableDevoluciones", DoubleType(), True),
                StructField("TotalDevoluciones", DoubleType(), True),
            ]
        )
        df_det_nc = spark.createDataFrame(
            [(6001, 1.0, 1.0, 2.0, 20.0, 2.0, 1.0, 19.0)],
            schema=det_nc_schema,
        )

        # detalle_facturas (for get_type_2)
        det_fact_schema = StructType(
            [
                StructField("IdFactura", IntegerType(), False),
                StructField("Cantidad", DoubleType(), True),
                StructField("Devolucion", DoubleType(), True),
                StructField("Rotacion", DoubleType(), True),
            ]
        )
        df_det_fact = spark.createDataFrame(
            [
                (6001, 3.0, 1.0, 2.0),
                (6001, 5.0, 2.0, 3.0),
            ],
            schema=det_fact_schema,
        )

        processor.read_bronze_table.return_value = df_bronze

        def fake_read_table(schema_name, table_name, condition):
            if table_name == config_dict["sources"]["detalle_table"]:
                return df_det_nc
            if table_name == config_dict["sources"]["detalle_facturas_table"]:
                return df_det_fact
            raise ValueError("Tabla inesperada")

        processor.read_table.side_effect = fake_read_table

        # When
        processor.process()

        # Then
        processor.read_bronze_table.assert_called_once()
        assert processor.read_table.call_count == 2
        processor.write_delta_table.assert_called_once()
        processor.update_last_processed.assert_called_once()

        written_df = processor.write_delta_table.call_args.args[0]
        cols = set(written_df.columns)
        assert {"IdFactura", "Devolucion", "Rotacion", "TotalDevoluciones", "Tipo"}.issubset(cols)
