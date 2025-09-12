import os

exec_env = os.getenv("EXECUTION_ENV", "local")

if exec_env == "databricks-connect":
    from processors import (
        GoldDimClientesProcessor,
        GoldDimFormaPagoProcessor,
        GoldDimProductosProcessor,
        GoldDimRutasProcessor,
        GoldDimUbicacionesProcessor,
        GoldFactDetalleFacturasProcessor,
        GoldFactFacturasProcessor,
        SalesByRouteProcessor,
    )
else:
    from .processors import (
        GoldDimClientesProcessor,
        GoldDimFormaPagoProcessor,
        GoldDimProductosProcessor,
        GoldDimRutasProcessor,
        GoldDimUbicacionesProcessor,
        GoldFactDetalleFacturasProcessor,
        GoldFactFacturasProcessor,
        SalesByRouteProcessor,
    )

PROCESSOR_REGISTRY = {
    "sales_by_route": SalesByRouteProcessor,
    "dim_clientes": GoldDimClientesProcessor,
    "dim_ubicaciones": GoldDimUbicacionesProcessor,
    "dim_rutas": GoldDimRutasProcessor,
    "dim_productos": GoldDimProductosProcessor,
    "dim_forma_pago": GoldDimFormaPagoProcessor,
    "fact_facturas": GoldFactFacturasProcessor,
    "fact_detalle_facturas": GoldFactDetalleFacturasProcessor,
}
