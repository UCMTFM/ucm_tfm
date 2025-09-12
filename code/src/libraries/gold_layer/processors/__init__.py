from .dim_clientes import GoldDimClientesProcessor
from .dim_forma_pago import GoldDimFormaPagoProcessor
from .dim_productos import GoldDimProductosProcessor
from .dim_rutas import GoldDimRutasProcessor
from .dim_ubicaciones import GoldDimUbicacionesProcessor
from .fact_detalle_facturas import GoldFactDetalleFacturasProcessor
from .fact_facturas import GoldFactFacturasProcessor
from .sales_by_route import SalesByRouteProcessor

__all__ = [
    "SalesByRouteProcessor",
    "GoldDimClientesProcessor",
    "GoldDimUbicacionesProcessor",
    "GoldDimRutasProcessor",
    "GoldDimProductosProcessor",
    "GoldDimFormaPagoProcessor",
    "GoldFactFacturasProcessor",
    "GoldFactDetalleFacturasProcessor",
]
