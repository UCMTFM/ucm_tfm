# from .processors import DetalleFacturasProcessor
from processors import DetalleFacturasProcessor, FacturasProcessor

PROCESSOR_REGISTRY = {"detalle_facturas": DetalleFacturasProcessor, "facturas": FacturasProcessor}
