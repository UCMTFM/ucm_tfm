# from .processors import DetalleFacturasProcessor
from processors import (
    DetalleFacturasProcessor,
    DetalleNotasCreditoProcessor,
    FacturasProcessor,
    NotasCreditoProcessor,
)

PROCESSOR_REGISTRY = {
    "detalle_facturas": DetalleFacturasProcessor,
    "facturas": FacturasProcessor,
    "detalle_notas_credito": DetalleNotasCreditoProcessor,
    "notas_credito": NotasCreditoProcessor,
}
