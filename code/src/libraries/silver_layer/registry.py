import os

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    from processors import (
        DetalleFacturasProcessor,
        DetalleNotasCreditoProcessor,
        FacturasProcessor,
        NotasCreditoProcessor,
    )
else:
    from .processors import (
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
