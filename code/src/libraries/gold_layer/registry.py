import os

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    from processors import SalesByRouteProcessor
else:
    from .processors import SalesByRouteProcessor

PROCESSOR_REGISTRY = {"sales_by_route": SalesByRouteProcessor}
