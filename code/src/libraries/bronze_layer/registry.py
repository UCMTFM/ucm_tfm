import os

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    from ingestors import BatchIngestor, StreamingIngestor
else:
    from .ingestors import BatchIngestor, StreamingIngestor

INGESTOR_REGISTRY = {"batch": BatchIngestor, "streaming": StreamingIngestor}
