import os

from loguru import logger

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    from .registry import PROCESSOR_REGISTRY
else:
    from gold_layer.registry import PROCESSOR_REGISTRY


class GoldEngine:
    """
    Interface class for executing the appropriate data processor
    based on the dataset name.

    This engine dynamically loads and runs the processor class registered
    for a specific dataset via the `PROCESSOR_REGISTRY`.

    Attributes:
        dataset (str): The dataset identifier.
        processor (BaseProcessor): An instance of a concrete processor class.
    """

    def __init__(self, dataset: str, config_path: str) -> None:
        """
        Initialize the ProcessorEngine with the corresponding processor.

        Args:
            dataset (str): Name of the dataset (must exist in `PROCESSOR_REGISTRY`).
            config_path (str): Path to the configuration file for the processor.
        """
        self.dataset = dataset
        processor_class = PROCESSOR_REGISTRY.get(dataset)

        if not processor_class:
            error = f"No gold processor found for '{dataset}' dataset"
            logger.error(error)
            raise ValueError(error)

        self.processor = processor_class(config_path)

    def process(self) -> None:
        """
        Execute the data processing pipeline for the given dataset.
        """
        self.processor.process()


if __name__ == "__main__":
    dataset = "sales_by_route"
    config_path = f"dbfs:/FileStore/config/gold/{dataset}_config.json"
    engine = GoldEngine(dataset, config_path)
    engine.process()
