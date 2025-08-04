from loguru import logger

# from silver_layer.registry import PROCESSOR_REGISTRY
from registry import PROCESSOR_REGISTRY


class ProcessorEngine:
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
            error = f"No processor found for '{dataset}' dataset"
            logger.error(error)
            raise ValueError(error)

        self.processor = processor_class(config_path)

    def process(self) -> None:
        """
        Execute the data processing pipeline for the given dataset.
        """
        self.processor.process()


if __name__ == "__main__":
    dataset = "detalle_facturas"
    config_path = f"./config_files/silver/{dataset}_config.json"
    engine = ProcessorEngine(dataset, config_path)
    engine.process()
