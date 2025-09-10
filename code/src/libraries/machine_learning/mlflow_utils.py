import mlflow
import mlflow.spark
import pandas as pd
from loguru import logger
from mlflow.tracking import MlflowClient


class MLFlowUtils:
    """
    Utility helpers around MLflow experiments, runs, and model registry.

    This class focuses on:
      - Enforcing a *unique* tag across registered models (useful to mark the
        current "best" model).
      - Selecting the best run from an experiment based on validation metrics.
      - Registering a model artifact from a run to the MLflow Model Registry.
    """

    def __init__(self):
        """Initialize the MLflow client."""
        self.client = MlflowClient()

    def set_unique_registered_model_tag(self, model_name: str, key: str, value: str):
        """
        Set a tag on one registered model and remove the same tag from all others.
        This is useful to keep a single model marked as "best"

        Args:
            model_name (str): Target registered model name to keep tagged.
            key (str): Tag key (e.g., "best_model_selection").
            value (str): Tag value (e.g., "by_valid_rmse_then_r2").
        """
        victims = []
        flt = f"tag.{key} = '{value}'"
        victims = self.client.search_registered_models(filter_string=flt)
        for rm in victims:
            if rm.name != model_name:
                self.client.delete_registered_model_tag(name=rm.name, key=key)

        self.client.set_registered_model_tag(name=model_name, key=key, value=value)

    def find_best_run(self, exp_id: str) -> tuple[str, str, str, pd.DataFrame]:
        """
        Find the best finished run in an experiment using (validation_rmse ASC, validation_r2 DESC).
        Pulls all FINISHED runs with a non-negative `valid_rmse`, sorts them by
        ascending RMSE and tie-breaks by descending R², and returns the top run.

        Args:
            exp_id (str | int): MLflow experiment ID.

        Returns:
            Tuple[str, str, str, pandas.DataFrame]:
                - best_run_id: ID of the selected run.
                - cat_features_used: Value of `params.categorical_cols` for that run.
                - model_name: The run name from `tags.mlflow.runName`.
                - df_sorted: A pandas DataFrame with all candidate runs sorted by the criteria.
        """
        df = mlflow.search_runs(
            experiment_ids=[exp_id],
            filter_string="attributes.status = 'FINISHED' and metrics.valid_rmse >= 0",
            output_format="pandas",
        )

        if df.empty:
            raise RuntimeError("There are not runs with 'valid_rmse'.")

        df_sorted = df.sort_values(
            by=["metrics.valid_rmse", "metrics.valid_r2"],
            ascending=[True, False],
            ignore_index=True,
        )

        best = df_sorted.iloc[0]
        best_run_id = best["run_id"]
        best_rmse = best["metrics.valid_rmse"]
        best_r2 = best["metrics.valid_r2"]
        cat_features_used = best["params.categorical_cols"]
        model_name = best["tags.mlflow.runName"]

        logger.info(
            f"Best run: {best_run_id} | valid_rmse={best_rmse:.4f} | valid_r2={best_r2:.4f}"
        )
        logger.info(f"Best categorical features used: {cat_features_used}")
        return best_run_id, cat_features_used, model_name, df_sorted

    def set_best_model(self, best_run_id: str, model_name: str) -> None:
        """
        Register the model artifact from a run and tag the registered model as the unique "best".

        Args:
            best_run_id (str): The ID of the run whose model should be registered.
            model_name (str): Target registered model name.
        """
        model_uri = f"runs:/{best_run_id}/model"
        logger.info(f"Modelo cargado como SparkModel desde: {model_uri}")

        result = mlflow.register_model(model_uri, model_name)
        self.set_unique_registered_model_tag(
            model_name, "best_model_selection", "by_valid_rmse_then_r2"
        )
        logger.info(f"Registrado como {model_name}, version={result.version}")
