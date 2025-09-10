import mlflow
import mlflow.spark
from loguru import logger
from mlflow.tracking import MlflowClient


class MLFlowUtils:
    def __init__(self):
        self.client = MlflowClient()

    def set_unique_registered_model_tag(self, model_name: str, key: str, value: str):
        victims = []
        flt = f"tag.{key} = '{value}'"
        victims = self.client.search_registered_models(filter_string=flt)
        for rm in victims:
            if rm.name != model_name:
                self.client.delete_registered_model_tag(name=rm.name, key=key)

        self.client.set_registered_model_tag(name=model_name, key=key, value=value)

    def find_best_run(self, exp_id):
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

    def set_best_model(self, best_run_id, model_name):
        model_uri = f"runs:/{best_run_id}/model"
        loaded = mlflow.spark.load_model(model_uri)
        logger.info(f"Modelo cargado como SparkModel desde: {model_uri}")

        result = mlflow.register_model(model_uri, model_name)
        self.set_unique_registered_model_tag(
            model_name, "best_model_selection", "by_valid_rmse_then_r2"
        )
        logger.info(f"Registrado como {model_name}, version={result.version}")
