import os

import mlflow
from loguru import logger
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F
from xgboost.spark import SparkXGBRegressor

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class ModelXGB:

    def __init__(self, exp_path: str, catalog: str) -> None:
        """
        Initialize the BaseIngestor instance.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.exp_path = exp_path
        self.catalog = catalog
        self.client = MlflowClient()

    def train_model(self, df_encoded: DF, categorical_cols: list[str], numeric_cols: list[str]):
        df = df_encoded.withColumn("is_valid", (F.rand(seed=42) < 0.2))

        ohe_cols = [f"{cat_var}_ohe" for cat_var in categorical_cols]
        label_col = "CantidadTotal"
        numeric_no_label = [c for c in numeric_cols if c != label_col]
        feature_cols = ohe_cols + numeric_no_label

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        xgb = SparkXGBRegressor(
            features_col="features",
            label_col=label_col,
            prediction_col="prediction",
            validation_indicator_col="is_valid",
            objective="reg:squarederror",
            eval_metric="rmse",
            num_workers=self.spark.sparkContext.defaultParallelism,
            max_depth=8,
            eta=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.0,
            reg_lambda=1.0,
            num_round=150,
            early_stopping_rounds=30,
        )

        pipe = Pipeline(stages=[assembler, xgb])
        model = pipe.fit(df)
        return model, df, feature_cols, label_col

    def evaluate_xgb_model(model, df, label_col="CantidadTotal"):
        pred = model.transform(df)

        e_rmse = RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="rmse"
        ).evaluate(pred.filter(F.col("is_valid") == 1))

        e_r2 = RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="r2"
        ).evaluate(pred.filter(F.col("is_valid") == 1))

        return {"valid_rmse": float(e_rmse), "valid_r2": float(e_r2)}

    def train_with_mlflow(
        self, df_encoded: DF, categorical_cols: list[str], numeric_cols: list[str], exp_id: str
    ):
        mlflow.set_tags(
            {
                "project": "TFM-demand-forecast",
                "framework": "Spark + XGBoost",
                "data_window": ">=2023",
                "env": "databricks",
            }
        )

        with mlflow.start_run(
            nested=mlflow.active_run() is not None, run_name=f"xgb-demand-{exp_id}"
        ) as run:
            run_id = run.info.run_id

            mlflow.log_params(
                {
                    "split_seed": 42,
                    "valid_ratio": 0.2,
                    "label_col": "CantidadTotal",
                    "categorical_cols": categorical_cols,
                    "num_numeric_input": len([c for c in numeric_cols if c != "CantidadTotal"]),
                }
            )

            model, df, feature_cols, label_col = self.train_model(
                df_encoded, categorical_cols, numeric_cols
            )

            metrics = ModelXGB.evaluate_xgb_model(model, df, label_col=label_col)
            mlflow.log_metrics(metrics)

            xgb_est = None
            for s in model.stages:
                if s.__class__.__name__ in ("SparkXGBRegressor", "SparkXGBRegressorModel"):
                    xgb_est = s
            if xgb_est is not None:
                params_to_log = {}
                for pname in [
                    "max_depth",
                    "eta",
                    "subsample",
                    "colsample_bytree",
                    "reg_alpha",
                    "reg_lambda",
                    "num_round",
                    "early_stopping_rounds",
                    "eval_metric",
                    "objective",
                ]:
                    try:
                        val = (
                            getattr(xgb_est, pname)
                            if hasattr(xgb_est, pname)
                            else xgb_est.getOrDefault(pname)
                        )
                    except Exception:
                        val = None
                    if val is not None:
                        params_to_log[pname] = val
                if params_to_log:
                    mlflow.log_params(params_to_log)

            mlflow.log_text("\n".join(feature_cols), "inputs/feature_cols.txt")
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=f"xgb_demand_model_{exp_id}",
            )

            logger.info(f"Run logged in MLflow (experiment: {self.exp_path})")
            logger.info(f"Run ID: {run_id}")
            logger.info(f"Metrics: {metrics}")
            return run_id, metrics

    def run_experiment(self, exp_id: str, categorical_cols: list[str], since_year: int = 2023):
        mlflow.set_experiment(self.exp_path)
        numeric_cols = ["CantidadTotalPrevia", "CantidadTotal"]
        self.spark.sql(f"USE CATALOG {self.catalog}")
        df_encoded = self.spark.table("gold.sales_by_route").drop("prediction")
        run_id, metrics = self.train_with_mlflow(
            df_encoded.filter(F.col("Anio") >= since_year), categorical_cols, numeric_cols, exp_id
        )
        mlflow.end_run()
