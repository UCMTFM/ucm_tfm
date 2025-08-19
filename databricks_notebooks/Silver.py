# Databricks notebook source
dbutils.widgets.text("dataset", "SUSTITUIR")

# COMMAND ----------

from silver_layer.process_engine import ProcessorEngine
dataset = dbutils.widgets.get("dataset")
config_dbfs_path = f"dbfs:/FileStore/config/silver/{dataset}_config.json"
engine = ProcessorEngine(dataset, config_dbfs_path)
engine.process()