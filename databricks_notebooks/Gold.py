# Databricks notebook source
dbutils.widgets.text("dataset", "SUSTITUIR")

# COMMAND ----------

from gold_layer.gold_engine import GoldEngine

dataset = dbutils.widgets.get("dataset")
config_dbfs_path = f"dbfs:/FileStore/config/silver/{dataset}_config.json"
engine = GoldEngine(dataset, config_dbfs_path)
engine.process()
