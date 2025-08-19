# Databricks notebook source
dbutils.widgets.text("dataset", "SUSTITUIR")
dbutils.widgets.text("workload", "SUSTITUIR")

# COMMAND ----------

from bronze_layer.ingest_engine import Engine

dataset = dbutils.widgets.get("dataset")
workload = dbutils.widgets.get("workload")
config_dbfs_path = f"dbfs:/FileStore/config/bronze/{dataset}_config.json"
engine = Engine(workload, config_dbfs_path)
engine.ingest()