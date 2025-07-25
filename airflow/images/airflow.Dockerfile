FROM apache/airflow:3.0.1-python3.12

USER root

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-databricks
