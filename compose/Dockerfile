FROM apache/airflow:slim-3.1.6-python3.12 AS base
USER airflow
RUN pip install --no-cache-dir  "apache-airflow[postgres]==3.1.6" \
  asyncpg \
  apache-airflow-providers-apache-spark \
  apache-airflow-providers-amazon