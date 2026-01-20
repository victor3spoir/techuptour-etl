FROM apache/airflow:slim-3.1.6-python3.12 AS base
USER root
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  graphviz \
  unzip \
  curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir  \
  "apache-airflow[postgres,pandas]" \
  asyncpg \
  apache-airflow-providers-apache-spark \
  apache-airflow-providers-amazon \
  pydantic httpx