# syntax=docker/dockerfile:1.7
# Airflow 2.10.5 with a JRE and the platform dependencies, so tasks can run
# Spark jobs (local mode) and Python ingestion code directly.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY docker/common/fetch_spark_jars.sh /usr/local/bin/fetch_spark_jars.sh
RUN chmod +x /usr/local/bin/fetch_spark_jars.sh

USER airflow
COPY docker/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
# Install against the official constraints so Airflow's own dependencies stay intact.
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      -r /tmp/requirements-airflow.txt \
 && fetch_spark_jars.sh

ENV PYTHONPATH=/opt/platform
