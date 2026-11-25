# syntax=docker/dockerfile:1.7
# Platform runtime image: Python 3.11 + OpenJDK 17 + PySpark 3.5.4 + Delta 3.3.0
# Used by: spark-streaming, event-producer, streamlit, one-shot jobs and tests.
FROM python:3.11.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tini make \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONPATH=/opt/platform \
    PYSPARK_PYTHON=python \
    SPARK_LOCAL_IP=0.0.0.0

WORKDIR /opt/platform

# Heaviest dependency in its own cached layer; the pip cache mount lets a
# retried build resume instead of re-downloading on slow networks.
RUN --mount=type=cache,target=/root/.cache/pip pip install pyspark==3.5.4
COPY requirements.txt requirements-dev.txt ./
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements-dev.txt

COPY docker/common/fetch_spark_jars.sh /usr/local/bin/fetch_spark_jars.sh
RUN chmod +x /usr/local/bin/fetch_spark_jars.sh && fetch_spark_jars.sh

# Non-root runtime user.
RUN useradd --create-home --uid 1000 platform \
 && mkdir -p /opt/platform /tmp/spark-events \
 && chown -R platform:platform /opt/platform /tmp/spark-events

COPY --chown=platform:platform . /opt/platform

USER platform
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-c", "print('platform image ready')"]
