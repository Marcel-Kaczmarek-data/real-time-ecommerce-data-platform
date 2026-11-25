#!/usr/bin/env bash
# Download the JVM dependencies the pipelines need into PySpark's jars folder so
# that no Ivy/Maven resolution happens at runtime (offline, reproducible
# containers). Versions must match pyspark==3.5.4 (Scala 2.12, Hadoop 3.3.4).
set -euo pipefail

JARS_DIR="${1:-$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')}"
MAVEN="https://repo1.maven.org/maven2"

ARTIFACTS=(
  "io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0.jar"
  "io/delta/delta-storage/3.3.0/delta-storage-3.3.0.jar"
  "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar"
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
  "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
)

mkdir -p "${JARS_DIR}"
for artifact in "${ARTIFACTS[@]}"; do
  name="$(basename "${artifact}")"
  if [[ ! -f "${JARS_DIR}/${name}" ]]; then
    echo "Fetching ${name}"
    curl -fsSL --retry 5 --retry-delay 3 -o "${JARS_DIR}/${name}" "${MAVEN}/${artifact}"
  fi
done
echo "Spark jars installed in ${JARS_DIR}"
