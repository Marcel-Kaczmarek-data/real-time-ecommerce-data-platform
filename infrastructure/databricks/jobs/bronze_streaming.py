"""Databricks job: Kafka -> Bronze continuous streaming (Databricks continuous job)."""

import sys

from _bootstrap import bootstrap

bootstrap()

from pipelines.bronze.stream_kafka_to_bronze import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main([]))
