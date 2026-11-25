"""Infrastructure smoke test: Spark + Delta Lake on MinIO, read back via delta-rs.

Run inside the platform container:
    docker compose run --rm platform python scripts/smoke_lakehouse.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from pipelines.common.logs import get_logger
from pipelines.common.spark import get_spark
from pipelines.common.storage import delta_rs_storage_options, to_s3_uri
from pipelines.common.tables import table

log = get_logger("smoke")


def main() -> int:
    spark = get_spark("smoke-lakehouse")
    ref = table("audit.infrastructure_smoke")
    df = spark.createDataFrame(
        [(1, "spark-delta-minio", datetime.now(UTC))], "id INT, check_name STRING, checked_at TIMESTAMP"
    )
    df.write.format("delta").mode("overwrite").save(ref.path)
    spark_rows = spark.read.format("delta").load(ref.path).count()
    log.info("spark wrote and read delta table", extra={"path": ref.path, "rows": spark_rows})
    spark.stop()

    from deltalake import DeltaTable

    dt = DeltaTable(to_s3_uri(ref.path), storage_options=delta_rs_storage_options())
    rs_rows = dt.to_pyarrow_table().num_rows
    log.info("delta-rs read table", extra={"version": dt.version(), "rows": rs_rows})
    ok = spark_rows == rs_rows == 1
    print("SMOKE TEST", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
