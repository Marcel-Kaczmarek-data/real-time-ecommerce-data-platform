"""Batch ingestion failure handling: corrupted files, malformed rows, duplicate files."""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def it_env():  # type: ignore[no-untyped-def]
    run = uuid.uuid4().hex[:8]
    os.environ["LAKEHOUSE_ROOT"] = f"s3a://lakehouse/_it/{run}"
    os.environ["CHECKPOINT_ROOT"] = f"file:///tmp/it-ckpt/{run}"
    from pipelines.common.spark import get_spark

    spark = get_spark("it-batch-ingestion")
    yield run, spark
    os.environ.pop("LAKEHOUSE_ROOT", None)
    os.environ.pop("CHECKPOINT_ROOT", None)


def test_corrupt_malformed_and_duplicate_files(it_env) -> None:  # type: ignore[no-untyped-def]
    from ingestion.batch.loader import load_source
    from ingestion.batch.registry import Registry
    from pipelines.common.db import connect
    from pipelines.common.storage import s3_client
    from pipelines.common.tables import read_table

    run, spark = it_env
    s3 = s3_client()
    prefix = f"_it/{run}/feed"
    good_csv = b"product_id,product_name,unit_price\nP1,Lamp,10.5\nP2,Desk,99.0\n"
    malformed_csv = b'product_id,product_name,unit_price\nP3,Chair,12.0\nP4,"unterminated,1,2,3,4\n'
    s3.put_object(Bucket="landing", Key=f"{prefix}/a_good.csv", Body=good_csv)
    s3.put_object(Bucket="landing", Key=f"{prefix}/b_duplicate_of_a.csv", Body=good_csv)
    s3.put_object(Bucket="landing", Key=f"{prefix}/c_malformed.csv", Body=malformed_csv)
    source = {
        "type": "file",
        "format": "csv",
        "landing": prefix,
        "target": "bronze.it_feed_raw",
        "group": "it",
    }
    conn = connect()
    registry = Registry(conn)
    name = f"it_feed_{run}"
    try:
        result = load_source(spark, registry, s3, "landing", name, source)
        assert result.files_loaded == 2  # good + malformed (partially)
        assert result.files_duplicate == 1  # same checksum as a_good.csv
        assert result.rows_quarantined >= 1  # malformed row captured, not dropped
        bronze = read_table(spark, "bronze.it_feed_raw")
        assert {r.product_id for r in bronze.collect()} >= {"P1", "P2", "P3"}
        assert bronze.filter("source_file LIKE '%duplicate%'").count() == 0
        assert "file_checksum" in bronze.columns and "raw_payload" in bronze.columns

        # A corrupted Parquet file is quarantined as a whole and marked FAILED.
        parquet_source = {
            **source,
            "format": "parquet",
            "landing": f"{prefix}_pq",
            "target": "bronze.it_pq_raw",
        }
        s3.put_object(
            Bucket="landing", Key=f"{prefix}_pq/broken.parquet", Body=b"PAR1 this is not parquet PAR1"
        )
        res2 = load_source(spark, registry, s3, "landing", f"{name}_pq", parquet_source)
        assert res2.files_failed == 1 and res2.errors
        moved = s3.list_objects_v2(Bucket="landing", Prefix=f"quarantine/{prefix}_pq/").get("KeyCount", 0)
        assert moved == 1, "corrupted file must be moved to the quarantine prefix"
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM meta.file_registry WHERE source_name = %s", (f"{name}_pq",))
            assert cur.fetchone()[0] == "FAILED"
        # Re-running is idempotent: nothing new to load.
        again = load_source(spark, registry, s3, "landing", name, source)
        assert again.files_loaded == 0 and again.rows_loaded == 0
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM meta.file_registry WHERE source_name LIKE %s", (f"it_feed_{run}%",))
        conn.commit()
        conn.close()
