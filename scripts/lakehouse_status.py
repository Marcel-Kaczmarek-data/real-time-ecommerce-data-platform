"""Print every Delta table in the lakehouse with its version and row count (delta-rs, no Spark).

python scripts/lakehouse_status.py [--layer silver]
"""

from __future__ import annotations

import argparse
import sys

from deltalake import DeltaTable

from pipelines.common.config import load_config
from pipelines.common.storage import delta_rs_storage_options, s3_client
from pipelines.common.tables import LAYERS


def list_tables(layer: str) -> list[str]:
    cfg = load_config()
    bucket = str(cfg.get_path("storage.lakehouse_bucket"))
    s3 = s3_client(cfg)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{layer}/", Delimiter="/")
    return sorted(p["Prefix"].rstrip("/").split("/")[-1] for p in resp.get("CommonPrefixes", []))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=LAYERS, action="append")
    args = parser.parse_args(argv)
    cfg = load_config()
    bucket = str(cfg.get_path("storage.lakehouse_bucket"))
    opts = delta_rs_storage_options(cfg)
    for layer in args.layer or LAYERS:
        for name in list_tables(layer):
            try:
                dt = DeltaTable(f"s3://{bucket}/{layer}/{name}", storage_options=opts)
                rows = sum(int(a["num_records"]) for a in dt.get_add_actions(flatten=True).to_pylist())
                print(f"{layer + '.' + name:45s} version={dt.version():<5} rows={rows:>10,}")
            except Exception as exc:  # not a Delta table
                print(f"{layer + '.' + name:45s} (not a delta table: {type(exc).__name__})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
