"""SQL serving layer for Gold: DuckDB over Delta tables read with delta-rs.

* No JVM and no extra service: Gold Delta tables on S3 / SeaweedFS are exposed
  as DuckDB views over Arrow datasets, with filter and projection push-down to Parquet.
* Each query re-checks the Delta version of the tables it references (a cheap
  log read) and re-registers a table when a new version was committed, so
  dashboards always show the latest committed data and never a partial write.
* Thread-safe for Streamlit, which runs sessions in threads.

    wh = GoldWarehouse()
    df = wh.query("SELECT date, net_revenue_usd FROM mart_executive_summary WHERE date >= ?", [start])
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any

import duckdb
import pandas as pd
from deltalake import DeltaTable

from pipelines.common.config import Config, load_config
from pipelines.common.storage import delta_rs_storage_options
from pipelines.common.tables import table

# Table name in SQL -> lakehouse table.
SERVED_TABLES = {
    **{
        name: f"gold.{name}"
        for name in (
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_category",
            "dim_country",
            "dim_sales_channel",
            "dim_warehouse",
            "dim_promotion",
            "fact_orders",
            "fact_order_items",
            "fact_payments",
            "fact_shipments",
            "fact_refunds",
            "fact_returns",
            "fact_inventory_snapshots",
            "mart_daily_sales",
            "mart_daily_category_sales",
            "mart_product_performance",
            "mart_customer_360",
            "mart_customer_retention",
            "mart_customer_lifetime_value",
            "mart_conversion_funnel",
            "mart_inventory_health",
            "mart_order_fulfillment",
            "mart_payment_performance",
            "mart_returns_analysis",
            "mart_marketing_performance",
            "mart_executive_summary",
            "financial_reconciliation",
            "rt_sales_minute",
            "rt_payments_minute",
        )
    },
    "dq_results": "audit.dq_results",
    "gold_watermarks": "audit.gold_watermarks",
    "silver_rejects": "quarantine.silver_rejects",
}
_IDENT = re.compile(r"\b([a-z_][a-z0-9_]*)\b")


class TableNotAvailableError(RuntimeError):
    pass


class GoldWarehouse:
    def __init__(self, cfg: Config | None = None) -> None:
        self.cfg = cfg or load_config()
        self.storage_options = delta_rs_storage_options(self.cfg)
        self.con = duckdb.connect(database=":memory:")
        self.con.execute("SET TimeZone = 'UTC'")
        self._versions: dict[str, int] = {}
        self._lock = threading.Lock()

    def _uri(self, name: str) -> str:
        path = table(SERVED_TABLES[name], self.cfg).path
        return "s3://" + path[len("s3a://") :] if path.startswith("s3a://") else path.replace("file://", "")

    def _ensure(self, name: str) -> None:
        try:
            dt = DeltaTable(self._uri(name), storage_options=self.storage_options)
        except Exception as exc:  # table not built yet
            raise TableNotAvailableError(
                f"{SERVED_TABLES[name]} is not available yet ({type(exc).__name__})"
            ) from exc
        version = dt.version()
        if self._versions.get(name) != version:
            self.con.register(name, dt.to_pyarrow_dataset())
            self._versions[name] = version

    def referenced_tables(self, sql: str) -> list[str]:
        return sorted({tok for tok in _IDENT.findall(sql.lower()) if tok in SERVED_TABLES})

    def query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        with self._lock:
            for name in self.referenced_tables(sql):
                self._ensure(name)
            t0 = time.perf_counter()
            df = self.con.execute(sql, params or []).df()
            self.last_query_ms = (time.perf_counter() - t0) * 1000
            return df

    def versions(self) -> dict[str, int]:
        return dict(self._versions)
