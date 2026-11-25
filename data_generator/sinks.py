"""Output sinks for generated data: legacy history exports, partner files,
the operational PostgreSQL database and the simulation anchor."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from data_generator.events import Event
from data_generator.world import World
from pipelines.common.logs import get_logger

log = get_logger(__name__)

STATUS_RANK = {"created": 0, "confirmed": 1, "cancelled": 2, "shipped": 3, "delivered": 4, "returned": 5}


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(type(value))


def to_json(obj: Any) -> str:
    return json.dumps(obj, default=_json_default, separators=(",", ":"))


# ---------------------------------------------------------------- anchor ---
@dataclass(frozen=True)
class SimulationAnchor:
    """Fixes the simulated timeline so every component reproduces the same events."""

    seed: int
    scale: str
    history_start: datetime
    go_live: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "scale": self.scale,
            "history_start": self.history_start.isoformat(),
            "go_live": self.go_live.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SimulationAnchor:
        return cls(
            seed=int(d["seed"]),
            scale=str(d["scale"]),
            history_start=datetime.fromisoformat(d["history_start"]),
            go_live=datetime.fromisoformat(d["go_live"]),
        )


# ------------------------------------------------------------- landing -----
class Landing:
    """Writes files to the landing bucket (S3 API) or to a local directory."""

    def __init__(self, local_dir: Path | None = None, bucket: str | None = None) -> None:
        self.local_dir = local_dir
        self.bucket = bucket
        self._s3 = None
        if local_dir is None:
            from pipelines.common.config import load_config
            from pipelines.common.storage import s3_client

            cfg = load_config()
            self.bucket = bucket or str(cfg.get_path("storage.landing_bucket"))
            self._s3 = s3_client(cfg)

    def put(self, key: str, data: bytes) -> str:
        if self._s3 is not None:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)
            return f"s3://{self.bucket}/{key}"
        if self.local_dir is None:
            raise RuntimeError("Landing has neither S3 client nor local directory")
        path = self.local_dir / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)

    def get(self, key: str) -> bytes | None:
        if self._s3 is not None:
            try:
                return bytes(self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
            except self._s3.exceptions.NoSuchKey:
                return None
        if self.local_dir is None:
            raise RuntimeError("Landing has neither S3 client nor local directory")
        path = self.local_dir / key
        return path.read_bytes() if path.exists() else None


def csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {k: (v.isoformat() if isinstance(v, datetime | date) else v) for k, v in row.items()}
            )
    return buf.getvalue().encode()


def parquet_bytes(rows: list[dict[str, Any]], schema: Any = None) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(rows, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue()


# ------------------------------------------------------ history exports ---
@dataclass
class HistoryCollector:
    """Folds pre-go-live order events into the legacy order system's final-state export."""

    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    items: dict[str, dict[str, Any]] = field(default_factory=dict)
    payments: dict[str, dict[str, Any]] = field(default_factory=dict)

    def add(self, event: Event) -> None:
        p = event.payload
        if event.entity == "orders" and p.get("order_id"):
            current = self.orders.get(p["order_id"])
            rank = STATUS_RANK.get(str(p.get("order_status")), -1)
            if current is None or rank >= STATUS_RANK.get(str(current.get("order_status")), -1):
                row = {
                    k: p.get(k)
                    for k in (
                        "order_id",
                        "customer_id",
                        "order_timestamp",
                        "order_status",
                        "sales_channel",
                        "currency",
                        "subtotal_amount",
                        "total_amount",
                        "shipping_amount",
                        "discount_amount",
                        "tax_amount",
                        "promotion_id",
                        "country",
                        "item_count",
                        "status_reason",
                    )
                }
                row["last_status_at"] = event.event_timestamp
                self.orders[p["order_id"]] = row
        elif event.entity == "order_items" and p.get("order_item_id"):
            self.items[p["order_item_id"]] = {
                k: p.get(k)
                for k in (
                    "order_item_id",
                    "order_id",
                    "product_id",
                    "quantity",
                    "unit_price",
                    "discount_amount",
                    "currency",
                )
            }
        elif event.entity == "payments" and p.get("payment_id"):
            current = self.payments.get(p["payment_id"])
            if current is None or p.get("payment_status") in {"captured", "failed"}:
                self.payments[p["payment_id"]] = {
                    k: p.get(k)
                    for k in (
                        "payment_id",
                        "order_id",
                        "payment_method",
                        "payment_status",
                        "payment_amount",
                        "currency",
                        "payment_timestamp",
                        "attempt_number",
                        "failure_reason",
                    )
                }

    def write(self, landing: Landing, export_date: date) -> list[str]:
        """Write monthly files: Parquet for orders/payments, CSV for items (mixed formats on purpose)."""
        written: list[str] = []

        def month_of(row: dict[str, Any], ts_key: str) -> str:
            ts = row.get(ts_key)
            return ts.strftime("%Y-%m") if isinstance(ts, datetime) else "unknown"

        order_month = {oid: month_of(o, "order_timestamp") for oid, o in self.orders.items()}
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for o in self.orders.values():
            groups.setdefault(("orders", order_month[o["order_id"]]), []).append(o)
        for it in self.items.values():
            groups.setdefault(
                ("order_items", order_month.get(str(it.get("order_id")), "unknown")), []
            ).append(it)
        for pay in self.payments.values():
            groups.setdefault(("payments", month_of(pay, "payment_timestamp")), []).append(pay)
        import pyarrow as pa

        ts = pa.timestamp("us", tz="UTC")
        schemas = {
            "orders": pa.schema(
                [
                    ("order_id", pa.string()),
                    ("customer_id", pa.string()),
                    ("order_timestamp", ts),
                    ("order_status", pa.string()),
                    ("sales_channel", pa.string()),
                    ("currency", pa.string()),
                    ("subtotal_amount", pa.float64()),
                    ("total_amount", pa.float64()),
                    ("shipping_amount", pa.float64()),
                    ("discount_amount", pa.float64()),
                    ("tax_amount", pa.float64()),
                    ("promotion_id", pa.string()),
                    ("country", pa.string()),
                    ("item_count", pa.int32()),
                    ("status_reason", pa.string()),
                    ("last_status_at", ts),
                ]
            ),
            "payments": pa.schema(
                [
                    ("payment_id", pa.string()),
                    ("order_id", pa.string()),
                    ("payment_method", pa.string()),
                    ("payment_status", pa.string()),
                    ("payment_amount", pa.float64()),
                    ("currency", pa.string()),
                    ("payment_timestamp", ts),
                    ("attempt_number", pa.int32()),
                    ("failure_reason", pa.string()),
                ]
            ),
        }
        for (entity, month), rows in sorted(groups.items()):
            if entity == "order_items":
                key = f"historical_orders/{entity}/month={month}/{entity}_{month}_{export_date:%Y%m%d}.csv"
                written.append(landing.put(key, csv_bytes(rows)))
            else:
                key = (
                    f"historical_orders/{entity}/month={month}/{entity}_{month}_{export_date:%Y%m%d}.parquet"
                )
                written.append(landing.put(key, parquet_bytes(rows, schemas.get(entity))))
        log.info(
            "history export written",
            extra={
                "files": len(written),
                "orders": len(self.orders),
                "items": len(self.items),
                "payments": len(self.payments),
            },
        )
        return written


# ------------------------------------------------------- partner files ----
def write_partner_files(world: World, landing: Landing, export_date: date) -> list[str]:
    """Product catalogue feed (CSV) and supplier master (JSON lines) from the PIM / procurement systems."""
    stamp = f"{export_date:%Y%m%d}"
    catalog = [
        {
            **p.record(),
            "category_name": world.category_by_id[p.category_id].category_name,
            "department": p.department,
        }
        for p in world.products
    ]
    suppliers = [s.record() for s in world.suppliers]
    return [
        landing.put(
            f"product_catalog/dt={export_date.isoformat()}/product_catalog_{stamp}.csv", csv_bytes(catalog)
        ),
        landing.put(
            f"suppliers/dt={export_date.isoformat()}/suppliers_{stamp}.json",
            "\n".join(to_json(s) for s in suppliers).encode(),
        ),
    ]


# ------------------------------------------------------- operational DB ---
OPS_LOAD_ORDER = (
    "countries",
    "currencies",
    "categories",
    "suppliers",
    "warehouses",
    "customers",
    "products",
    "promotions",
    "inventory",
    "supplier_deliveries",
)


def _copy_rows(cur: Any, table: str, rows: Iterable[dict[str, Any]], columns: list[str]) -> int:
    buf = io.StringIO()
    writer = csv.writer(buf)
    n = 0
    for row in rows:
        writer.writerow(["\\N" if row.get(c) is None else row.get(c) for c in columns])
        n += 1
    buf.seek(0)
    cur.copy_expert(f"COPY ops.{table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    return n


def load_operational_db(
    conn: Any,
    world: World,
    deliveries: list[dict[str, Any]],
    promotions: list[dict[str, Any]],
    as_of: datetime,
) -> dict[str, int]:
    """(Re)load the operational schema. Local dev seed only: truncates ops tables first."""
    data: dict[str, list[dict[str, Any]]] = {
        "countries": world.country_records(),
        "currencies": world.currency_records(),
        "categories": [c.record() for c in world.categories],
        "suppliers": [s.record() for s in world.suppliers],
        "warehouses": [w.record() for w in world.warehouses],
        "customers": [{**c.record(), "country_code": c.country} for c in world.customers],
        "products": [p.record() for p in world.products],
        "promotions": promotions,
        "inventory": world.inventory.records(),
        "supplier_deliveries": deliveries,
    }
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("TRUNCATE " + ", ".join(f"ops.{t}" for t in reversed(OPS_LOAD_ORDER)) + " CASCADE")
        cur.execute("ALTER TABLE ops.customers DISABLE TRIGGER USER")
        cur.execute("ALTER TABLE ops.products DISABLE TRIGGER USER")
        for table in OPS_LOAD_ORDER:
            rows = data[table]
            if not rows:
                counts[table] = 0
                continue
            columns = [c for c in rows[0] if c != "country" or table != "customers"]
            for r in rows:
                if "updated_at" in columns and r.get("updated_at") is None:
                    r["updated_at"] = as_of
            counts[table] = _copy_rows(cur, table, rows, columns)
        cur.execute("ALTER TABLE ops.customers ENABLE TRIGGER USER")
        cur.execute("ALTER TABLE ops.products ENABLE TRIGGER USER")
        cur.execute("TRUNCATE meta.ingestion_watermarks, meta.file_registry")
    conn.commit()
    return counts


def upsert_customers(conn: Any, customers: list[dict[str, Any]]) -> int:
    """Operational write-back used by the live producer (registrations / profile changes)."""
    if not customers:
        return 0
    from psycopg2.extras import execute_values

    cols = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country_code",
        "registration_date",
        "customer_segment",
        "loyalty_status",
        "marketing_opt_in",
    ]
    # updated_at is NOT taken from the simulation: the operational DB stamps the
    # real write time (DEFAULT now() / trigger), which is what makes watermark
    # extraction safe for events that are simulated "in the past" during catch-up.
    rows = [tuple(c["country"] if col == "country_code" else c[col] for col in cols) for c in customers]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""INSERT INTO ops.customers ({", ".join(cols)}) VALUES %s
                ON CONFLICT (customer_id) DO UPDATE SET
                  first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
                  email = EXCLUDED.email, country_code = EXCLUDED.country_code,
                  customer_segment = EXCLUDED.customer_segment, loyalty_status = EXCLUDED.loyalty_status,
                  marketing_opt_in = EXCLUDED.marketing_opt_in""",
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_supplier_deliveries(conn: Any, deliveries: list[dict[str, Any]]) -> int:
    if not deliveries:
        return 0
    from psycopg2.extras import execute_values

    cols = [
        "delivery_id",
        "supplier_id",
        "warehouse_id",
        "product_id",
        "quantity",
        "ordered_at",
        "expected_at",
        "delivered_at",
        "status",
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""INSERT INTO ops.supplier_deliveries ({", ".join(cols)}) VALUES %s
                ON CONFLICT (delivery_id) DO UPDATE
                SET delivered_at = EXCLUDED.delivered_at, status = EXCLUDED.status""",
            [tuple(d[c] for c in cols) for d in deliveries],
        )
    conn.commit()
    return len(deliveries)
