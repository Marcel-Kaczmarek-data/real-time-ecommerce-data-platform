"""Declarative data-quality engine (PySpark).

Rules live in ``data_quality/rules/<suite>.yaml``. Each rule is validated with
pydantic, evaluated against a Delta table, and produces one result row:

    status = PASS | FAIL | ERROR
    failure_rate = failed_records / total_records   (compared with max_failure_rate)

Results are appended to ``audit.dq_results`` (see ``persist_results``).
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, model_validator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.common.logs import get_logger
from pipelines.common.tables import read_table, table, table_exists

log = get_logger("data-quality")
RULES_DIR = Path(__file__).resolve().parents[1] / "rules"
SEVERITY_WEIGHT = {"critical": 3, "warning": 2, "informational": 1}
CheckType = Literal[
    "unique",
    "not_null",
    "accepted_values",
    "range",
    "expression",
    "referential",
    "freshness",
    "row_count",
    "sql",
    "duplicate_rate",
]


class Rule(BaseModel):
    name: str
    table: str
    type: CheckType
    severity: Literal["critical", "warning", "informational"]
    columns: list[str] = Field(default_factory=list)
    column: str | None = None
    values: list[Any] = Field(default_factory=list)
    min: float | None = None
    max: float | None = None
    expression: str | None = None
    sql: str | None = None
    ref_table: str | None = None
    ref_column: str | None = None
    max_age_hours: float | None = None
    min_rows: int | None = None
    max_rows: int | None = None
    max_change_pct: float | None = None
    max_failure_rate: float = 0.0
    description: str | None = None

    @model_validator(mode="after")
    def _required_fields(self) -> Rule:
        required = {
            "unique": ["columns"],
            "not_null": ["columns"],
            "duplicate_rate": ["columns"],
            "accepted_values": ["column", "values"],
            "range": ["column"],
            "expression": ["expression"],
            "referential": ["column", "ref_table", "ref_column"],
            "freshness": ["column", "max_age_hours"],
            "sql": ["sql"],
            "row_count": [],
        }[self.type]
        missing = [f for f in required if not getattr(self, f)]
        if missing:
            raise ValueError(f"rule {self.name}: type '{self.type}' requires {missing}")
        return self


@dataclass
class CheckResult:
    run_id: str
    suite: str
    check_name: str
    table_name: str
    check_type: str
    severity: str
    status: str
    total_records: int
    failed_records: int
    failure_rate: float
    threshold: float
    observed_value: float | None
    details: str
    duration_ms: int
    executed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


def load_suite(suite: str) -> tuple[str, list[Rule]]:
    raw = yaml.safe_load((RULES_DIR / f"{suite}.yaml").read_text(encoding="utf-8"))
    return raw.get("owner", "unknown"), [Rule(**r) for r in raw["checks"]]


_PLACEHOLDER = re.compile(r"\{([a-z_]+\.[a-z0-9_]+)\}")


def render_sql(spark: SparkSession, sql: str) -> str:
    """Replace ``{layer.table}`` placeholders with registered temp views."""

    def repl(m: re.Match[str]) -> str:
        fqn = m.group(1)
        view = fqn.replace(".", "__")
        read_table(spark, fqn).createOrReplaceTempView(view)
        return view

    return _PLACEHOLDER.sub(repl, sql)


def _previous_value(spark: SparkSession, check_name: str) -> float | None:
    ref = table("audit.dq_results")
    if not table_exists(spark, ref):
        return None
    row = (
        read_table(spark, ref)
        .filter((F.col("check_name") == check_name) & F.col("observed_value").isNotNull())
        .orderBy(F.col("executed_at").desc())
        .select("observed_value")
        .first()
    )
    return float(row[0]) if row else None


def _freshness(df: Any, rule: Rule) -> tuple[int, int, float | None, str, str | None]:
    newest = df.agg(F.max(rule.column)).first()[0]
    if newest is None:
        return 0, 0, None, "table is empty", "FAIL"
    if isinstance(newest, date) and not isinstance(newest, datetime):
        newest = datetime(newest.year, newest.month, newest.day)
    newest_utc = newest if newest.tzinfo else newest.replace(tzinfo=UTC)
    age = (datetime.now(UTC) - newest_utc).total_seconds() / 3600
    failed = 1 if age > (rule.max_age_hours or 0) else 0
    return (
        1,
        failed,
        age,
        f"newest {rule.column} = {newest_utc:%Y-%m-%d %H:%M} ({age:.1f} h old, max {rule.max_age_hours} h)",
        None,
    )


def evaluate(spark: SparkSession, rule: Rule, run_id: str, suite: str) -> CheckResult:
    t0 = time.time()
    observed: float | None = None
    status_override: str | None = None
    try:
        if not table_exists(spark, table(rule.table)):
            raise FileNotFoundError(f"table {rule.table} does not exist")
        df = read_table(spark, rule.table)
        total = df.count()
        failed = 0
        if rule.type in ("unique", "duplicate_rate"):
            failed = total - df.select(*rule.columns).distinct().count()
            details = (
                f"{failed} duplicate key rows on {rule.columns}"
                if rule.type == "unique"
                else f"{failed} duplicate events (Bronze keeps them; Silver deduplicates)"
            )
        elif rule.type == "not_null":
            failed = df.filter(" OR ".join(f"`{c}` IS NULL" for c in rule.columns)).count()
            details = f"rows with NULL in any of {rule.columns}"
        elif rule.type == "accepted_values":
            failed = df.filter(~F.col(rule.column).isin(rule.values) | F.col(rule.column).isNull()).count()
            sample = [
                r[0]
                for r in df.filter(~F.col(rule.column).isin(rule.values))
                .select(rule.column)
                .distinct()
                .limit(5)
                .collect()
            ]
            details = f"unexpected values (sample): {sample}"
        elif rule.type == "range":
            col = F.col(rule.column)
            bad = col.isNull()
            if rule.min is not None:
                bad = bad | (col < rule.min)
            if rule.max is not None:
                bad = bad | (col > rule.max)
            failed = df.filter(bad).count()
            stats = df.agg(F.min(col), F.max(col)).first()
            details = f"observed min={stats[0]} max={stats[1]}, allowed [{rule.min}, {rule.max}]"
        elif rule.type == "expression":
            failed = df.filter(f"NOT coalesce(({rule.expression}), false)").count()
            details = f"rows violating: {rule.expression}"
        elif rule.type == "referential":
            ref = (
                read_table(spark, str(rule.ref_table)).select(F.col(rule.ref_column).alias("_ref")).distinct()
            )
            failed = (
                df.select(rule.column)
                .filter(F.col(rule.column).isNotNull())
                .join(ref, F.col(rule.column) == F.col("_ref"), "left_anti")
                .count()
            )
            details = f"{rule.table}.{rule.column} values missing in {rule.ref_table}.{rule.ref_column}"
        elif rule.type == "freshness":
            total, failed, observed, details, status_override = _freshness(df, rule)
        elif rule.type == "row_count":
            observed = float(total)
            previous = _previous_value(spark, rule.name)
            problems = []
            if rule.min_rows is not None and total < rule.min_rows:
                problems.append(f"below minimum {rule.min_rows}")
            if rule.max_rows is not None and total > rule.max_rows:
                problems.append(f"above maximum {rule.max_rows}")
            if rule.max_change_pct is not None and previous:
                change = abs(total - previous) / previous * 100
                if change > rule.max_change_pct:
                    problems.append(f"changed {change:.1f}% vs previous run ({previous:.0f})")
            failed = 1 if problems else 0
            details = f"{total} rows; " + ("; ".join(problems) if problems else "within expected bounds")
            total = 1
        else:  # sql
            row = spark.sql(render_sql(spark, rule.sql or "")).first()
            failed = int(row["failed"] or 0) if row else 0
            details = f"{failed} failing records from custom SQL"
        rate = failed / total if total else 0.0
        status = status_override or ("PASS" if (failed == 0 or rate <= rule.max_failure_rate) else "FAIL")
        result = CheckResult(
            run_id,
            suite,
            rule.name,
            rule.table,
            rule.type,
            rule.severity,
            status,
            total,
            failed,
            round(rate, 6),
            rule.max_failure_rate,
            observed if observed is not None else float(failed),
            details,
            int((time.time() - t0) * 1000),
        )
    except Exception as exc:  # a broken check is itself a finding
        result = CheckResult(
            run_id,
            suite,
            rule.name,
            rule.table,
            rule.type,
            rule.severity,
            "ERROR",
            0,
            0,
            0.0,
            rule.max_failure_rate,
            None,
            f"{type(exc).__name__}: {str(exc)[:500]}",
            int((time.time() - t0) * 1000),
        )
    log.info(
        "dq check",
        extra={
            "check": rule.name,
            "status": result.status,
            "severity": rule.severity,
            "failed": result.failed_records,
            "rate": result.failure_rate,
        },
    )
    return result


def run_suite(
    spark: SparkSession, suite: str, severities: set[str] | None = None, run_id: str | None = None
) -> list[CheckResult]:
    _, rules = load_suite(suite)
    run_id = run_id or f"dq-{suite}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    return [
        evaluate(spark, r, run_id, suite) for r in rules if severities is None or r.severity in severities
    ]


def quality_score(results: list[CheckResult]) -> float:
    """Severity-weighted share of passing checks (0-100)."""
    total = sum(SEVERITY_WEIGHT[r.severity] for r in results)
    ok = sum(SEVERITY_WEIGHT[r.severity] for r in results if r.passed)
    return round(100 * ok / total, 2) if total else 100.0


RESULT_SCHEMA = (
    "run_id STRING, suite STRING, check_name STRING, table_name STRING, check_type STRING, severity STRING, "
    "status STRING, total_records BIGINT, failed_records BIGINT, failure_rate DOUBLE, threshold DOUBLE, "
    "observed_value DOUBLE, details STRING, duration_ms BIGINT, executed_at TIMESTAMP"
)


def persist_results(spark: SparkSession, results: list[CheckResult]) -> None:
    if not results:
        return
    rows = [tuple(asdict(r).values()) for r in results]
    df = spark.createDataFrame(rows, RESULT_SCHEMA).withColumn("execution_date", F.to_date("executed_at"))
    ref = table("audit.dq_results")
    writer = df.write.format("delta").mode("append").partitionBy("execution_date")
    writer.saveAsTable(ref.qualified_name) if ref.uses_catalog else writer.save(ref.path)
