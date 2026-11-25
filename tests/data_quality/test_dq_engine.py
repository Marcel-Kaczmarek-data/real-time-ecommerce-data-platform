"""Data-quality framework tests: rule validation, every check type, scoring and the promotion gate."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from data_quality.validators.engine import RULES_DIR, CheckResult, Rule, load_suite, quality_score


def test_all_committed_rule_files_are_valid() -> None:
    names = []
    for path in RULES_DIR.glob("*.yaml"):
        _, rules = load_suite(path.stem)
        assert rules, f"{path.name} has no checks"
        names += [r.name for r in rules]
    assert len(names) == len(set(names)), "check names must be unique across suites"
    assert any(r.severity == "critical" for r in load_suite("silver")[1])


def test_rule_requires_type_specific_fields() -> None:
    with pytest.raises(ValidationError):
        Rule(name="x", table="silver.orders", type="referential", severity="warning", column="customer_id")
    with pytest.raises(ValidationError):
        Rule(name="x", table="silver.orders", type="unique", severity="nonsense", columns=["a"])  # type: ignore[arg-type]


def _result(severity: str, status: str) -> CheckResult:
    return CheckResult(
        "r", "s", f"c-{severity}-{status}", "t", "unique", severity, status, 1, 0, 0.0, 0.0, 0.0, "", 1
    )


def test_quality_score_is_severity_weighted() -> None:
    results = [_result("critical", "PASS"), _result("warning", "FAIL"), _result("informational", "PASS")]
    assert quality_score(results) == pytest.approx(100 * (3 + 1) / 6, rel=1e-3)
    assert quality_score([]) == 100.0


@pytest.mark.spark
def test_check_types_on_spark(spark, lakehouse_root) -> None:  # type: ignore[no-untyped-def]
    from data_quality.validators.engine import evaluate
    from pipelines.common.tables import table

    now = datetime.utcnow()
    spark.createDataFrame(
        [
            ("O1", "C1", 10.0, "created", now),
            ("O1", "C2", -5.0, "bogus", now - timedelta(hours=30)),
            ("O3", "C9", 7.0, "shipped", now),
        ],
        "order_id STRING, customer_id STRING, amount DOUBLE, status STRING, ts TIMESTAMP",
    ).write.format("delta").mode("overwrite").save(table("silver.dq_test_orders").path)
    spark.createDataFrame([("C1",), ("C2",)], "customer_id STRING").write.format("delta").mode(
        "overwrite"
    ).save(table("silver.dq_test_customers").path)

    def run(**kw):  # type: ignore[no-untyped-def]
        return evaluate(
            spark, Rule(name=kw.pop("name", "t"), table="silver.dq_test_orders", **kw), "run", "test"
        )

    assert run(type="unique", columns=["order_id"], severity="critical").failed_records == 1
    assert run(type="not_null", columns=["order_id"], severity="critical").status == "PASS"
    r = run(type="accepted_values", column="status", values=["created", "shipped"], severity="critical")
    assert r.status == "FAIL" and "bogus" in r.details
    assert run(type="range", column="amount", min=0, severity="critical").failed_records == 1
    assert (
        run(type="expression", expression="amount >= 0", severity="warning", max_failure_rate=0.5).status
        == "PASS"
    )  # 1/3 failing tolerated
    ref = run(
        type="referential",
        column="customer_id",
        ref_table="silver.dq_test_customers",
        ref_column="customer_id",
        severity="warning",
    )
    assert ref.failed_records == 1  # C9 missing
    sql = run(
        type="sql",
        sql="SELECT count(*) AS failed FROM {silver.dq_test_orders} WHERE amount < 0",
        severity="critical",
    )
    assert sql.failed_records == 1
    fresh = run(type="freshness", column="ts", max_age_hours=1, severity="warning")
    assert fresh.status == "PASS" and fresh.observed_value is not None and fresh.observed_value < 1
    missing = evaluate(
        spark,
        Rule(name="m", table="silver.does_not_exist", type="unique", columns=["a"], severity="critical"),
        "run",
        "test",
    )
    assert missing.status == "ERROR"


@pytest.mark.spark
def test_gate_blocks_on_critical_failure(spark, lakehouse_root, monkeypatch: pytest.MonkeyPatch) -> None:  # type: ignore[no-untyped-def]
    from data_quality import gate
    from data_quality.validators import engine
    from pipelines.common.tables import table

    spark.createDataFrame([("A",), ("A",)], "k STRING").write.format("delta").mode("overwrite").save(
        table("silver.dq_gate_test").path
    )
    critical = [
        Rule(
            name="gate_unique", table="silver.dq_gate_test", type="unique", columns=["k"], severity="critical"
        )
    ]
    monkeypatch.setattr(engine, "load_suite", lambda suite: ("owner", critical))
    with pytest.raises(gate.DataQualityGateError, match="gate_unique"):
        gate.assert_promotable(spark)
    from pipelines.common.tables import read_table

    persisted = read_table(spark, "audit.dq_results").filter("check_name = 'gate_unique'").collect()
    assert persisted and persisted[-1].status == "FAIL"
