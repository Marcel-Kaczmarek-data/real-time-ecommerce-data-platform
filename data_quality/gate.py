"""Promotion gate: critical Silver checks must pass before Gold is built.

Gold jobs call :func:`assert_promotable` first. It runs the *critical* checks of
the Silver suite, persists the results (audit trail), and raises
:class:`DataQualityGateError` if any of them fails or errors. The affected
data stays in Silver until fixed, and the previous Gold state remains
queryable: consumers see slightly stale but correct numbers instead of
wrong ones.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from data_quality.validators.engine import persist_results, run_suite
from pipelines.common.logs import get_logger

log = get_logger("dq-gate")


class DataQualityGateError(RuntimeError):
    pass


def assert_promotable(spark: SparkSession, stage: str = "gold", suite: str = "silver") -> None:
    results = run_suite(spark, suite, severities={"critical"})
    persist_results(spark, results)
    blocking = [r for r in results if r.status != "PASS"]
    if blocking:
        summary = "; ".join(f"{r.check_name} ({r.status}: {r.details})" for r in blocking)
        log.error("promotion blocked", extra={"stage": stage, "failed_checks": len(blocking)})
        raise DataQualityGateError(
            f"{len(blocking)} critical data-quality check(s) failed - {stage} promotion blocked: {summary}"
        )
    log.info("promotion gate passed", extra={"stage": stage, "critical_checks": len(results)})
