"""Run data-quality suites, persist results, push metrics and write a report.

python -m data_quality.run                         # bronze + silver + gold
python -m data_quality.run --suite silver --fail-on critical
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from data_quality.reports.report import write_markdown_report
from data_quality.validators.engine import persist_results, quality_score, run_suite
from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark

log = get_logger("dq-run")
SUITES = ["bronze", "silver", "gold"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--suite", action="append", choices=SUITES)
    parser.add_argument("--fail-on", choices=["critical", "warning", "never"], default="critical")
    args = parser.parse_args(argv)
    spark = get_spark("data-quality")
    run_id = f"dq-{datetime.now(UTC):%Y%m%dT%H%M%S}"
    all_results = []
    with log_context("data-quality", run_id), track_job("data_quality_validation") as metrics:
        for suite in args.suite or SUITES:
            results = run_suite(spark, suite, run_id=run_id)
            persist_results(spark, results)
            all_results.extend(results)
            metrics.gauge(
                "ecom_dq_quality_score",
                "Severity-weighted share of passing checks",
                {"suite": suite},
                quality_score(results),
            )
            for sev in ("critical", "warning", "informational"):
                failed = sum(1 for r in results if r.severity == sev and not r.passed)
                metrics.gauge(
                    "ecom_dq_failed_checks",
                    "Failed checks by severity",
                    {"suite": suite, "severity": sev},
                    failed,
                )
            for r in results:
                metrics.gauge(
                    "ecom_dq_check_failed_records",
                    "Failing records per check",
                    {"check": r.check_name, "severity": r.severity},
                    r.failed_records,
                )
                if r.check_type == "freshness" and r.observed_value is not None:
                    metrics.gauge(
                        "ecom_dq_data_age_hours",
                        "Age of newest record",
                        {"table": r.table_name},
                        r.observed_value,
                    )
        path = write_markdown_report(all_results)
    for r in all_results:
        mark = {"PASS": "ok  ", "FAIL": "FAIL", "ERROR": "ERR "}[r.status]
        print(f"[{mark}] {r.severity:13s} {r.check_name:45s} failed={r.failed_records:<6} {r.details[:80]}")
    print(f"quality score: {quality_score(all_results)} - report: {path}")
    spark.stop()
    severities = {"critical": {"critical"}, "warning": {"critical", "warning"}, "never": set()}[args.fail_on]
    return 1 if any(r.severity in severities and not r.passed for r in all_results) else 0


if __name__ == "__main__":
    sys.exit(main())
