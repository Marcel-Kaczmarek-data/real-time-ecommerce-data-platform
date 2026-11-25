"""Markdown data-quality report for the latest run (data_quality/reports/latest.md)."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from data_quality.validators.engine import CheckResult, quality_score

REPORT_DIR = Path(__file__).resolve().parent


def write_markdown_report(results: list[CheckResult], path: Path | None = None) -> Path:
    path = path or REPORT_DIR / "latest.md"
    by_status = Counter(r.status for r in results)
    header = [
        "# Data quality report",
        "",
        f"- Run id: `{results[0].run_id}`" if results else "- Run id: -",
        f"- Executed at (UTC): {results[0].executed_at:%Y-%m-%d %H:%M:%S}" if results else "- Executed at: -",
        f"- Checks: {len(results)} (pass {by_status['PASS']}, fail {by_status['FAIL']}, error {by_status['ERROR']})",
        f"- Severity-weighted quality score: **{quality_score(results)} / 100**",
        "",
        "| Status | Severity | Suite | Check | Table | Failed | Rate | Threshold | Details |",
        "|---|---|---|---|---|---:|---:|---:|---|",
    ]
    order = {"FAIL": 0, "ERROR": 1, "PASS": 2}
    sev = {"critical": 0, "warning": 1, "informational": 2}
    rows = []
    for r in sorted(results, key=lambda x: (order[x.status], sev[x.severity], x.check_name)):
        icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "⚠️"}[r.status]
        rows.append(
            f"| {icon} {r.status} | {r.severity} | {r.suite} | `{r.check_name}` | `{r.table_name}` | "
            f"{r.failed_records} | {r.failure_rate:.4%} | {r.threshold:.2%} | {r.details.replace('|', '/')} |"
        )
    path.write_text("\n".join(header + rows) + "\n", encoding="utf-8", newline="\n")
    return path
