"""Data Quality & Pipeline Health: quality score, failed rules, quarantine, freshness, load watermarks."""

from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from lib import theme
from lib.components import (
    bar_chart,
    chart,
    empty_state,
    export_button,
    fmt_int,
    fmt_pct,
    kpi_card,
    kpi_row,
    line_chart,
    page_header,
    section,
    status_badge,
)
from lib.data import Filters, safe_query

WEIGHT = {"critical": 3, "warning": 2, "informational": 1}


def render(f: Filters) -> None:
    page_header(
        "Data Quality & Pipeline Health",
        "Results of the declarative rule suites (audit.dq_results), "
        "quarantined records and Gold load watermarks.",
    )
    # Current status of every rule = its most recent result (gate runs only re-check critical rules).
    latest = safe_query("""SELECT * FROM dq_results
                           QUALIFY row_number() OVER (PARTITION BY check_name ORDER BY executed_at DESC) = 1""")
    if latest is None or latest.empty:
        empty_state("No data-quality runs yet. Run `make quality`.")
        return
    weights = latest["severity"].map(WEIGHT)
    score = 100 * (weights * (latest["status"] == "PASS")).sum() / weights.sum()
    failed = latest[latest["status"] != "PASS"]
    crit = int(((latest["severity"] == "critical") & (latest["status"] != "PASS")).sum())
    processed = latest.loc[latest["check_type"].isin(["unique", "not_null"]), ["table_name", "total_records"]]
    total_records = int(processed.groupby("table_name")["total_records"].max().sum())
    dup = latest[latest["check_type"] == "duplicate_rate"]
    dup_rate = dup["failed_records"].sum() / dup["total_records"].sum() if not dup.empty else None
    rejects = safe_query(
        "SELECT entity, source, count(*) n FROM silver_rejects GROUP BY 1, 2 ORDER BY n DESC"
    )
    quarantined = 0 if rejects is None else int(rejects["n"].sum())
    kpi_row(
        [
            kpi_card("Quality score", f"{score:.1f} / 100", hint="severity-weighted pass rate"),
            kpi_card(
                "Active rules",
                fmt_int(len(latest)),
                hint=f"last check {latest['executed_at'].max():%d %b %H:%M} UTC",
            ),
            kpi_card("Failed rules", fmt_int(len(failed)), hint=f"{crit} critical (blocks Gold)"),
            kpi_card("Records validated", fmt_int(total_records), hint="rows in checked tables"),
            kpi_card(
                "Valid records", fmt_pct(1 - quarantined / max(total_records, 1), 3), hint="not quarantined"
            ),
            kpi_card(
                "Quarantined records",
                fmt_int(quarantined),
                hint=f"duplicate event rate {fmt_pct(dup_rate, 2)}",
            ),
        ]
    )

    section("Failed and erroring rules")
    if failed.empty:
        st.markdown(status_badge("PASS") + " &nbsp; all checks passed", unsafe_allow_html=True)
    else:
        rows = "".join(
            f"<tr><td>{status_badge(r.status)}</td><td>{html.escape(r.severity)}</td>"
            f"<td><code>{html.escape(r.check_name)}</code></td><td class='muted'>{html.escape(r.table_name)}</td>"
            f"<td style='text-align:right'>{int(r.failed_records):,}</td>"
            f"<td class='muted'>{html.escape(str(r.details)[:110])}</td></tr>"
            for r in failed.sort_values("severity").itertuples()
        )
        st.markdown(
            f"""<table style="width:100%;font-size:0.85rem">
<tr class="muted"><th align="left">Status</th><th align="left">Severity</th><th align="left">Rule</th>
<th align="left">Table</th><th align="right">Failed</th><th align="left">Details</th></tr>{rows}</table>""",
            unsafe_allow_html=True,
        )

    a, b = st.columns([3, 2])
    with a:
        hist = safe_query("""SELECT date_trunc('hour', executed_at) AS run,
                                    100 * sum(CASE severity WHEN 'critical' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END
                                              * CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)
                                        / sum(CASE severity WHEN 'critical' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END) score
                             FROM dq_results GROUP BY 1 ORDER BY 1""")
        if hist is not None and len(hist) > 1:
            chart(
                line_chart(
                    hist,
                    "run",
                    {"score": "Quality score"},
                    "Quality score per run",
                    colors={"score": theme.SERIES[2]},
                    y_format=",.1f",
                ),
                280,
            )
        elif hist is not None:
            st.caption("Quality-score trend appears after the second run.")
    with b:
        if rejects is not None and not rejects.empty:
            rejects["label"] = rejects["entity"] + " · " + rejects["source"]
            chart(
                bar_chart(
                    rejects.head(10),
                    "label",
                    "n",
                    "Quarantined records by entity / source",
                    horizontal=True,
                    value_format=",.0f",
                    color=theme.SERIES[7],
                ),
                280,
            )

    section("Data freshness")
    fresh = latest[latest["check_type"] == "freshness"][
        ["table_name", "observed_value", "threshold", "status", "details"]
    ]
    if not fresh.empty:
        fresh = fresh.rename(columns={"observed_value": "age_hours"}).sort_values(
            "age_hours", ascending=False
        )
        st.dataframe(
            fresh,
            hide_index=True,
            use_container_width=True,
            column_config={"age_hours": st.column_config.NumberColumn("Age (h)", format="%.1f")},
        )

    section("Gold incremental load watermarks")
    wm = safe_query(
        "SELECT fact_name, watermark, rows_last_run, updated_at FROM gold_watermarks ORDER BY fact_name"
    )
    if wm is not None and not wm.empty:
        wm["lag_minutes"] = (
            pd.Timestamp.utcnow().tz_localize(None) - pd.to_datetime(wm["watermark"]).dt.tz_localize(None)
        ).dt.total_seconds() / 60
        st.dataframe(
            wm,
            hide_index=True,
            use_container_width=True,
            column_config={"lag_minutes": st.column_config.NumberColumn("Lag (min)", format="%.0f")},
        )

    with st.expander("All checks of the latest run"):
        cols = [
            "status",
            "severity",
            "suite",
            "check_name",
            "table_name",
            "check_type",
            "failed_records",
            "failure_rate",
            "threshold",
            "details",
        ]
        st.dataframe(
            latest[cols].sort_values(["status", "severity"]), hide_index=True, use_container_width=True
        )
        export_button(latest[cols], "dq_results_latest")
