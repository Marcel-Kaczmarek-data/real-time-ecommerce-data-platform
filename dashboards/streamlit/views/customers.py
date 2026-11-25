"""Customer Analytics: new vs returning, segments, cohort retention, RFM, CLV."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib import theme
from lib.components import (
    bar_chart,
    chart,
    empty_state,
    fmt_int,
    fmt_money,
    fmt_pct,
    kpi_card,
    kpi_row,
    line_chart,
    page_header,
    section,
)
from lib.data import Filters, safe_query

SEGMENT_ORDER = ["High-value", "Loyal", "Frequent", "New", "Occasional", "At-risk", "Inactive", "Prospect"]


def _seq_scale() -> list[list[object]]:
    return [[i / (len(theme.SEQUENTIAL) - 1), c] for i, c in enumerate(theme.SEQUENTIAL)]


def render(f: Filters) -> None:
    page_header(
        "Customer Analytics", "Customers, segments and retention. Segment rules: docs/kpi-definitions.md."
    )
    frag, p = f.in_list("country_code", f.countries)
    base = safe_query(
        f"""SELECT count(*) customers,
                                 count(*) FILTER (WHERE total_completed_orders > 0) buyers,
                                 count(*) FILTER (WHERE total_completed_orders >= 2) repeat_buyers,
                                 avg(historical_clv_usd) FILTER (WHERE total_completed_orders > 0) avg_clv
                          FROM mart_customer_360 WHERE {frag}""",
        p,
    )
    act = safe_query(
        """SELECT sum(new_customers) new_c, sum(returning_customers) ret_c
                        FROM mart_executive_summary WHERE date BETWEEN ? AND ?""",
        (f.start, f.end),
    )
    if base is None or base.empty:
        empty_state()
        return
    b = base.iloc[0]
    a = act.iloc[0] if act is not None and not act.empty else None
    kpi_row(
        [
            kpi_card("Customers", fmt_int(b["customers"]), None, "registered (current profile)"),
            kpi_card(
                "New customers",
                fmt_int(None if a is None else a["new_c"]),
                None,
                "first paid order in period",
            ),
            kpi_card(
                "Returning buyers",
                fmt_int(None if a is None else a["ret_c"]),
                None,
                "customer-days in period",
            ),
            kpi_card(
                "Repeat purchase rate",
                fmt_pct(b["repeat_buyers"] / b["buyers"] if b["buyers"] else None),
                None,
                "2+ completed orders / buyers",
            ),
            kpi_card("Average historical CLV", fmt_money(b["avg_clv"], 2), None, "net revenue per buyer"),
        ]
    )

    trend = safe_query(
        """SELECT date, new_customers, returning_customers FROM mart_executive_summary
                          WHERE date BETWEEN ? AND ? ORDER BY date""",
        (f.start, f.end),
    )
    left, right = st.columns([3, 2])
    with left:
        if trend is not None and not trend.empty:
            chart(
                line_chart(
                    trend,
                    "date",
                    {"new_customers": "New", "returning_customers": "Returning"},
                    "New vs returning purchasing customers per day",
                    y_format=",.0f",
                ),
                330,
            )
    with right:
        seg = safe_query(
            f"""SELECT customer_segment, count(*) customers FROM mart_customer_360
                             WHERE {frag} GROUP BY 1""",
            p,
        )
        if seg is not None and not seg.empty:
            seg["order"] = seg["customer_segment"].map({s: i for i, s in enumerate(SEGMENT_ORDER)})
            chart(
                bar_chart(
                    seg.sort_values("order"),
                    "customer_segment",
                    "customers",
                    "Customers by segment",
                    horizontal=True,
                    value_format=",.0f",
                ),
                330,
            )

    section("Cohort retention: share of each acquisition cohort purchasing N months after its first order")
    coh = safe_query("""SELECT cohort_month, months_since_first, retention_rate, cohort_customers
                        FROM mart_customer_retention ORDER BY cohort_month, months_since_first""")
    if coh is not None and not coh.empty:
        mat = coh.pivot_table(index="cohort_month", columns="months_since_first", values="retention_rate")
        sizes = coh.groupby("cohort_month")["cohort_customers"].first()
        labels = [f"{d:%b %Y} (n={sizes[d]:,})" for d in mat.index]
        later = mat.drop(columns=[0], errors="ignore")
        zmax = float(later.max().max()) if not later.empty and later.notna().any().any() else 1.0
        fig = go.Figure(
            go.Heatmap(
                z=mat.values,
                x=[f"M+{c}" for c in mat.columns],
                y=labels,
                colorscale=_seq_scale(),
                zmin=0,
                zmax=zmax,
                text=[["" if pd.isna(v) else f"{v:.0%}" for v in row] for row in mat.values],
                texttemplate="%{text}",
                hovertemplate="%{y} · %{x}: %{z:.1%}<extra></extra>",
                xgap=2,
                ygap=2,
                colorbar={"tickformat": ".0%", "len": 0.8},
            )
        )
        fig.update_layout(title="Monthly acquisition cohorts (M+0 = 100% by definition)", hovermode="closest")
        fig.update_yaxes(autorange="reversed")
        chart(fig, 340)

    section("RFM and lifetime value")
    c1, c2 = st.columns(2)
    with c1:
        rfm = safe_query(
            f"""SELECT r_score, f_score, avg(total_net_spend_usd) spend FROM mart_customer_360
                             WHERE r_score IS NOT NULL AND {frag} GROUP BY 1, 2""",
            p,
        )
        if rfm is not None and not rfm.empty:
            mat = rfm.pivot_table(index="f_score", columns="r_score", values="spend")
            fig = go.Figure(
                go.Heatmap(
                    z=mat.values,
                    x=[f"R{c}" for c in mat.columns],
                    y=[f"F{i}" for i in mat.index],
                    colorscale=_seq_scale(),
                    texttemplate="$%{z:,.0f}",
                    xgap=2,
                    ygap=2,
                    hovertemplate="%{y} %{x}: avg spend $%{z:,.0f}<extra></extra>",
                    colorbar={"tickprefix": "$"},
                )
            )
            fig.update_layout(
                title="Average net spend by recency (R) and frequency (F) score", hovermode="closest"
            )
            chart(fig, 330)
    with c2:
        tiers = safe_query("""SELECT clv_tier, count(*) customers, sum(historical_clv_usd) clv
                              FROM mart_customer_lifetime_value GROUP BY 1 ORDER BY 1""")
        if tiers is not None and not tiers.empty:
            chart(
                bar_chart(
                    tiers,
                    "clv_tier",
                    "clv",
                    "Historical CLV by tier (total net revenue)",
                    color=theme.SERIES[6],
                ),
                330,
            )
