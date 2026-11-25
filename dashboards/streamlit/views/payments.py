"""Payments & Refunds: success / failure rates, methods, failure reasons, refunds, reconciliation."""

from __future__ import annotations

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
    status_badge,
)
from lib.data import Filters, safe_query


def render(f: Filters) -> None:
    page_header(
        "Payments & Refunds",
        "Attempt-level payment outcomes, refunds booked at the original order FX "
        "rate, and the daily finance reconciliation.",
    )
    frag, p = f.in_list("country_code", f.countries)
    where, params = f"date BETWEEN ? AND ? AND {frag}", (f.start, f.end, *p)
    tot = safe_query(
        f"""SELECT sum(payment_attempts) attempts, sum(successful_payments) ok,
                                sum(failed_payments) ko, sum(captured_amount_usd) captured,
                                sum(refunded_amount_usd) refunded, sum(refunds) refunds,
                                sum(reconciliation_difference_usd) recon_diff
                         FROM mart_payment_performance WHERE {where}""",
        params,
    )
    if tot is None or tot.empty or not tot.loc[0, "attempts"]:
        empty_state()
        return
    t = tot.iloc[0]
    kpi_row(
        [
            kpi_card("Payment attempts", fmt_int(t["attempts"])),
            kpi_card("Success rate", fmt_pct(t["ok"] / (t["ok"] + t["ko"]))),
            kpi_card("Failure rate", fmt_pct(t["ko"] / (t["ok"] + t["ko"]))),
            kpi_card("Captured", fmt_money(t["captured"], 1)),
            kpi_card("Refunded", fmt_money(t["refunded"], 1), hint=f"{int(t['refunds'] or 0):,} refunds"),
            kpi_card(
                "Reconciliation gap", fmt_money(t["recon_diff"], 2), hint="captured minus paid order totals"
            ),
        ]
    )
    a, b = st.columns(2)
    with a:
        daily = safe_query(
            f"""SELECT date, sum(successful_payments) / sum(successful_payments + failed_payments) r
                               FROM mart_payment_performance WHERE {where} GROUP BY 1 ORDER BY 1""",
            params,
        )
        if daily is not None and not daily.empty:
            chart(
                line_chart(
                    daily,
                    "date",
                    {"r": "Success rate"},
                    "Payment success rate per day",
                    colors={"r": theme.SERIES[2]},
                    y_format=".1%",
                ),
                300,
            )
    with b:
        meth = safe_query(
            f"""SELECT payment_method, sum(payment_attempts) attempts,
                                     sum(successful_payments) / sum(successful_payments + failed_payments) success_rate
                              FROM mart_payment_performance WHERE {where} GROUP BY 1 ORDER BY attempts DESC""",
            params,
        )
        if meth is not None and not meth.empty:
            fig = go.Figure(
                go.Bar(
                    x=meth["payment_method"],
                    y=meth["attempts"],
                    marker={"color": theme.SERIES[0], "cornerradius": 4},
                    customdata=meth["success_rate"],
                    hovertemplate="%{x}: %{y:,} attempts, success %{customdata:.1%}<extra></extra>",
                )
            )
            fig.update_layout(
                title="Payment method distribution (attempts)", hovermode="closest", bargap=0.35
            )
            chart(fig, 300)

    c, d = st.columns(2)
    with c:
        reasons = safe_query(
            f"""SELECT failure_reason, count(*) failures FROM fact_payments
                                 WHERE is_failure AND payment_date BETWEEN ? AND ? AND {frag}
                                 GROUP BY 1 ORDER BY 2 DESC""",
            (f.start, f.end, *p),
        )
        if reasons is not None and not reasons.empty:
            chart(
                bar_chart(
                    reasons,
                    "failure_reason",
                    "failures",
                    "Payment failures by reason",
                    horizontal=True,
                    value_format=",.0f",
                    color=theme.SERIES[7],
                ),
                300,
            )
    with d:
        ref = safe_query(
            f"""SELECT refund_date AS date, refund_reason, sum(refund_amount_usd) amount
                             FROM fact_refunds WHERE refund_date BETWEEN ? AND ? AND {frag}
                             GROUP BY 1, 2 ORDER BY 1""",
            (f.start, f.end, *p),
        )
        if ref is not None and not ref.empty:
            pivot = ref.pivot_table(
                index="date", columns="refund_reason", values="amount", fill_value=0
            ).reset_index()
            cols = [col for col in ("product_return", "order_cancelled") if col in pivot.columns]
            chart(
                line_chart(
                    pivot,
                    "date",
                    {col: col.replace("_", " ") for col in cols},
                    "Refunds per day by reason",
                    colors={"product_return": theme.SERIES[0], "order_cancelled": theme.SERIES[1]},
                ),
                300,
            )

    section("Daily financial reconciliation (orders vs captures vs refunds)")
    rec = safe_query(
        """SELECT order_date, paid_orders, order_total_usd, captured_usd, difference_usd,
                               orders_without_single_capture, over_refunded_orders, is_reconciled
                        FROM financial_reconciliation WHERE order_date BETWEEN ? AND ? ORDER BY order_date DESC""",
        (f.start, f.end),
    )
    if rec is not None and not rec.empty:
        ok = int(rec["is_reconciled"].sum())
        status = "PASS" if ok == len(rec) else "FAIL"
        st.markdown(
            f"{status_badge(status)} &nbsp; {ok} of {len(rec)} days reconciled within tolerance",
            unsafe_allow_html=True,
        )
        st.dataframe(
            rec,
            hide_index=True,
            use_container_width=True,
            height=260,
            column_config={
                "order_total_usd": st.column_config.NumberColumn("Paid orders", format="$%.2f"),
                "captured_usd": st.column_config.NumberColumn("Captured", format="$%.2f"),
                "difference_usd": st.column_config.NumberColumn("Difference", format="$%.2f"),
            },
        )
