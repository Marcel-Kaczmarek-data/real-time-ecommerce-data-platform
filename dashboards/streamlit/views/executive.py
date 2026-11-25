"""Executive Overview: headline KPIs, revenue trend, channel / region mix, live orders."""

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
from lib.data import Filters, pct_change, safe_query


def _totals(f: Filters, start, end) -> pd.Series | None:  # type: ignore[no-untyped-def]
    where, params = f.where_sales(start=start, end=end)
    df = safe_query(
        f"""
        SELECT sum(net_revenue_usd) net, sum(gross_revenue_usd) gross, sum(gmv_usd) gmv,
               sum(paid_orders) orders, sum(placed_orders) placed, sum(cancelled_orders) cancelled,
               sum(purchasing_customers) customers
        FROM mart_daily_sales WHERE {where}""",
        params,
    )
    return None if df is None or df.empty else df.iloc[0]


def _funnel(f: Filters, start, end) -> float | None:  # type: ignore[no-untyped-def]
    frag, p = f.in_list("sales_channel", f.channels)
    df = safe_query(
        f"""SELECT sum(converted_sessions) / nullif(sum(sessions), 0) AS cr FROM mart_conversion_funnel
                        WHERE date BETWEEN ? AND ? AND {frag}""",
        (start, end, *p),
    )
    return None if df is None or df.empty else df.loc[0, "cr"]


def _ops(f: Filters, start, end) -> tuple[float | None, float | None]:  # type: ignore[no-untyped-def]
    frag, p = f.in_list("country_code", f.countries)
    pay = safe_query(
        f"""SELECT sum(successful_payments) / nullif(sum(successful_payments + failed_payments), 0) r
                         FROM mart_payment_performance WHERE date BETWEEN ? AND ? AND {frag}""",
        (start, end, *p),
    )
    ship = safe_query(
        """SELECT sum(on_time_delivery_rate * delivered_shipments) / nullif(sum(delivered_shipments), 0) r
                         FROM mart_order_fulfillment WHERE date BETWEEN ? AND ?""",
        (start, end),
    )
    return (None if pay is None else pay.loc[0, "r"], None if ship is None else ship.loc[0, "r"])


def render(f: Filters) -> None:
    page_header(
        "Executive Overview",
        f"{f.start:%d %b %Y} – {f.end:%d %b %Y} · compared with the previous {f.days} days · USD",
    )
    cur, prev = _totals(f, f.start, f.end), _totals(f, f.prev_start, f.prev_end)
    if cur is None or pd.isna(cur["orders"]):
        empty_state()
        return
    cr, cr_prev = _funnel(f, f.start, f.end), _funnel(f, f.prev_start, f.prev_end)
    (pay, ship), (pay_prev, ship_prev) = _ops(f, f.start, f.end), _ops(f, f.prev_start, f.prev_end)
    aov = cur["gross"] / cur["orders"] if cur["orders"] else None
    aov_prev = prev["gross"] / prev["orders"] if prev is not None and prev["orders"] else None

    def g(s, k):  # type: ignore[no-untyped-def]
        return None if s is None else s[k]

    kpi_row(
        [
            kpi_card(
                "Net revenue",
                fmt_money(cur["net"], 1),
                pct_change(cur["net"], g(prev, "net")),
                "after refunds, ex tax",
            ),
            kpi_card("Gross revenue", fmt_money(cur["gross"], 1), pct_change(cur["gross"], g(prev, "gross"))),
            kpi_card(
                "GMV", fmt_money(cur["gmv"], 1), pct_change(cur["gmv"], g(prev, "gmv")), "all placed orders"
            ),
            kpi_card("Paid orders", fmt_int(cur["orders"]), pct_change(cur["orders"], g(prev, "orders"))),
        ]
    )
    st.write("")
    kpi_row(
        [
            kpi_card("Average order value", fmt_money(aov, 2), pct_change(aov, aov_prev)),
            kpi_card(
                "Conversion rate",
                fmt_pct(cr, 2),
                None if cr is None or cr_prev is None else cr - cr_prev,
                "sessions → purchase",
                delta_is_points=True,
            ),
            kpi_card(
                "Payment success",
                fmt_pct(pay),
                None if pay is None or pay_prev is None else pay - pay_prev,
                delta_is_points=True,
            ),
            kpi_card(
                "On-time delivery",
                fmt_pct(ship),
                None if ship is None or ship_prev is None else ship - ship_prev,
                delta_is_points=True,
            ),
        ]
    )

    where, params = f.where_sales()
    trend = safe_query(
        f"""SELECT date, sum(net_revenue_usd) net FROM mart_daily_sales WHERE {where}
                           GROUP BY date ORDER BY date""",
        params,
    )
    left, right = st.columns([2, 1])
    with left:
        if trend is not None and not trend.empty:
            trend["net_7d"] = trend["net"].rolling(7, min_periods=1).mean()
            fig = line_chart(
                trend,
                "date",
                {"net": "Net revenue", "net_7d": "7-day average"},
                "Net revenue per day",
                colors={"net": theme.SERIES[0], "net_7d": theme.SERIES[6]},
                fill_first=True,
            )
            chart(fig, 340)
    with right:
        mix = safe_query(
            f"""SELECT sales_channel, sum(net_revenue_usd) net FROM mart_daily_sales WHERE {where}
                             GROUP BY 1 ORDER BY 2 DESC""",
            params,
        )
        if mix is not None and not mix.empty:
            colors = [theme.CHANNEL_COLORS.get(c, theme.MUTED) for c in mix["sales_channel"]]
            fig = go.Figure(
                go.Pie(
                    labels=mix["sales_channel"],
                    values=mix["net"],
                    hole=0.62,
                    marker={"colors": colors, "line": {"color": theme.SURFACE, "width": 2}},
                    textinfo="percent",
                    sort=False,
                    hovertemplate="%{label}: $%{value:,.0f}<extra></extra>",
                )
            )
            fig.update_layout(
                title="Revenue by sales channel",
                showlegend=True,
                hovermode="closest",
                annotations=[
                    {
                        "text": fmt_money(mix["net"].sum(), 1),
                        "showarrow": False,
                        "font": {"size": 18, "color": theme.INK},
                    }
                ],
            )
            chart(fig, 340)

    c1, c2 = st.columns(2)
    with c1:
        reg = safe_query(
            f"""SELECT region, sum(net_revenue_usd) net FROM mart_daily_sales WHERE {where}
                             GROUP BY 1 ORDER BY 2 DESC""",
            params,
        )
        if reg is not None and not reg.empty:
            chart(
                bar_chart(
                    reg,
                    "region",
                    "net",
                    "Net revenue by region",
                    horizontal=True,
                    colors=[theme.REGION_COLORS.get(r, theme.MUTED) for r in reg["region"]],
                ),
                300,
            )
    with c2:
        cat_frag, cat_p = f.in_list("category_name", f.categories)
        top = safe_query(
            f"""
            SELECT p.product_name, sum(i.line_net_amount_usd) revenue
            FROM fact_order_items i JOIN dim_product p ON p.product_sk = i.product_sk
            WHERE i.is_paid AND i.order_date BETWEEN ? AND ? AND {cat_frag.replace("category_name", "p.category_name")}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8""",
            (f.start, f.end, *cat_p),
        )
        if top is not None and not top.empty:
            chart(bar_chart(top, "product_name", "revenue", "Top products by revenue", horizontal=True), 300)

    section("Live operations · last 60 minutes (real-time stream, watermarked)")
    rt = safe_query("""SELECT window_start, sum(orders_created) orders, sum(orders_cancelled) cancelled
                       FROM rt_sales_minute WHERE window_start >= (SELECT max(window_start) FROM rt_sales_minute)
                             - INTERVAL 60 MINUTE GROUP BY 1 ORDER BY 1""")
    if rt is not None and not rt.empty:
        a, b = st.columns([3, 1])
        with a:
            chart(
                line_chart(
                    rt, "window_start", {"orders": "Orders / minute"}, "Orders per minute", y_format=",.0f"
                ),
                240,
            )
        with b:
            st.markdown(
                kpi_card(
                    "Orders, last 60 min",
                    fmt_int(rt["orders"].sum()),
                    None,
                    f"latest minute: {rt['window_start'].max():%H:%M} UTC",
                ),
                unsafe_allow_html=True,
            )
            st.caption(
                "Operational view from the streaming aggregate. Events beyond the 10-minute watermark "
                "are reconciled in the daily Gold numbers above."
            )
