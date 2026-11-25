"""Sales & Revenue: trends by channel, geography, categories, discount and refund impact."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from lib import theme
from lib.components import (
    bar_chart,
    chart,
    empty_state,
    export_button,
    fmt_money,
    fmt_pct,
    kpi_card,
    kpi_row,
    line_chart,
    page_header,
    section,
)
from lib.data import ISO3, Filters, pct_change, safe_query


def render(f: Filters) -> None:
    page_header("Sales & Revenue", "Revenue = paid orders, excluding tax; net revenue is after refunds. USD.")
    where, params = f.where_sales()
    pwhere, pparams = f.where_sales(start=f.prev_start, end=f.prev_end)
    sql = """SELECT sum(gross_revenue_usd) gross, sum(net_revenue_usd) net, sum(discount_usd) disc,
                    sum(refunds_net_of_tax_usd) refunds, sum(units_sold) units
             FROM mart_daily_sales WHERE {w}"""
    cur, prev = safe_query(sql.format(w=where), params), safe_query(sql.format(w=pwhere), pparams)
    if cur is None or cur.empty or cur.loc[0, "gross"] is None:
        empty_state()
        return
    c = cur.iloc[0]
    p = prev.iloc[0] if prev is not None and not prev.empty else None

    def prev_of(key: str):  # type: ignore[no-untyped-def]
        return None if p is None else p[key]

    kpi_row(
        [
            kpi_card("Gross revenue", fmt_money(c["gross"], 1), pct_change(c["gross"], prev_of("gross"))),
            kpi_card("Net revenue", fmt_money(c["net"], 1), pct_change(c["net"], prev_of("net"))),
            kpi_card(
                "Discounts given",
                fmt_money(c["disc"], 1),
                pct_change(c["disc"], prev_of("disc")),
                good_when_up=False,
            ),
            kpi_card(
                "Refunds (ex tax)",
                fmt_money(c["refunds"], 1),
                pct_change(c["refunds"], prev_of("refunds")),
                good_when_up=False,
            ),
            kpi_card("Refund rate", fmt_pct(c["refunds"] / c["gross"] if c["gross"] else None)),
        ]
    )

    trend = safe_query(
        f"""SELECT date, sales_channel, sum(gross_revenue_usd) gross FROM mart_daily_sales
                           WHERE {where} GROUP BY 1, 2 ORDER BY 1""",
        params,
    )
    if trend is not None and not trend.empty:
        pivot = trend.pivot_table(
            index="date", columns="sales_channel", values="gross", fill_value=0
        ).reset_index()
        channels = [ch for ch in theme.CHANNEL_COLORS if ch in pivot.columns]
        chart(
            line_chart(
                pivot,
                "date",
                {ch: ch for ch in channels},
                "Gross revenue per day by sales channel",
                colors=theme.CHANNEL_COLORS,
            ),
            340,
        )

    left, right = st.columns([3, 2])
    geo = safe_query(
        f"""SELECT country_code, any_value(country_name) country, sum(net_revenue_usd) net,
                                sum(paid_orders) orders
                         FROM mart_daily_sales WHERE {where} GROUP BY 1""",
        params,
    )
    with left:
        if geo is not None and not geo.empty:
            geo["iso3"] = geo["country_code"].map(ISO3)
            fig = go.Figure(
                go.Choropleth(
                    locations=geo["iso3"],
                    z=geo["net"],
                    text=geo["country"],
                    locationmode="ISO-3",
                    colorscale=[[i / (len(theme.SEQUENTIAL) - 1), c] for i, c in enumerate(theme.SEQUENTIAL)],
                    marker_line_color=theme.SURFACE,
                    marker_line_width=0.5,
                    colorbar={"tickprefix": "$", "tickformat": ",.2s", "len": 0.7},
                    hovertemplate="%{text}: $%{z:,.0f}<extra></extra>",
                )
            )
            fig.update_geos(
                bgcolor=theme.SURFACE,
                showframe=False,
                showcoastlines=False,
                landcolor="#262624",
                showcountries=True,
                countrycolor=theme.AXIS,
                projection_type="natural earth",
            )
            fig.update_layout(title="Net revenue by country", hovermode="closest")
            chart(fig, 380)
    with right:
        if geo is not None and not geo.empty:
            top = geo.sort_values("net", ascending=False).head(10)
            chart(bar_chart(top, "country", "net", "Top 10 countries by net revenue", horizontal=True), 380)

    section("Category performance")
    cat_frag, cat_p = f.in_list("category_name", f.categories)
    cats = safe_query(
        f"""SELECT category_name, department, sum(net_merchandise_usd) revenue,
                                 sum(gross_margin_usd) margin, sum(units_sold) units, sum(orders) orders
                          FROM mart_daily_category_sales WHERE date BETWEEN ? AND ? AND {cat_frag}
                          GROUP BY 1, 2 ORDER BY revenue DESC""",
        (f.start, f.end, *cat_p),
    )
    if cats is not None and not cats.empty:
        a, b = st.columns([3, 2])
        with a:
            chart(
                bar_chart(
                    cats.head(15),
                    "category_name",
                    "revenue",
                    "Revenue by product category (top 15)",
                    horizontal=True,
                ),
                420,
            )
        with b:
            cats["margin_pct"] = cats["margin"] / cats["revenue"]
            st.dataframe(
                cats,
                hide_index=True,
                use_container_width=True,
                height=380,
                column_config={
                    "revenue": st.column_config.NumberColumn("Revenue", format="$%.0f"),
                    "margin": st.column_config.NumberColumn("Margin", format="$%.0f"),
                    "margin_pct": st.column_config.ProgressColumn(
                        "Margin %", format="%.2f", min_value=0, max_value=1
                    ),
                },
            )
            export_button(cats, "category_performance")

    section("Discount and refund impact")
    imp = safe_query(
        f"""SELECT date,
                                sum(discount_usd) / nullif(sum(gross_revenue_usd) + sum(discount_usd), 0) disc_rate,
                                sum(refunds_net_of_tax_usd) / nullif(sum(gross_revenue_usd), 0) refund_rate
                         FROM mart_daily_sales WHERE {where} GROUP BY 1 ORDER BY 1""",
        params,
    )
    if imp is not None and not imp.empty:
        x, y = st.columns(2)
        with x:
            chart(
                line_chart(
                    imp,
                    "date",
                    {"disc_rate": "Discount rate"},
                    "Discount as share of list price",
                    y_format=".1%",
                ),
                260,
            )
        with y:
            chart(
                line_chart(
                    imp,
                    "date",
                    {"refund_rate": "Refund rate"},
                    "Refunds as share of revenue",
                    colors={"refund_rate": theme.SERIES[7]},
                    y_format=".1%",
                ),
                260,
            )
    detail = safe_query(
        f"SELECT * FROM mart_daily_sales WHERE {where} ORDER BY date DESC, net_revenue_usd DESC", params
    )
    if detail is not None:
        with st.expander("Daily sales detail (date x country x channel)"):
            st.dataframe(detail, hide_index=True, use_container_width=True, height=320)
            export_button(detail, "daily_sales")
