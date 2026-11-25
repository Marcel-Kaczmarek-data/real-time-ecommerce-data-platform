"""Conversion Funnel: sessions -> product view -> cart -> checkout -> purchase, by device and source."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from lib import theme
from lib.components import (
    chart,
    empty_state,
    fmt_int,
    fmt_pct,
    kpi_card,
    kpi_row,
    line_chart,
    page_header,
    section,
)
from lib.data import Filters, safe_query


def _where(f: Filters) -> tuple[str, tuple[object, ...]]:
    frag, p = f.in_list("sales_channel", f.channels)
    return f"date BETWEEN ? AND ? AND {frag}", (f.start, f.end, *p)


def render(f: Filters) -> None:
    page_header(
        "Conversion Funnel",
        "Unit of analysis: reconstructed sessions (30-minute inactivity rule, identity stitched at login).",
    )
    where, params = _where(f)
    tot = safe_query(
        f"""SELECT sum(sessions) sessions, sum(sessions_with_product_view) viewed,
                                sum(sessions_with_cart) carts, sum(sessions_with_checkout) checkouts,
                                sum(converted_sessions) purchases, sum(bounced_sessions) bounced
                         FROM mart_conversion_funnel WHERE {where}""",
        params,
    )
    if tot is None or tot.empty or not tot.loc[0, "sessions"]:
        empty_state()
        return
    t = tot.iloc[0]
    kpi_row(
        [
            kpi_card("Sessions", fmt_int(t["sessions"])),
            kpi_card("Add-to-cart rate", fmt_pct(t["carts"] / t["sessions"], 2)),
            kpi_card("Checkout rate", fmt_pct(t["checkouts"] / t["sessions"], 2)),
            kpi_card("Purchase conversion", fmt_pct(t["purchases"] / t["sessions"], 2)),
            kpi_card(
                "Cart abandonment",
                fmt_pct(1 - t["purchases"] / t["carts"] if t["carts"] else None),
                hint="carts without purchase",
            ),
        ]
    )
    left, right = st.columns([2, 3])
    with left:
        stages = ["Sessions", "Viewed a product", "Added to cart", "Started checkout", "Purchased"]
        values = [t["sessions"], t["viewed"], t["carts"], t["checkouts"], t["purchases"]]
        fig = go.Figure(
            go.Funnel(
                y=stages,
                x=values,
                textinfo="value+percent initial",
                marker={"color": [theme.SEQUENTIAL[i] for i in (6, 5, 4, 3, 2)]},
                connector={"line": {"color": theme.AXIS, "width": 1}},
                hovertemplate="%{y}: %{x:,} (%{percentInitial:.1%} of sessions)<extra></extra>",
            )
        )
        fig.update_layout(title="Session funnel", hovermode="closest")
        chart(fig, 380)
    with right:
        dev = safe_query(
            f"""SELECT device_type, sum(converted_sessions) / sum(sessions) cr,
                                    sum(sessions_with_cart) / sum(sessions) atc
                             FROM mart_conversion_funnel WHERE {where} GROUP BY 1 ORDER BY 1""",
            params,
        )
        if dev is not None and not dev.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=dev["device_type"],
                    y=dev["atc"],
                    name="Add-to-cart rate",
                    marker={"color": theme.SERIES[0], "cornerradius": 4},
                )
            )
            fig.add_trace(
                go.Bar(
                    x=dev["device_type"],
                    y=dev["cr"],
                    name="Purchase conversion",
                    marker={"color": theme.SERIES[1], "cornerradius": 4},
                )
            )
            fig.update_layout(
                title="Funnel rates by device",
                barmode="group",
                bargap=0.3,
                bargroupgap=0.08,
                hovermode="x unified",
            )
            fig.update_yaxes(tickformat=".1%")
            chart(fig, 380)

    section("By traffic source")
    src = safe_query(
        f"""SELECT traffic_source, sum(sessions) sessions, sum(converted_sessions) purchases,
                                sum(converted_sessions) / sum(sessions) conversion_rate,
                                sum(sessions_with_cart) / sum(sessions) add_to_cart_rate,
                                1 - sum(converted_sessions) / nullif(sum(sessions_with_cart), 0) cart_abandonment_rate,
                                avg(avg_session_seconds) avg_session_seconds
                         FROM mart_conversion_funnel WHERE {where} GROUP BY 1 ORDER BY conversion_rate DESC""",
        params,
    )
    if src is not None and not src.empty:
        st.dataframe(
            src,
            hide_index=True,
            use_container_width=True,
            column_config={
                "conversion_rate": st.column_config.ProgressColumn(
                    "Conversion", format="%.3f", min_value=0, max_value=float(src["conversion_rate"].max())
                ),
                "add_to_cart_rate": st.column_config.NumberColumn("Add-to-cart", format="%.3f"),
                "cart_abandonment_rate": st.column_config.NumberColumn("Cart abandonment", format="%.3f"),
                "avg_session_seconds": st.column_config.NumberColumn("Avg session (s)", format="%.0f"),
            },
        )
    trend = safe_query(
        f"""SELECT date, sum(converted_sessions) / sum(sessions) cr,
                                  1 - sum(converted_sessions) / nullif(sum(sessions_with_cart), 0) abandon
                           FROM mart_conversion_funnel WHERE {where} GROUP BY 1 ORDER BY 1""",
        params,
    )
    if trend is not None and not trend.empty:
        a, b = st.columns(2)
        with a:
            chart(
                line_chart(
                    trend, "date", {"cr": "Conversion rate"}, "Purchase conversion per day", y_format=".2%"
                ),
                260,
            )
        with b:
            chart(
                line_chart(
                    trend,
                    "date",
                    {"abandon": "Cart abandonment"},
                    "Cart abandonment per day",
                    colors={"abandon": theme.SERIES[7]},
                    y_format=".1%",
                ),
                260,
            )
