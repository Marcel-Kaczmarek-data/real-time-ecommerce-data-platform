"""Order Fulfillment: fulfilment and delivery times, on-time rate, carriers, SLA violations."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib import theme
from lib.components import (
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
)
from lib.data import Filters, safe_query


def render(f: Filters) -> None:
    page_header(
        "Order Fulfillment",
        "Shipments by ship date. On-time = delivered by the promised date; "
        "SLA = carrier hand-over within 48 h of confirmation.",
    )
    tot = safe_query(
        """SELECT sum(shipments) shipments, sum(delivered_shipments) delivered,
                               sum(delayed_shipments) delayed, sum(sla_breaches) breaches,
                               sum(avg_fulfillment_hours * shipments) / sum(shipments) fulfil_h,
                               sum(avg_transit_days * delivered_shipments) / nullif(sum(delivered_shipments), 0) transit,
                               sum(on_time_delivery_rate * delivered_shipments) / nullif(sum(delivered_shipments), 0) ontime
                        FROM mart_order_fulfillment WHERE date BETWEEN ? AND ?""",
        (f.start, f.end),
    )
    canc = safe_query(
        f"""SELECT sum(cancelled_orders) / sum(placed_orders) r FROM mart_daily_sales
                          WHERE {f.where_sales()[0]}""",
        f.where_sales()[1],
    )
    if tot is None or tot.empty or not tot.loc[0, "shipments"]:
        empty_state()
        return
    t = tot.iloc[0]
    kpi_row(
        [
            kpi_card("Shipments", fmt_int(t["shipments"])),
            kpi_card("Avg fulfilment time", f"{t['fulfil_h']:.1f} h", hint="confirmation → carrier"),
            kpi_card("Avg transit time", "–" if t["transit"] is None else f"{t['transit']:.1f} days"),
            kpi_card("On-time delivery", fmt_pct(t["ontime"])),
            kpi_card(
                "Delayed shipments", fmt_int(t["delayed"]), hint=f"SLA breaches: {int(t['breaches'] or 0):,}"
            ),
            kpi_card(
                "Cancellation rate",
                fmt_pct(None if canc is None else canc.loc[0, "r"]),
                hint="of placed orders",
            ),
        ]
    )
    daily = safe_query(
        """SELECT date, sum(on_time_delivery_rate * delivered_shipments) / nullif(sum(delivered_shipments), 0) ontime,
                                 sum(avg_fulfillment_hours * shipments) / sum(shipments) fulfil_h
                          FROM mart_order_fulfillment WHERE date BETWEEN ? AND ? GROUP BY 1 ORDER BY 1""",
        (f.start, f.end),
    )
    if daily is not None and not daily.empty:
        a, b = st.columns(2)
        with a:
            chart(
                line_chart(
                    daily,
                    "date",
                    {"ontime": "On-time rate"},
                    "On-time delivery rate by ship date",
                    colors={"ontime": theme.SERIES[2]},
                    y_format=".1%",
                ),
                280,
            )
        with b:
            chart(
                line_chart(
                    daily,
                    "date",
                    {"fulfil_h": "Fulfilment hours"},
                    "Average fulfilment time (hours)",
                    y_format=",.1f",
                ),
                280,
            )

    section("Carrier performance")
    car = safe_query(
        """SELECT carrier, sum(shipments) shipments, sum(delivered_shipments) delivered,
                               sum(on_time_delivery_rate * delivered_shipments) / nullif(sum(delivered_shipments), 0) on_time_rate,
                               sum(avg_transit_days * delivered_shipments) / nullif(sum(delivered_shipments), 0) avg_transit_days,
                               sum(delayed_shipments) delayed
                        FROM mart_order_fulfillment WHERE date BETWEEN ? AND ? GROUP BY 1 ORDER BY shipments DESC""",
        (f.start, f.end),
    )
    if car is not None and not car.empty:
        a, b = st.columns([3, 2])
        with a:
            fig = go.Figure(
                go.Bar(
                    x=car["carrier"],
                    y=car["on_time_rate"],
                    marker={"color": theme.SERIES[0], "cornerradius": 4},
                    text=["" if pd.isna(v) else f"{v:.0%}" for v in car["on_time_rate"]],
                    textposition="outside",
                    textfont={"color": theme.INK_2},
                    hovertemplate="%{x}: %{y:.1%}<extra></extra>",
                )
            )
            fig.update_layout(title="On-time rate by carrier", hovermode="closest", bargap=0.4)
            fig.update_yaxes(tickformat=".0%", range=[0, 1.1])
            chart(fig, 320)
        with b:
            st.dataframe(
                car,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "on_time_rate": st.column_config.NumberColumn("On-time", format="%.3f"),
                    "avg_transit_days": st.column_config.NumberColumn("Transit (d)", format="%.2f"),
                },
            )
    section("SLA violations and delays by warehouse")
    wh = safe_query(
        """SELECT warehouse_name, region, sum(shipments) shipments, sum(sla_breaches) sla_breaches,
                              sum(delayed_shipments) delayed, max(p90_fulfillment_hours) worst_p90_fulfilment_h
                       FROM mart_order_fulfillment WHERE date BETWEEN ? AND ? GROUP BY 1, 2
                       ORDER BY sla_breaches DESC""",
        (f.start, f.end),
    )
    if wh is not None and not wh.empty:
        st.dataframe(wh, hide_index=True, use_container_width=True)
        export_button(wh, "fulfilment_by_warehouse")
