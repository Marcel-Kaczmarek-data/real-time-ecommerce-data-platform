"""Inventory Management: stock position, alerts, replenishment needs and warehouse utilisation."""

from __future__ import annotations

import html

import streamlit as st

from lib import theme
from lib.components import (
    bar_chart,
    chart,
    empty_state,
    export_button,
    fmt_int,
    fmt_money,
    fmt_pct,
    kpi_card,
    kpi_row,
    page_header,
    section,
    status_badge,
)
from lib.data import Filters, safe_query


def render(f: Filters) -> None:
    page_header(
        "Inventory Management",
        "Latest daily snapshot (opening stock + all movements since). "
        "Demand = paid units in the last 30 days.",
    )
    frag, p = f.in_list("category_name", f.categories)
    inv = safe_query(f"SELECT * FROM mart_inventory_health WHERE {frag}", p)
    if inv is None or inv.empty:
        empty_state()
        return
    total = len(inv)
    kpi_row(
        [
            kpi_card(
                "Units available",
                fmt_int(inv["available_quantity"].sum()),
                hint=f"{total:,} product × warehouse",
            ),
            kpi_card(
                "Units reserved",
                fmt_int(inv["reserved_quantity"].sum()),
                hint="allocated to unshipped orders",
            ),
            kpi_card(
                "Stock-out rate",
                fmt_pct(inv["is_stockout"].mean()),
                hint=f"{int(inv['is_stockout'].sum())} positions",
            ),
            kpi_card(
                "Low-stock positions", fmt_int(inv["is_low_stock"].sum()), hint="below reorder threshold"
            ),
            kpi_card(
                "Inventory value", fmt_money(inv["inventory_value_usd"].sum(), 1), hint="on hand × unit cost"
            ),
        ]
    )
    left, right = st.columns([3, 2])
    with left:
        section("Inventory alerts")
        alerts = inv[inv["stock_status"].isin(["Stock-out", "Low stock"])].sort_values(
            ["is_stockout", "days_of_cover"], ascending=[False, True]
        )[
            [
                "stock_status",
                "product_name",
                "category_name",
                "warehouse_name",
                "available_quantity",
                "reserved_quantity",
                "reorder_threshold",
                "days_of_cover",
                "replenishment_units",
            ]
        ]
        if alerts.empty:
            st.success("✔ No stock-outs or low-stock positions.")
        else:
            badges = alerts.head(12).copy()
            rows = "".join(
                f"<tr><td class='nowrap'>{status_badge(r.stock_status)}</td><td>{html.escape(str(r.product_name))}</td>"
                f"<td class='muted'>{html.escape(str(r.warehouse_name))}</td>"
                f"<td style='text-align:right'>{int(r.available_quantity)}</td>"
                f"<td style='text-align:right'>{int(r.replenishment_units)}</td></tr>"
                for r in badges.itertuples()
            )
            st.markdown(
                f"""<table style="width:100%;font-size:0.85rem;border-collapse:collapse">
<tr class="muted"><th align="left">Status</th><th align="left">Product</th><th align="left">Warehouse</th>
<th align="right">Available</th><th align="right">Reorder qty</th></tr>{rows}</table>""",
                unsafe_allow_html=True,
            )
            with st.expander(f"All {len(alerts):,} alerts"):
                st.dataframe(alerts, hide_index=True, use_container_width=True)
                export_button(alerts, "inventory_alerts")
    with right:
        status = inv.groupby("stock_status", as_index=False).size().rename(columns={"size": "positions"})
        order = {"Stock-out": 0, "Low stock": 1, "Healthy": 2, "Overstock": 3}
        status = status.sort_values("stock_status", key=lambda s: s.map(order))
        colors = {
            "Stock-out": theme.CRITICAL,
            "Low stock": theme.WARNING,
            "Healthy": theme.GOOD,
            "Overstock": theme.SERIOUS,
        }
        chart(
            bar_chart(
                status,
                "stock_status",
                "positions",
                "Positions by stock status (labelled)",
                value_format=",.0f",
                colors=[colors[s] for s in status["stock_status"]],
            ),
            330,
        )

    section("Warehouses")
    wh = (
        inv.groupby(["warehouse_name", "region"], as_index=False)
        .agg(
            utilization=("warehouse_utilization", "max"),
            units=("on_hand_quantity", "sum"),
            stockouts=("is_stockout", "sum"),
            value=("inventory_value_usd", "sum"),
        )
        .sort_values("utilization", ascending=False)
    )
    a, b = st.columns([3, 2])
    with a:
        chart(
            bar_chart(
                wh,
                "warehouse_name",
                "utilization",
                "Warehouse utilisation (on-hand units / capacity)",
                horizontal=True,
                value_format=".1%",
                colors=[theme.REGION_COLORS.get(r, theme.MUTED) for r in wh["region"]],
            ),
            460,
        )
    with b:
        st.dataframe(
            wh,
            hide_index=True,
            use_container_width=True,
            height=440,
            column_config={
                "utilization": st.column_config.ProgressColumn(
                    "Utilisation", format="%.3f", min_value=0, max_value=1
                ),
                "value": st.column_config.NumberColumn("Value", format="$%.0f"),
            },
        )
    turnover = (
        inv.groupby("category_name", as_index=False)["annualised_turnover"]
        .mean()
        .dropna()
        .sort_values("annualised_turnover", ascending=False)
    )
    if not turnover.empty:
        chart(
            bar_chart(
                turnover.head(15),
                "category_name",
                "annualised_turnover",
                "Inventory turnover by category (annualised, from 30-day demand)",
                horizontal=True,
                value_format=",.1f",
                color=theme.SERIES[2],
            ),
            420,
        )
