"""Product Performance: rankings, category mix, view-to-order conversion and returns."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from lib import theme
from lib.components import bar_chart, chart, empty_state, export_button, page_header, section
from lib.data import Filters, safe_query


def render(f: Filters) -> None:
    page_header(
        "Product Performance",
        "Paid order lines, behaviour from the clickstream and returns, "
        "aggregated per product for the selected period.",
    )
    cat_frag, cat_p = f.in_list("p.category_name", f.categories)
    df = safe_query(
        f"""
        WITH s AS (SELECT product_id, count(DISTINCT order_id) orders, sum(quantity) units,
                          sum(line_net_amount_usd) revenue, sum(line_margin_usd) margin
                   FROM fact_order_items WHERE is_paid AND order_date BETWEEN ? AND ? GROUP BY 1),
             v AS (SELECT product_id, sum(product_views) views, sum(add_to_cart_events) carts
                   FROM mart_product_performance WHERE month BETWEEN date_trunc('month', ?::DATE) AND ?
                   GROUP BY 1),
             r AS (SELECT product_id, sum(returned_quantity) returned FROM fact_returns
                   WHERE return_date BETWEEN ? AND ? GROUP BY 1)
        SELECT p.product_id, p.product_name, p.brand, p.category_name, p.unit_price AS list_price_usd,
               coalesce(s.orders, 0) orders, coalesce(s.units, 0) units, round(coalesce(s.revenue, 0), 2) revenue,
               round(coalesce(s.margin, 0), 2) margin, coalesce(v.views, 0) views, coalesce(v.carts, 0) carts,
               coalesce(r.returned, 0) returned,
               round(coalesce(v.carts, 0) / nullif(v.views, 0), 4) add_to_cart_rate,
               round(coalesce(r.returned, 0) / nullif(s.units, 0), 4) return_rate
        FROM dim_product p
        LEFT JOIN s ON s.product_id = p.product_id
        LEFT JOIN v ON v.product_id = p.product_id
        LEFT JOIN r ON r.product_id = p.product_id
        WHERE p.is_current AND p.product_id <> 'UNKNOWN' AND {cat_frag}
        ORDER BY revenue DESC""",
        (f.start, f.end, f.start, f.end, f.start, f.end, *cat_p),
    )
    if df is None or df.empty:
        empty_state()
        return
    df["revenue_rank"] = range(1, len(df) + 1)
    left, right = st.columns([3, 2])
    with left:
        chart(
            bar_chart(
                df.head(15), "product_name", "revenue", "Top 15 products by net revenue", horizontal=True
            ),
            440,
        )
    with right:
        cats = (
            df.groupby("category_name", as_index=False)["revenue"]
            .sum()
            .sort_values("revenue", ascending=False)
        )
        chart(
            bar_chart(
                cats.head(12),
                "category_name",
                "revenue",
                "Revenue by category",
                horizontal=True,
                color=theme.SERIES[2],
            ),
            440,
        )

    section("Demand vs conversion")
    sample = df[df["views"] > 0]
    if not sample.empty:
        fig = go.Figure(
            go.Scatter(
                x=sample["views"],
                y=sample["add_to_cart_rate"],
                mode="markers",
                marker={
                    "size": 8,
                    "color": theme.SERIES[0],
                    "opacity": 0.75,
                    "line": {"color": theme.SURFACE, "width": 1},
                },
                text=sample["product_name"],
                hovertemplate="%{text}<br>views %{x:,}<br>add-to-cart %{y:.1%}<extra></extra>",
            )
        )
        fig.update_layout(
            title="Product views vs add-to-cart rate (one dot per product)", hovermode="closest"
        )
        fig.update_xaxes(
            title="Product views", type="log", showgrid=True, gridcolor=theme.GRID, griddash="dot"
        )
        fig.update_yaxes(title="Add-to-cart rate", tickformat=".0%")
        chart(fig, 360)

    section("Product ranking")
    st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
        height=420,
        column_config={
            "revenue": st.column_config.NumberColumn("Revenue", format="$%.0f"),
            "margin": st.column_config.NumberColumn("Margin", format="$%.0f"),
            "list_price_usd": st.column_config.NumberColumn("List price", format="$%.2f"),
            "add_to_cart_rate": st.column_config.NumberColumn("Add-to-cart", format="%.3f"),
            "return_rate": st.column_config.NumberColumn("Return rate", format="%.3f"),
        },
    )
    export_button(df, "product_performance")
