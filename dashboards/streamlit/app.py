"""E-commerce analytics dashboard (Streamlit) on the Gold layer.

    streamlit run dashboards/streamlit/app.py

All numbers are queried live from Gold Delta tables through the DuckDB
serving layer (serving/warehouse.py). Nothing is hard-coded.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import streamlit as st

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
for p in (str(HERE), str(ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from lib import theme  # noqa: E402
from lib.data import ALL_CHANNELS, Filters, data_bounds, options, warehouse  # noqa: E402
from views import (  # noqa: E402
    conversion,
    customer_360,
    customers,
    data_quality,
    executive,
    fulfillment,
    inventory,
    payments,
    products,
    sales,
)

st.set_page_config(
    page_title="E-Commerce Data Platform", page_icon="📊", layout="wide", initial_sidebar_state="expanded"
)
theme.register_plotly_template()
st.markdown(theme.CSS, unsafe_allow_html=True)


def sidebar_filters() -> Filters:
    lo, hi = data_bounds()
    with st.sidebar:
        st.markdown("### 🛒 E-Commerce Platform")
        st.caption("Gold layer · Delta Lake · DuckDB serving")
        preset = st.radio(
            "Period",
            ["Last 7 days", "Last 30 days", "Last 90 days", "All data", "Custom"],
            index=1,
            horizontal=False,
        )
        if preset == "Custom":
            picked = st.date_input(
                "Date range", value=(max(lo, hi - timedelta(days=29)), hi), min_value=lo, max_value=hi
            )
            start, end = picked if isinstance(picked, tuple) and len(picked) == 2 else (lo, hi)
        else:
            days = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}.get(preset)
            start, end = (lo, hi) if days is None else (max(lo, hi - timedelta(days=days - 1)), hi)
        countries = st.multiselect(
            "Country",
            options("SELECT country_code FROM dim_country WHERE country_code <> 'UNKNOWN' ORDER BY 1"),
            placeholder="All",
        )
        channels = st.multiselect("Sales channel", ALL_CHANNELS, placeholder="All")
        categories = st.multiselect(
            "Product category",
            options("SELECT category_name FROM dim_category WHERE category_id <> 'UNKNOWN' ORDER BY 1"),
            placeholder="All",
        )
        st.divider()
        st.session_state["auto_refresh"] = st.toggle(
            "Auto-refresh (60 s)", value=st.session_state.get("auto_refresh", False)
        )
        versions = warehouse().versions()
        if versions:
            st.caption(f"Tables loaded: {len(versions)} · data through **{hi:%d %b %Y}**")
    return Filters(
        start=start,
        end=end,
        countries=tuple(countries),
        channels=tuple(channels),
        categories=tuple(categories),
    )


def make_page(render: Callable[[Filters], None], filters: Filters) -> Callable[[], None]:
    def page() -> None:
        run_every = 60 if st.session_state.get("auto_refresh") else None

        @st.fragment(run_every=run_every)
        def body() -> None:
            with st.spinner("Loading…"):
                render(filters)

        body()

    page.__name__ = render.__module__.split(".")[-1]
    return page


filters = sidebar_filters()
PAGES = [
    (executive.render, "Executive Overview", "📈"),
    (sales.render, "Sales & Revenue", "💰"),
    (customers.render, "Customer Analytics", "👥"),
    (customer_360.render, "Customer 360", "🧑"),
    (products.render, "Product Performance", "📦"),
    (conversion.render, "Conversion Funnel", "🔻"),
    (inventory.render, "Inventory Management", "🏭"),
    (fulfillment.render, "Order Fulfillment", "🚚"),
    (payments.render, "Payments & Refunds", "💳"),
    (data_quality.render, "Data Quality & Pipeline Health", "🛡️"),
]
nav = st.navigation(
    [
        st.Page(
            make_page(fn, filters),
            title=title,
            icon=icon,
            default=(i == 0),
            url_path=title.lower().replace(" & ", "-").replace(" ", "-"),
        )
        for i, (fn, title, icon) in enumerate(PAGES)
    ]
)
nav.run()
