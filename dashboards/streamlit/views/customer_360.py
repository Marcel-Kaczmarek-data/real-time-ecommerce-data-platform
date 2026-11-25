"""Customer 360: unified profile of one customer (masked PII) and the top-customer list."""

from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from lib.components import (
    empty_state,
    export_button,
    fmt_int,
    fmt_money,
    kpi_card,
    kpi_row,
    page_header,
    section,
)
from lib.data import Filters, safe_query


def render(f: Filters) -> None:
    page_header(
        "Customer 360",
        "Unified profile from master data, orders, payments, refunds, returns and sessions. "
        "PII is masked in the lakehouse.",
    )
    frag, p = f.in_list("country_code", f.countries)
    top = safe_query(
        f"""SELECT customer_id, first_name || ' ' || last_name_initial AS name, country_code,
                                customer_segment, loyalty_status, total_completed_orders, total_net_spend_usd,
                                average_order_value_usd, days_since_last_purchase, rfm_code
                         FROM mart_customer_360 WHERE {frag}
                         ORDER BY total_net_spend_usd DESC LIMIT 500""",
        p,
    )
    if top is None or top.empty:
        empty_state()
        return
    c1, c2 = st.columns([1, 2])
    with c1:
        typed = st.text_input("Customer id", placeholder="CUST-0000123", key="c360-id").strip().upper()
    with c2:
        picked = st.selectbox(
            "…or pick one of the top customers by net spend",
            (top["customer_id"] + " · " + top["name"]).tolist(),
            key="c360-pick",
        )
    customer_id = typed or (picked.split(" · ")[0] if picked else None)
    prof = safe_query("SELECT * FROM mart_customer_360 WHERE customer_id = ?", (customer_id,))
    if prof is None or prof.empty:
        st.warning(f"Customer {customer_id} not found.")
        return
    c = prof.iloc[0]
    left, right = st.columns([1, 2])
    with left:
        last_session = c["most_recent_session"]
        last_session_txt = "–" if pd.isna(last_session) else f"{pd.Timestamp(last_session):%d %b %Y %H:%M}"
        st.markdown(
            f"""
<div class="profile-card">
  <div class="profile-name">{html.escape(str(c["first_name"]))} {html.escape(str(c["last_name_initial"]))}</div>
  <div class="muted">{html.escape(str(c["customer_id"]))} · {html.escape(str(c["email_masked"]))}</div>
  <div style="margin:10px 0">
    <span class="pill">{html.escape(str(c["customer_segment"]))}</span>
    <span class="pill">loyalty: {html.escape(str(c["loyalty_status"]))}</span>
    <span class="pill">{html.escape(str(c["country_code"]))}</span>
    <span class="pill">{html.escape(str(c["account_type"]))}</span>
  </div>
  <div class="muted">Registered: {pd.Timestamp(c["registration_date"]):%d %b %Y}</div>
  <div class="muted">First / last purchase: {c["first_purchase_date"] or "–"} / {c["last_purchase_date"] or "–"}</div>
  <div class="muted">Favourite category: {html.escape(str(c["favorite_category"] or "–"))}</div>
  <div class="muted">Preferred channel: {html.escape(str(c["preferred_channel"] or "–"))}</div>
  <div class="muted">Most recent session: {last_session_txt}</div>
  <div class="muted">RFM: {html.escape(str(c["rfm_code"] or "–"))} · marketing opt-in: {bool(c["marketing_opt_in"])}</div>
</div>""",
            unsafe_allow_html=True,
        )
    with right:
        kpi_row(
            [
                kpi_card("Completed orders", fmt_int(c["total_completed_orders"])),
                kpi_card("Net spend (CLV)", fmt_money(c["total_net_spend_usd"], 2)),
                kpi_card("Average order value", fmt_money(c["average_order_value_usd"], 2)),
            ]
        )
        st.write("")
        kpi_row(
            [
                kpi_card("Days since last purchase", fmt_int(c["days_since_last_purchase"])),
                kpi_card("Returns", fmt_int(c["total_returns"])),
                kpi_card("Refunds", fmt_money(c["total_refunds_usd"], 2)),
            ]
        )
        orders = safe_query(
            """SELECT order_id, order_date, sales_channel, order_status, item_count,
                                      round(gross_revenue_usd, 2) revenue_usd, round(refund_amount_usd, 2) refunds_usd,
                                      is_paid, payment_attempts
                               FROM fact_orders WHERE customer_id = ? ORDER BY order_timestamp DESC""",
            (customer_id,),
        )
        section("Order history")
        if orders is not None and not orders.empty:
            st.dataframe(orders, hide_index=True, use_container_width=True, height=260)
        else:
            empty_state("No orders yet.")
    section("Top customers by net spend")
    st.dataframe(
        top,
        hide_index=True,
        use_container_width=True,
        height=320,
        column_config={
            "total_net_spend_usd": st.column_config.NumberColumn("Net spend", format="$%.2f"),
            "average_order_value_usd": st.column_config.NumberColumn("AOV", format="$%.2f"),
        },
    )
    export_button(top, "top_customers")
