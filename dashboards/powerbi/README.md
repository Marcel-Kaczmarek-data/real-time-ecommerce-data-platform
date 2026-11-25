# Power BI integration

No `.pbix` file is committed. A binary report can't be reviewed in Git, and
generating one without Power BI Desktop would mean shipping an artefact nobody
has opened. This folder holds everything needed to build the report in about
15 minutes, together with the **semantic model specification** and
**DAX measures** that encode the KPI definitions used everywhere else in the
platform (`docs/kpi-definitions.md`).

## 1. Connect to the Gold layer

| Environment | Source | Mode |
|---|---|---|
| Local (Docker) | PostgreSQL `analytics` schema, published by `python -m pipelines.gold.publish_serving` (Airflow `daily_sales_aggregation`) | Import |
| Cloud | Databricks SQL Warehouse → Unity Catalog `ecommerce_<env>.gold` | Import or DirectQuery |

**Local:**
1. `make up && make demo` (or run the `daily_sales_aggregation` DAG) so that `analytics.*` exists.
2. Power BI Desktop → *Get data* → *PostgreSQL database*.
   - Server: `localhost:5433` (host port of the platform's PostgreSQL)
   - Database: `ecommerce`
   - Data connectivity mode: **Import**
3. Credentials: *Database* authentication, with the `POSTGRES_USER` / `POSTGRES_PASSWORD` from `.env`.
   In production, use a read-only role (`docs/security.md#bi-access`).
4. Select the tables listed in the model below (`analytics.dim_*`, `analytics.fact_*`, `analytics.mart_*`).

**Databricks:**
*Get data* → *Azure Databricks* / *Databricks*, then use the SQL Warehouse's
*Server hostname* and *HTTP path*. Authenticate with Microsoft Entra ID or a PAT
held in a credential manager, never stored in the PBIX.

Published tables never contain raw PII: `email_hash` and internal columns are
dropped by the publisher, and names are masked in Silver.

## 2. Semantic model

Star schema, single-direction filters (dimension → fact), with `dim_date` marked
as the date table. See [`semantic-model.md`](semantic-model.md) for every
table, column role and relationship.

```
dim_date ─┬─< fact_orders >─┬─ dim_customer (customer_sk)
          │                 ├─ dim_country (country_sk)
          │                 ├─ dim_sales_channel (channel_sk)
          │                 └─ dim_promotion (promotion_sk)
          ├─< fact_order_items >── dim_product (product_sk) ── dim_category
          ├─< fact_payments
          ├─< fact_shipments >── dim_warehouse
          └─< fact_refunds / fact_returns
```

## 3. Measures

[`measures.dax`](measures.dax) contains every KPI as a DAX measure: GMV, gross and
net revenue, AOV, refund rate, conversion, payment success, on-time delivery,
retention and time intelligence (MTD, YoY, WoW). Create a `_Measures` table
and paste the measures in (Model view → *New measure*), or use Tabular Editor →
*Advanced scripting*.

## 4. Suggested pages

These mirror the Streamlit app, so both front ends show identical numbers:

1. Executive overview: KPI cards (Net revenue, GMV, Paid orders, AOV, Conversion), net revenue trend with 7-day average, revenue by channel and region.
2. Sales: by country (filled map), category, channel; discount and refund impact.
3. Customers: segments (from `mart_customer_360`), cohort matrix (`mart_customer_retention`).
4. Operations: fulfilment, on-time delivery, inventory alerts, payment success.

## 5. Refresh

* Local: refresh after `daily_sales_aggregation` completes (it publishes atomically with staging-table swaps).
* Cloud: a scheduled refresh in the Power BI service via the on-premises / VNet gateway, or DirectQuery.

## 6. Validation

After building the report, compare its **Net revenue (USD)** for any period with the
Streamlit Executive Overview and `gold.mart_daily_sales`. They are computed from
the same facts, so the values must match to the cent.
