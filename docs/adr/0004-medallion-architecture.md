# ADR 0004 — Medallion architecture

**Status:** Accepted

## Decision
Organise the lakehouse in **Bronze → Silver → Gold**, plus **Audit** and **Quarantine** areas.

| Layer | Contract |
|---|---|
| Bronze | Raw, append-only, full payload and source coordinates. Never filtered for business rules. |
| Silver | Typed, deduplicated and conformed (UTC, USD), with masked PII and validation; one row per entity or event. |
| Gold | Business-ready star schema and marts; promoted only when critical DQ checks pass. |
| Audit | DQ results, pipeline runs, ingestion file registry, watermarks. |
| Quarantine | Rejected records, with reasons, for investigation and replay. |

## Consequences
* Any downstream table can be rebuilt from Bronze by replay, so transformation
  bugs can be fixed retroactively.
* Data is stored 2–3 times. Lifecycle rules, VACUUM and per-layer retention
  limit the cost.
* Ownership is clear: the platform team owns Bronze and Silver; analytics
  engineering owns Gold.
