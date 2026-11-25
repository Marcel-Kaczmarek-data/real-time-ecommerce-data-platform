# ADR 0007 — Data modelling decisions

**Status:** Accepted

## Decisions
1. **Kimball star schema** in Gold, with conformed dimensions shared by all
   facts. BI tools such as Power BI, and analysts, work best with stars.
2. **SCD Type 2** for customer segment / loyalty / country and for product
   price / status / category, so historical revenue keeps the attributes that
   were valid at transaction time. **Type 1** for correction-style attributes.
3. **Hashed surrogate keys** (`xxhash64(natural_key, valid_from)`) instead of
   sequences. They are deterministic across rebuilds and parallel jobs, and
   the collision risk is negligible at < 10⁸ rows. Trade-off: keys are not dense.
4. **Unknown (-1) and inferred members** for missing or late dimension rows,
   so facts are never silently dropped.
5. **Facts store both local currency and USD.** FX conversion happens once, in
   Silver, and the rate date is recorded.
6. **Marts are denormalised and pre-aggregated** for dashboard speed. They are
   rebuilt from facts, never edited.

## Alternatives considered
* Data Vault 2.0: better for many volatile sources; too heavy for a single domain.
* One big table: simple to serve, but painful for SCD2 history and conformed reuse.
