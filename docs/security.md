# Security

## Principles
1. **No secrets in Git.** Configuration reads secrets from environment variables (`${VAR}` placeholders in
   `config/*.yaml`). `.env` is git-ignored; `.env.example` holds placeholders only; CI runs Gitleaks.
2. **Local ≠ production.** The local defaults (`change-me-local-only`, `localdev`) exist only for disposable
   containers on a developer machine. They are deliberately weak and never valid anywhere else.
3. **Least privilege, data minimisation, defence in depth.**

## Local development vs production

| Concern | Local (Docker Compose) | Production (AWS + Databricks) |
|---|---|---|
| Secrets | `.env` file (git-ignored) | AWS Secrets Manager / Databricks secret scope `ecom-platform`, injected as env vars (`{{secrets/...}}` in the job definition) |
| Kafka | PLAINTEXT on a private Docker network | SASL_SSL (Confluent Cloud API keys / MSK IAM), TLS 1.2+, ACLs per service account |
| Object storage | SeaweedFS with per-identity keys (`platform`, read-only `bi-reader`) | S3 with SSE-KMS (customer key, rotation on), TLS-only bucket policy, public access blocked, access logging |
| Compute access | Containers on one host | Unity Catalog storage credential (IAM role, external id); job clusters with a cluster policy (`SINGLE_USER` mode) |
| Database | Single user | Separate roles: `ingestion_reader` (SELECT on `ops`), `platform_writer` (`meta`, `analytics`), `bi_reader` (SELECT on `analytics`) |
| Network | Host ports bound for convenience | Private subnets, S3 gateway endpoint, no public endpoints; Databricks PrivateLink (recommended) |

## Identity & access (cloud)

Defined in `infrastructure/terraform/modules/iam` and `modules/databricks_uc`:

* `uc-lakehouse-access` (role): assumed only by the Databricks Unity Catalog master role with the account's
  external id. It has read/write access to the two data buckets and use of the KMS key, nothing else.
* `ingestion-writer` (role, optional): `PutObject` on the landing bucket only. It cannot read anything.
* `gold-read-only` (policy): `GetObject` on `gold/*` only. Bronze and Silver (which may contain PII) are not readable.
* Unity Catalog grants: `data-engineers` own all schemas; `analysts` have `USE` + `SELECT` on `gold` only.

### BI access
Power BI connects with a dedicated read-only principal: the PostgreSQL `bi_reader` role on `analytics` (local), or a
Databricks service principal with `SELECT` on `gold` through a SQL Warehouse (cloud). Report credentials live in
the Power BI gateway / service, never in the PBIX.

## Personal data (PII)

| Layer | Customer PII | Controls |
|---|---|---|
| Kafka `ecommerce.customers` | names, e-mail | 14-day retention; topic ACLs restricted to the platform |
| Bronze `customers_*` | raw names, e-mail | restricted schema; 90-day retention; not exposed to BI |
| Silver `customers` | first name, last-name initial, `email_masked` (`j***@domain`), `email_hash` (salted SHA-256) | salt from a secret (`PII_HASH_SALT`), rotated per environment |
| Gold / BI | masked fields only; `email_hash` is dropped by the Power BI publisher | |

All generated data is synthetic (Faker). E-mails use reserved `example.*` domains (RFC 2606), and no payment card data
exists anywhere. The data model stores only the payment method and status.

**Right to erasure (GDPR Art. 17) approach:** delete the customer in the operational DB. The change flows through
CDC / incremental extract. Run `DELETE ... WHERE customer_id = ?` on Bronze and Silver (Delta supports row-level deletes),
then `VACUUM` to purge the files after the retention window. Gold dimensions keep only masked attributes.

## Encryption
* At rest: S3 SSE-KMS with bucket keys; CloudWatch log groups and SNS encrypted with the same CMK; RDS / MSK encryption on.
* In transit: TLS enforced by bucket policy (`aws:SecureTransport`), SASL_SSL for Kafka, HTTPS for Databricks APIs.

## Audit logging
* S3 server access logs → dedicated log bucket (365-day expiry).
* Databricks / Unity Catalog audit logs (system tables `system.access.audit`).
* Pipeline audit trail: `meta.pipeline_runs`, `meta.file_registry`, `audit.dq_results`, Delta table history.
* Structured JSON application logs with correlation ids (CloudWatch log groups per pipeline).

## Data retention

| Data | Retention |
|---|---|
| Kafka topics | 3 days (clickstream), 7 days (transactions), 14 days (customers, refunds, returns), 30 days (dead letter) |
| Landing files | 30 days → Infrequent Access; expire after 180 days (prod) / 30 days (dev) |
| Bronze | 90 days (`delta_table_maintenance` retention task) |
| Silver | 2 years · Gold: 5 years (finance) · Audit: 1 year |
| Delta time travel | 7 days (deleted-file retention), 30 days of log history |

## Supply chain
* Pinned dependency versions; `pip-audit` and Gitleaks in CI; Ruff security rules (`S`).
* Containers run as non-root (`platform` uid 1000, `airflow` uid 50000).
* Images are built from pinned base tags. Recommended next step: pin digests and add an image scanner (Trivy) in CI.
