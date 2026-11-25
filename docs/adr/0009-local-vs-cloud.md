# ADR 0009 — Local versus cloud deployment

**Status:** Accepted

## Decision
Local-first. The complete platform (Airflow, monitoring and BI included) runs
with Docker Compose and MinIO at no cost. AWS (S3, IAM, KMS and CloudWatch,
via Terraform) and Databricks (Asset Bundle jobs) form an **optional** target.
It uses the same Python modules and the same configuration schema (`config/cloud.yaml`).

## Consequences
* Reviewers can run everything with `make demo`.
* Some features differ by runtime (see the ADR 0003 table). Code branches on
  `is_databricks()` rather than keeping two implementations.
* Nothing in this repository provisions paid resources automatically.
  CI only runs `terraform validate`.
