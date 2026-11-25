# Terraform (optional AWS deployment)

Nothing here runs automatically. CI only executes `terraform fmt -check` and `terraform validate`
(`terraform init -backend=false`), which create no resources.

## Layout
```
modules/
  kms/            customer-managed key (rotation on) for S3, CloudWatch, SNS
  s3_lakehouse/   landing + lakehouse + access-log buckets: SSE-KMS, versioning, TLS-only, lifecycle, logging
  iam/            Unity Catalog storage role (external id), ingestion writer, Gold read-only policy
  monitoring/     log groups + metric filters, S3 5xx alarm, SNS alerts, monthly budget
  databricks_uc/  storage credential, external location, catalog, medallion schemas, grants, cluster policy
environments/
  dev/            7-day KMS window, force_destroy, 30-day landing expiry, 50 USD budget, Databricks off
  prod/           30-day KMS window, no force_destroy, 180-day landing, 90-day logs, 1,500 USD budget, Databricks on
```

## Prerequisites
* Terraform ≥ 1.6, AWS credentials for a **non-production sandbox** account (`aws sts get-caller-identity`).
* Optional Databricks: workspace with Unity Catalog, `DATABRICKS_HOST` + `DATABRICKS_TOKEN` (or OAuth), account id.
* A remote state bucket + lock table (see the commented `backend "s3"` block).

## Usage
```bash
cd infrastructure/terraform/environments/dev
cp terraform.tfvars.example terraform.tfvars        # git-ignored; never commit account ids / e-mails
terraform init
terraform plan -out tfplan                          # review every resource
terraform apply tfplan                              # deliberate, manual step (creates billable resources)
```
Outputs (`lakehouse_bucket`, `unity_catalog_role_arn`, `databricks_catalog`) feed `config/cloud.yaml`
(`LAKEHOUSE_BUCKET`, `DATABRICKS_CATALOG`) and the Databricks bundle variables.

Costs and cleanup: [`docs/cost-analysis.md`](../../docs/cost-analysis.md). Security model: [`docs/security.md`](../../docs/security.md).
