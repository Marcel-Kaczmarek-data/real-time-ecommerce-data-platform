# Optional Databricks Unity Catalog wiring (workspace-level provider):
# storage credential -> external location -> catalog with medallion schemas and grants.
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

variable "name_prefix" {
  type = string
}

variable "catalog_name" {
  type = string
}

variable "lakehouse_bucket" {
  type = string
}

variable "uc_role_arn" {
  type = string
}

variable "data_engineers_group" {
  type    = string
  default = "data-engineers"
}

variable "analysts_group" {
  type    = string
  default = "analysts"
}

resource "databricks_storage_credential" "lakehouse" {
  name = "${var.name_prefix}-lakehouse"
  aws_iam_role {
    role_arn = var.uc_role_arn
  }
  comment = "Managed by Terraform - lakehouse bucket access"
}

resource "databricks_external_location" "lakehouse" {
  name            = "${var.name_prefix}-lakehouse"
  url             = "s3://${var.lakehouse_bucket}/"
  credential_name = databricks_storage_credential.lakehouse.id
  comment         = "Medallion lakehouse root"
}

resource "databricks_catalog" "this" {
  name         = var.catalog_name
  storage_root = "s3://${var.lakehouse_bucket}/managed"
  comment      = "E-commerce data platform"
  depends_on   = [databricks_external_location.lakehouse]
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold", "audit", "quarantine"])
  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = "${each.key} layer"
}

# Engineers own all layers; analysts only read Gold (PII stays in Bronze/Silver).
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name
  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }
  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.this.name}.gold"
  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = var.data_engineers_group
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on = [databricks_schema.layers]
}

resource "databricks_cluster_policy" "jobs" {
  name = "${var.name_prefix}-job-clusters"
  definition = jsonencode({
    "spark_version"               = { type = "regex", pattern = "1[4-9]\\.[0-9]+\\.x-scala2\\.12" }
    "autotermination_minutes"     = { type = "fixed", value = 20, hidden = true }
    "custom_tags.project"         = { type = "fixed", value = "ecom-data-platform" }
    "aws_attributes.availability" = { type = "fixed", value = "SPOT_WITH_FALLBACK" }
    "autoscale.max_workers"       = { type = "range", maxValue = 8 }
    "data_security_mode"          = { type = "fixed", value = "SINGLE_USER" }
  })
}

output "catalog" {
  value = databricks_catalog.this.name
}

output "cluster_policy_id" {
  value = databricks_cluster_policy.jobs.id
}
