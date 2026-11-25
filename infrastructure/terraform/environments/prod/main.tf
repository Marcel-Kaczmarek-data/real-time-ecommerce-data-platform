# PROD environment. Nothing is applied automatically: run `terraform plan` and
# review it; `apply` is a deliberate manual step (see infrastructure/terraform/README.md).
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }
  # Remote state (create the bucket / table once, then uncomment):
  # backend "s3" {
  #   bucket         = "<org>-terraform-state"
  #   key            = "ecom-data-platform/prod/terraform.tfstate"
  #   region         = "eu-central-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.tags
  }
}

provider "databricks" {
  # Configured via DATABRICKS_HOST / DATABRICKS_TOKEN (or OAuth) environment variables.
}

locals {
  name_prefix = "ecom-dp-${var.environment}"
  tags = {
    project     = "ecom-data-platform"
    environment = var.environment
    managed_by  = "terraform"
    owner       = var.owner
  }
}

module "kms" {
  source               = "../../modules/kms"
  name_prefix          = local.name_prefix
  deletion_window_days = var.kms_deletion_window_days
  tags                 = local.tags
}

module "storage" {
  source                  = "../../modules/s3_lakehouse"
  name_prefix             = local.name_prefix
  kms_key_arn             = module.kms.key_arn
  landing_expiration_days = var.landing_expiration_days
  force_destroy           = var.force_destroy
  tags                    = local.tags
}

module "iam" {
  source                   = "../../modules/iam"
  name_prefix              = local.name_prefix
  lakehouse_bucket_arn     = module.storage.bucket_arns["lakehouse"]
  landing_bucket_arn       = module.storage.bucket_arns["landing"]
  kms_key_arn              = module.kms.key_arn
  databricks_account_id    = var.databricks_account_id
  ingestion_principal_arns = var.ingestion_principal_arns
  tags                     = local.tags
}

module "monitoring" {
  source             = "../../modules/monitoring"
  name_prefix        = local.name_prefix
  kms_key_arn        = module.kms.key_arn
  lakehouse_bucket   = module.storage.lakehouse_bucket
  alert_email        = var.alert_email
  log_retention_days = var.log_retention_days
  monthly_budget_usd = var.monthly_budget_usd
  tags               = local.tags
}

module "databricks" {
  count            = var.enable_databricks ? 1 : 0
  source           = "../../modules/databricks_uc"
  name_prefix      = local.name_prefix
  catalog_name     = "ecommerce_${var.environment}"
  lakehouse_bucket = module.storage.lakehouse_bucket
  uc_role_arn      = module.iam.unity_catalog_role_arn
}
