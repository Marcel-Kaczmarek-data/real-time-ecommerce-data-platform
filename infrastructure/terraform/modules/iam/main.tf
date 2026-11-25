# Least-privilege IAM:
#   * unity_catalog_access - assumed by Databricks Unity Catalog (storage credential) for the lakehouse bucket
#   * ingestion_writer     - write-only to landing/, used by producers / batch extractors outside Databricks
#   * bi_reader            - read-only on gold/ (e.g. Athena / external BI) - no Bronze/Silver access

variable "name_prefix" {
  type = string
}

variable "lakehouse_bucket_arn" {
  type = string
}

variable "landing_bucket_arn" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "databricks_account_id" {
  type        = string
  default     = ""
  description = "Databricks account id; used as sts:ExternalId for the Unity Catalog role."
}

variable "unity_catalog_master_role_arn" {
  type        = string
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
  description = "Databricks-owned Unity Catalog master role (see Databricks docs for your region / cloud)."
}

variable "ingestion_principal_arns" {
  type        = list(string)
  default     = []
  description = "Principals allowed to assume the ingestion writer role."
}

variable "tags" {
  type    = map(string)
  default = {}
}

data "aws_caller_identity" "current" {}

locals {
  kms_use = {
    Effect   = "Allow"
    Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
    Resource = [var.kms_key_arn]
  }
}

# --------------------------------------------------- Unity Catalog storage
resource "aws_iam_role" "unity_catalog_access" {
  name = "${var.name_prefix}-uc-lakehouse-access"
  tags = var.tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = [var.unity_catalog_master_role_arn] }
        Action    = "sts:AssumeRole"
        Condition = { StringEquals = { "sts:ExternalId" = var.databricks_account_id } }
      },
      {
        # Self-assume is required by Unity Catalog storage credentials.
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "sts:AssumeRole"
        Condition = {
          ArnEquals = { "aws:PrincipalArn" = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.name_prefix}-uc-lakehouse-access" }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "unity_catalog_access" {
  name = "lakehouse-read-write"
  role = aws_iam_role.unity_catalog_access.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${var.lakehouse_bucket_arn}/*", "${var.landing_bucket_arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [var.lakehouse_bucket_arn, var.landing_bucket_arn]
      },
      local.kms_use
    ]
  })
}

# ------------------------------------------------------ ingestion writer
resource "aws_iam_role" "ingestion_writer" {
  count = length(var.ingestion_principal_arns) > 0 ? 1 : 0
  name  = "${var.name_prefix}-ingestion-writer"
  tags  = var.tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.ingestion_principal_arns }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ingestion_writer" {
  count = length(var.ingestion_principal_arns) > 0 ? 1 : 0
  name  = "landing-write-only"
  role  = aws_iam_role.ingestion_writer[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = ["${var.landing_bucket_arn}/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = [var.landing_bucket_arn] },
      merge(local.kms_use, { Action = ["kms:Encrypt", "kms:GenerateDataKey*"] })
    ]
  })
}

# ------------------------------------------------------------- BI reader
resource "aws_iam_policy" "bi_reader" {
  name        = "${var.name_prefix}-gold-read-only"
  description = "Read-only access to the Gold layer (no Bronze / Silver PII)."
  tags        = var.tags
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${var.lakehouse_bucket_arn}/gold/*"] },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [var.lakehouse_bucket_arn]
        Condition = { StringLike = { "s3:prefix" = ["gold/*"] } }
      },
      merge(local.kms_use, { Action = ["kms:Decrypt", "kms:DescribeKey"] })
    ]
  })
}

output "unity_catalog_role_arn" {
  value = aws_iam_role.unity_catalog_access.arn
}

output "bi_reader_policy_arn" {
  value = aws_iam_policy.bi_reader.arn
}
