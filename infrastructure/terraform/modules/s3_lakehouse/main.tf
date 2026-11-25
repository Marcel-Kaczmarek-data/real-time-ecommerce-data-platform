variable "name_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "landing_expiration_days" {
  type        = number
  default     = 180
  description = "Raw landing files are deleted after this many days (Bronze keeps the data)."
}

variable "noncurrent_version_expiration_days" {
  type    = number
  default = 30
}

variable "force_destroy" {
  type        = bool
  default     = false
  description = "Allow terraform destroy to delete non-empty buckets (dev only)."
}

variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  buckets = {
    landing   = "${var.name_prefix}-landing"
    lakehouse = "${var.name_prefix}-lakehouse"
  }
}

# ----------------------------------------------------------------- access logs
resource "aws_s3_bucket" "logs" {
  bucket        = "${var.name_prefix}-access-logs"
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { purpose = "s3-access-logs" })
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # S3 server access logging does not support SSE-KMS targets
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "expire-access-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = 365
    }
  }
}

# ---------------------------------------------------------- data buckets
resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { layer = each.key })
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_logging" "this" {
  for_each      = aws_s3_bucket.this
  bucket        = each.value.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "${each.key}/"
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.this]
}

# Landing: raw partner files are transient once loaded into Bronze.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.this["landing"].id
  rule {
    id     = "tier-and-expire-raw-files"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = var.landing_expiration_days
    }
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Lakehouse: Delta manages live files (VACUUM); expire old versions of deleted objects.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.this["lakehouse"].id
  rule {
    id     = "bronze-intelligent-tiering"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }
  rule {
    id     = "noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "landing_bucket" {
  value = aws_s3_bucket.this["landing"].bucket
}

output "lakehouse_bucket" {
  value = aws_s3_bucket.this["lakehouse"].bucket
}

output "bucket_arns" {
  value = { for k, b in aws_s3_bucket.this : k => b.arn }
}
