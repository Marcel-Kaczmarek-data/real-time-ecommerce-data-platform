variable "name_prefix" {
  type        = string
  description = "Prefix for resource names, e.g. ecom-dp-dev"
}

variable "deletion_window_days" {
  type        = number
  default     = 30
  description = "KMS key deletion window (7-30 days)"
}

variable "tags" {
  type    = map(string)
  default = {}
}

data "aws_caller_identity" "current" {}

resource "aws_kms_key" "lakehouse" {
  description             = "${var.name_prefix} lakehouse encryption key (S3 SSE-KMS, CloudWatch logs)"
  deletion_window_in_days = var.deletion_window_days
  enable_key_rotation     = true
  tags                    = var.tags

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AccountAdministration"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowCloudWatchLogs"
        Effect    = "Allow"
        Principal = { Service = "logs.amazonaws.com" }
        Action    = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
        Resource  = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${var.name_prefix}-lakehouse"
  target_key_id = aws_kms_key.lakehouse.key_id
}

output "key_arn" {
  value = aws_kms_key.lakehouse.arn
}

output "key_id" {
  value = aws_kms_key.lakehouse.key_id
}
