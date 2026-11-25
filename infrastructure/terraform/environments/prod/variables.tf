variable "environment" {
  type    = string
  default = "prod"
}

variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

variable "owner" {
  type    = string
  default = "data-platform-team"
}

variable "kms_deletion_window_days" {
  type    = number
  default = 30
}

variable "landing_expiration_days" {
  type    = number
  default = 180
}

variable "force_destroy" {
  type        = bool
  default     = false
  description = "Production buckets are never force-destroyed."
}

variable "databricks_account_id" {
  type      = string
  default   = ""
  sensitive = true
}

variable "ingestion_principal_arns" {
  type    = list(string)
  default = []
}

variable "alert_email" {
  type    = string
  default = ""
}

variable "log_retention_days" {
  type    = number
  default = 90
}

variable "monthly_budget_usd" {
  type    = number
  default = 1500
}

variable "enable_databricks" {
  type        = bool
  default     = true
  description = "Create Unity Catalog storage credential, external location, catalog and schemas."
}
