output "landing_bucket" {
  value = module.storage.landing_bucket
}

output "lakehouse_bucket" {
  value = module.storage.lakehouse_bucket
}

output "kms_key_arn" {
  value = module.kms.key_arn
}

output "unity_catalog_role_arn" {
  value = module.iam.unity_catalog_role_arn
}

output "alerts_topic_arn" {
  value = module.monitoring.alerts_topic_arn
}

output "databricks_catalog" {
  value = var.enable_databricks ? module.databricks[0].catalog : null
}
