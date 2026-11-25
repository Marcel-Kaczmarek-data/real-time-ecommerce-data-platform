# CloudWatch log groups, an SNS alert topic, S3 error alarms and a cost budget.

variable "name_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "lakehouse_bucket" {
  type = string
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional e-mail subscription for alerts (requires confirmation)."
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "monthly_budget_usd" {
  type        = number
  default     = 100
  description = "Monthly AWS budget; alerts at 80% (forecast) and 100% (actual)."
}

variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_sns_topic" "alerts" {
  name              = "${var.name_prefix}-data-platform-alerts"
  kms_master_key_id = var.kms_key_arn
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_group" "pipelines" {
  for_each          = toset(["ingestion", "bronze-streaming", "silver", "gold", "data-quality", "maintenance"])
  name              = "/ecom-data-platform/${var.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

# Request metrics must be enabled on the bucket for 4xx/5xx alarms.
resource "aws_s3_bucket_metric" "lakehouse" {
  bucket = var.lakehouse_bucket
  name   = "EntireBucket"
}

resource "aws_cloudwatch_metric_alarm" "s3_5xx" {
  alarm_name          = "${var.name_prefix}-lakehouse-s3-5xx"
  alarm_description   = "Server errors from the lakehouse bucket (storage interruption)."
  namespace           = "AWS/S3"
  metric_name         = "5xxErrors"
  dimensions          = { BucketName = var.lakehouse_bucket, FilterId = aws_s3_bucket_metric.lakehouse.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 10
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_metric_alarm" "pipeline_errors" {
  alarm_name          = "${var.name_prefix}-pipeline-error-logs"
  alarm_description   = "ERROR-level log lines emitted by the pipelines (structured JSON logs)."
  namespace           = "EcomDataPlatform"
  metric_name         = "PipelineErrors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_log_metric_filter" "errors" {
  for_each       = aws_cloudwatch_log_group.pipelines
  name           = "${var.name_prefix}-${each.key}-errors"
  log_group_name = each.value.name
  pattern        = "{ $.level = \"ERROR\" }"
  metric_transformation {
    name      = "PipelineErrors"
    namespace = "EcomDataPlatform"
    value     = "1"
  }
}

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$ecom-data-platform"]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
