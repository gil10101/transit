# Cost guardrail: monthly budget with email alerts at 80% actual and 100% forecast.

variable "monthly_limit_usd" {
  type    = number
  default = 50
}
variable "alert_email" { type = string }

resource "aws_budgets_budget" "monthly" {
  name         = "transit-pulse-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# --- Pipeline health alerting -------------------------------------------------
# [rev 2026-08-25] Until now the ONLY alert in the whole system was the budget email
# above: detection existed (the Dagster freshness tripwire works) but notification did
# not. The proof is that raw_feed_freshness_job failed every 15 minutes for a full day
# and was found by hand, not by anyone being told. This topic is the missing half.
#
# Email subscriptions are NOT auto-confirmed by AWS — the address gets a confirmation
# link and the subscription stays "pending confirmation" until someone clicks it. An
# unconfirmed subscription silently drops every message, so confirm it and then verify
# with `aws sns list-subscriptions-by-topic`.
resource "aws_sns_topic" "pipeline_alerts" {
  name = "transit-pulse-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "pipeline_alerts_email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

output "pipeline_alerts_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}
