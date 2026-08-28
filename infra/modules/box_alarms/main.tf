# External watchdog for the EC2 boxes, publishing to the pipeline-alerts topic.
#
# The Dagster run-failure sensor covers everything Dagster can see, which is
# exactly its blind spot: a box sick enough that runs never start emits no
# failure event. On 2026-08-28 two overlapping warehouse_chain runs memory-
# thrashed the services box — SSM agent and all pollers died at 18:06Z and the
# only signal was silence. These alarms watch from outside the box.
#
# This module lives separately from `monitoring` as an organizational choice:
# monitoring is account-level (budget, topic), these are per-instance. (An
# earlier revision claimed mutual module references would be a terraform
# cycle — false: terraform graphs at resource level, and topic -> instance ->
# alarm is a DAG. Kept separate anyway; the reason is tidiness, not necessity.)
#
# [import] All three alarms were created live via `aws cloudwatch
# put-metric-alarm` on 2026-08-28 with these exact names. Before the next
# apply, import them:
#   terraform import 'module.box_alarms.aws_cloudwatch_metric_alarm.services_cpu_wedge' transit-pulse-services-cpu-wedge
#   terraform import 'module.box_alarms.aws_cloudwatch_metric_alarm.services_status_check' transit-pulse-services-status-check
#   terraform import 'module.box_alarms.aws_cloudwatch_metric_alarm.kafka_status_check' transit-pulse-kafka-status-check

variable "prefix" { type = string }
variable "topic_arn" { type = string }
variable "services_instance_id" { type = string }
variable "kafka_instance_id" { type = string }

# Sustained CPU is the wedge signature: a normal chain's dbt phase spikes for
# ~15 min (3 datapoints); 30 min pinned means nothing is finishing.
resource "aws_cloudwatch_metric_alarm" "services_cpu_wedge" {
  alarm_name          = "${var.prefix}-services-cpu-wedge"
  alarm_description   = "services box CPU pinned 30+ min = wedge (2026-08-28 class); normal chain spikes are shorter"
  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  dimensions          = { InstanceId = var.services_instance_id }
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 6
  datapoints_to_alarm = 6
  threshold           = 55
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.topic_arn]
  ok_actions          = [var.topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "services_status_check" {
  alarm_name          = "${var.prefix}-services-status-check"
  alarm_description   = "services box failed EC2 status checks"
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed"
  dimensions          = { InstanceId = var.services_instance_id }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  # Missing data here means the instance is stopped, terminated, or too
  # dead to emit metrics — the highest-consequence states. notBreaching
  # would go silent exactly then (review finding, 2026-08-28).
  treat_missing_data  = "breaching"
  alarm_actions       = [var.topic_arn]
  ok_actions          = [var.topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "kafka_status_check" {
  alarm_name          = "${var.prefix}-kafka-status-check"
  alarm_description   = "kafka box failed EC2 status checks"
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed"
  dimensions          = { InstanceId = var.kafka_instance_id }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  # Missing data here means the instance is stopped, terminated, or too
  # dead to emit metrics — the highest-consequence states. notBreaching
  # would go silent exactly then (review finding, 2026-08-28).
  treat_missing_data  = "breaching"
  alarm_actions       = [var.topic_arn]
  ok_actions          = [var.topic_arn]
}
