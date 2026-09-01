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
#   terraform import 'module.box_alarms.aws_cloudwatch_metric_alarm.services_autoreboot' transit-pulse-services-autoreboot

variable "prefix" { type = string }
variable "topic_arn" { type = string }
variable "services_instance_id" { type = string }
variable "kafka_instance_id" { type = string }

# Sustained CPU is the wedge signature: a normal chain's dbt phase spikes for
# ~15 min (3 datapoints); 30 min pinned means nothing is finishing.
resource "aws_cloudwatch_metric_alarm" "services_cpu_wedge" {
  alarm_name          = "${var.prefix}-services-cpu-wedge"
  # [rev 2026-09-01] 55%/30min flapped four times in one afternoon: steady state
  # under a heavy drain + rebuild is 51-64%, so the threshold sat inside normal
  # operation. A flapping alarm is worse than none — it teaches the reader to
  # ignore the channel. 75% for 45 min is clear of measured normal work.
  alarm_description   = "services box CPU pinned 45+ min (2026-08-28 wedge class); normal heavy work measured 51-64%"
  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  dimensions          = { InstanceId = var.services_instance_id }
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 9
  datapoints_to_alarm = 9
  threshold           = 75
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

# Self-healing. Both box wedges (2026-08-28, 2026-09-01) left the instance
# "impaired" with a dead SSM agent, every poller stopped, and no way back except
# a human running reboot-instances — 6h of unrecoverable feed loss the second
# time. Unattended for a month that is days, not hours. EC2's built-in reboot
# action needs no agent on the box, which is the whole point: it works precisely
# when the box cannot help itself. Paired with an SNS notify so a reboot is never
# silent. StatusCheckFailed_INSTANCE (OS-level) is the right metric for reboot;
# a SYSTEM-level failure would need `recover` instead and is AWS's to fix.
resource "aws_cloudwatch_metric_alarm" "services_autoreboot" {
  alarm_name          = "${var.prefix}-services-autoreboot"
  alarm_description   = "Instance status check failed 10 min -> reboot automatically"
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_Instance"
  dimensions          = { InstanceId = var.services_instance_id }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  # missing = the box is not reporting at all; the paging status_check alarm
  # above owns that case, this one must not reboot on absent data
  treat_missing_data = "missing"
  alarm_actions = [
    "arn:aws:automate:${data.aws_region.current.region}:ec2:reboot",
    var.topic_arn,
  ]
}

data "aws_region" "current" {}
