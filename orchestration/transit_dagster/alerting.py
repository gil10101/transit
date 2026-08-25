"""Run-failure alerting: the half of observability this pipeline was missing.

[rev 2026-08-25] Before this, detection existed and notification did not. The raw-feed
tripwire correctly failed every 15 minutes for a full day when Zurich was asserted on
before its poller existed, and nobody was told — it was found by hand. The only alert in
the entire system was an AWS *budget* email.

This sensor fires on ANY Dagster run failure and publishes one short message to SNS.
Deliberately simple: no per-job routing, no severity levels, no dedup window. A pipeline
this size wants one channel that is always worth reading; the moment alerts need
filtering they stop being read at all.

Fails soft on purpose. If the topic is unset or SNS rejects the publish, the sensor logs
and returns rather than raising — an alerting failure must never cascade into a run
failure, which would then try to alert about itself.

The message shape lives in lib.alert_body so it is unit-testable in the repo venv, which
deliberately has no dagster installed.
"""

from __future__ import annotations

import os

from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from .lib import alert_body

TOPIC_ENV = "PIPELINE_ALERTS_TOPIC_ARN"


@run_failure_sensor(
    name="pipeline_failure_alert",
    default_status=DefaultSensorStatus.RUNNING,
    description="Publishes every Dagster run failure to the pipeline-alerts SNS topic.",
)
def pipeline_failure_alert(context: RunFailureSensorContext) -> None:
    topic = os.environ.get(TOPIC_ENV)
    if not topic:
        context.log.warning(f"{TOPIC_ENV} unset — run failure not alerted")
        return

    run = context.dagster_run
    job_name = run.job_name
    error = getattr(context.failure_event, "message", None) or str(
        context.failure_event.event_specific_data
    )

    try:
        import boto3

        boto3.client("sns", region_name=os.environ.get("AWS_REGION", "us-east-2")).publish(
            TopicArn=topic,
            Subject=f"transit-pulse FAILED: {job_name}"[:100],  # SNS caps Subject at 100
            Message=alert_body(run.run_id, job_name, error),
        )
        context.log.info(f"alerted run failure for {job_name} to SNS")
    except Exception as exc:  # noqa: BLE001 — alerting must never fail the pipeline
        context.log.error(f"failed to publish run-failure alert: {exc!r}")
