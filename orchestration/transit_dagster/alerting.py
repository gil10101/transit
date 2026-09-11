"""Run-failure alerting: the half of observability this pipeline was missing.

[rev 2026-08-25] Before this, detection existed and notification did not. The raw-feed
tripwire correctly failed every 15 minutes for a full day when Zurich was asserted on
before its poller existed, and nobody was told — it was found by hand. The only alert in
the entire system was an AWS *budget* email.

One channel, no per-job routing, no severity levels: a pipeline this size wants one
channel that is always worth reading; the moment alerts need filtering they stop being
read at all.

[rev 2026-09-10] Transition-only. The original "no dedup window" rule produced exactly
the fatigue it was meant to avoid: one six-hour TTC outage sent 22 byte-identical
emails, and the reader heard "the chain is failing" all afternoon. Now a job pages when
it starts failing or fails a NEW way (different error detail), stays silent on an
identical repeat, and sends one RECOVERED message on its first success after a failure
— so silence means "still the same incident", never "nobody is watching". The decision
is lib.alert_kind, unit-tested in the repo venv.

Fails soft on purpose. If the topic is unset, SNS rejects the publish, or the
previous-run lookup breaks, the sensor logs and carries on — a lookup failure pages
anyway rather than swallowing the alert, and nothing here may raise into a run.
"""

from __future__ import annotations

import os

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunsFilter,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)

from .lib import alert_body, alert_kind

TOPIC_ENV = "PIPELINE_ALERTS_TOPIC_ARN"


def _detail(event) -> str:
    """The error a human needs. A run-failure event carries the step's exception under
    event_specific_data.error; a reaped zombie carries only a message."""
    error = getattr(getattr(event, "event_specific_data", None), "error", None)
    return (getattr(error, "message", None) or getattr(event, "message", None) or "").strip()


def _previous(instance, run) -> tuple[str | None, str | None]:
    """Status and failure detail of this job's run immediately before `run`."""
    current = instance.get_run_record_by_id(run.run_id)
    records = instance.get_run_records(
        filters=RunsFilter(job_name=run.job_name, created_before=current.create_timestamp),
        limit=1,
    )
    if not records:
        return None, None
    prev = records[0].dagster_run
    if prev.status != DagsterRunStatus.FAILURE:
        return prev.status.value, None
    failures = instance.all_logs(prev.run_id, of_type=DagsterEventType.RUN_FAILURE)
    return prev.status.value, _detail(failures[-1].dagster_event) if failures else None


def _publish(context, subject: str, body: str) -> None:
    topic = os.environ.get(TOPIC_ENV)
    if not topic:
        context.log.warning(f"{TOPIC_ENV} unset — not sent: {subject}")
        return
    try:
        import boto3

        boto3.client("sns", region_name=os.environ.get("AWS_REGION", "us-east-2")).publish(
            TopicArn=topic,
            Subject=subject[:100],  # SNS caps Subject at 100
            Message=body,
        )
        context.log.info(f"alerted: {subject}")
    except Exception as exc:  # noqa: BLE001 — alerting must never fail the pipeline
        context.log.error(f"failed to publish alert: {exc!r}")


@run_failure_sensor(
    name="pipeline_failure_alert",
    default_status=DefaultSensorStatus.RUNNING,
    description="Publishes a run failure to SNS unless it repeats the previous failure verbatim.",
)
def pipeline_failure_alert(context: RunFailureSensorContext) -> None:
    run = context.dagster_run
    detail = _detail(context.failure_event)
    try:
        prev_status, prev_detail = _previous(context.instance, run)
    except Exception as exc:  # noqa: BLE001 — a lookup bug must not swallow the page
        context.log.error(f"previous-run lookup failed, alerting anyway: {exc!r}")
        prev_status, prev_detail = None, None
    if alert_kind(False, detail, prev_status, prev_detail) is None:
        context.log.info(f"suppressed repeat of an open failure: {run.job_name}")
        return
    _publish(
        context,
        f"transit-pulse FAILED: {run.job_name}",
        alert_body(run.run_id, run.job_name, detail),
    )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="pipeline_recovery_alert",
    default_status=DefaultSensorStatus.RUNNING,
    description="Publishes one RECOVERED message on a job's first success after a failure.",
)
def pipeline_recovery_alert(context: RunStatusSensorContext) -> None:
    run = context.dagster_run
    try:
        prev_status, prev_detail = _previous(context.instance, run)
    except Exception as exc:  # noqa: BLE001
        context.log.error(f"previous-run lookup failed, recovery not announced: {exc!r}")
        return
    if alert_kind(True, None, prev_status, prev_detail) == "recovered":
        _publish(
            context,
            f"transit-pulse RECOVERED: {run.job_name}",
            alert_body(
                run.run_id, run.job_name, f"recovered; the open failure was:\n{prev_detail}"
            ),
        )
