"""Start one EMR Serverless drain run. Fired by EventBridge Scheduler; exists because
the scheduler's universal target cannot supply a valid StartJobRun clientToken."""

import hashlib
import os
from datetime import datetime, timezone

import boto3

# Every NON-TERMINAL state, verified 2026-08-25 against the API's own enum (an invalid
# value returns: SUCCESS, SCHEDULED, CANCELLING, CANCELLED, QUEUED, PENDING, SUBMITTED,
# FAILED, RUNNING). Terminal = SUCCESS, FAILED, CANCELLED; everything else still holds
# the Spark checkpoint.
#
# [rev 2026-08-25] QUEUED and CANCELLING were missing. CANCELLING is demonstrably
# reachable — EmrResource cancels on timeout and 8 of the last 50 runs are CANCELLED —
# and a run mid-cancel still owns the checkpoint. Missing either state means `busy` reads
# empty and a second drain starts, which is precisely the overlap that killed a query
# with CONCURRENT_STREAM_LOG_UPDATE and destroyed 58 minutes of drained data on
# 2026-08-24. Keep this list in step with lib.EMR_ACTIVE_STATES (the Dagster side).
ACTIVE_STATES = ["SUBMITTED", "PENDING", "QUEUED", "SCHEDULED", "RUNNING", "CANCELLING"]


def _client_token() -> str:
    """Stable within a minute, so an EventBridge retry of the SAME fire deduplicates.

    [rev 2026-08-25] Was `str(uuid.uuid4())` — a fresh random per invocation, which gives
    StartJobRun nothing to deduplicate on. The schedule sets maximumRetryAttempts=1 with a
    DLQ, so retries genuinely happen, and this Lambda's stated reason to exist is that the
    scheduler cannot supply a valid clientToken. It was supplying one that did nothing.

    A minute bucket is the right granularity: the schedule fires hourly, so two distinct
    fires can never share a bucket, while a retry of one fire (seconds later) always does.
    """
    bucket = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")  # noqa: UP017 — py3.9 lambda
    return hashlib.sha256(f"transit-drain|{bucket}".encode()).hexdigest()[:64]


def handler(event, context):
    client = boto3.client("emr-serverless")
    app_id = os.environ["APP_ID"]

    # Skip when ANY drain is already in flight. Every drain writes the same
    # Spark checkpoint and Structured Streaming permits one writer per
    # checkpoint location, so an overlap kills a query outright
    # (CONCURRENT_STREAM_LOG_UPDATE) — far worse than a skipped cycle, since the
    # next run resumes from the same committed offset and catches up by itself.
    # The prefix match matters: Dagster's 2-hour chain submits the same work as
    # "transit-drain-dagster", and matching the exact name let that pair run
    # together on 2026-08-24 and destroy 58 minutes of drained data.
    active = client.list_job_runs(applicationId=app_id, states=ACTIVE_STATES)["jobRuns"]
    busy = [r["id"] for r in active if str(r.get("name", "")).startswith("transit-drain")]
    if busy:
        return {"skipped": True, "reason": "drain already in flight", "activeRunIds": busy}

    run = client.start_job_run(
        applicationId=app_id,
        executionRoleArn=os.environ["EXEC_ROLE_ARN"],
        clientToken=_client_token(),
        name="transit-drain",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": os.environ["ENTRY_POINT"],
                "sparkSubmitParameters": os.environ["SPARK_PARAMS"],
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": os.environ["LOG_URI"]}
            }
        },
    )
    return {"jobRunId": run["jobRunId"]}
