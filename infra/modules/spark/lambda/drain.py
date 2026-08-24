"""Start one EMR Serverless drain run. Fired by EventBridge Scheduler; exists because
the scheduler's universal target cannot supply a valid StartJobRun clientToken."""

import os
import uuid

import boto3

ACTIVE_STATES = ["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING"]


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
        clientToken=str(uuid.uuid4()),
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
