"""Start one EMR Serverless drain run. Fired by EventBridge Scheduler; exists because
the scheduler's universal target cannot supply a valid StartJobRun clientToken."""

import os
import uuid

import boto3

ACTIVE_STATES = ["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING"]


def handler(event, context):
    client = boto3.client("emr-serverless")
    app_id = os.environ["APP_ID"]

    # Skip when a drain is already in flight. Every drain writes the same Spark
    # checkpoint, and Structured Streaming assumes a single writer per
    # checkpoint location — overlapping runs risk corrupting the offset log,
    # which is far worse than a skipped cycle (the next run reads from the same
    # committed offset and catches up on its own). Overlap used to be routine:
    # a slow catch-up drain would still be running when the next fire landed.
    active = client.list_job_runs(applicationId=app_id, states=ACTIVE_STATES)["jobRuns"]
    busy = [r["id"] for r in active if r.get("name") == "transit-drain"]
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
