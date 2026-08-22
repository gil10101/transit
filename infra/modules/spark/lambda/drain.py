"""Start one EMR Serverless drain run. Fired by EventBridge Scheduler; exists because
the scheduler's universal target cannot supply a valid StartJobRun clientToken."""

import os
import uuid

import boto3


def handler(event, context):
    client = boto3.client("emr-serverless")
    run = client.start_job_run(
        applicationId=os.environ["APP_ID"],
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
