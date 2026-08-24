# EMR Serverless Spark app for the streaming jobs, VPC-attached so it reaches Kafka.
# S3 flows through the VPC gateway endpoint; jars ship from the artifacts bucket
# (VPC ENIs have no internet, so no --packages at runtime).

variable "prefix" { type = string }
variable "subnet_ids" { type = list(string) }
variable "sg_id" { type = string }
variable "bucket_arns" { type = map(string) }
variable "release_label" {
  type    = string
  default = "emr-7.5.0"
}
variable "max_vcpu" {
  type = number
  # 8, not 4. Every job here is one 2-vCPU driver plus one 2-vCPU executor, so a
  # 4-vCPU ceiling admits TWO drivers and then has nothing left for either
  # executor: both jobs sit in RUNNING forever, re-requesting a worker that can
  # never be allocated ("Worker could not be allocated as the application has
  # exceeded maximumCapacity", 287 times in one 4.5-hour hung drain on
  # 2026-08-24) while every later submit is rejected. At 8 two full jobs fit, so
  # a scheduled drain and the weekly static parse overlap cleanly, and a third
  # submit is REJECTED at the API — a clean failure the retry path handles,
  # not a deadlock. Serverless bills used vCPU-hours, not the ceiling, so idle
  # cost is unchanged.
  default = 8
}

resource "aws_emrserverless_application" "streaming" {
  name          = "${var.prefix}-streaming"
  release_label = var.release_label
  type          = "SPARK"

  maximum_capacity {
    cpu    = "${var.max_vcpu} vCPU"
    memory = "${var.max_vcpu * 4} GB"
  }

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = [var.sg_id]
  }

  auto_start_configuration {
    enabled = true
  }
  # streaming jobs run continuously; stop the app when idle 15m (job gone/failed)
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

resource "aws_iam_role" "execution" {
  name = "${var.prefix}-emr-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "execution" {
  name = "lake-access"
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = flatten([for arn in values(var.bucket_arns) : [arn, "${arn}/*"]])
      }
    ]
  })
}

# --- scheduled micro-batch drain: EventBridge fires the invoker Lambda hourly.
# Continuous EMR Serverless streaming costs ~10x the plan's budget; availableNow
# drains reuse the same checkpoints for exactly-once with <=1 h silver lag, which
# still beats the 2-hour Dagster chain that rebuilds gold.

variable "artifacts_bucket" { type = string }
variable "lakehouse_bucket" { type = string }
variable "kafka_private_ip" { type = string }
variable "drain_schedule" {
  type = string
  # Hourly, not every 15 minutes. Each drain bills ~7 minutes of a 4-vCPU app to
  # move a few minutes of data — Spark start-up dominates, so cost tracks the
  # number of runs far more than the volume drained (measured 2026-08-24: 0.47
  # vCPU-hr per run, 96 runs/day ≈ $99/month, the entire budget). Nothing
  # downstream consumes 15-minute freshness: gold is rebuilt by the 2-hour
  # Dagster chain, and the killed-feed tripwire watches raw S3 prefixes, not
  # silver. Raising cadence past 1 hour would start to bite that chain.
  default = "rate(1 hour)"
}

resource "aws_iam_role" "scheduler" {
  name = "${var.prefix}-emr-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "start-job-run"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun"]
        Resource = "*"
      },
      {
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = aws_iam_role.execution.arn
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      }
    ]
  })
}

locals {
  # Single source for the EMR job-submit strings: the drain Lambda below and the
  # Dagster resources on the services box (P5) both consume exactly these values
  # (Dagster via module outputs -> services module user_data env). Staged by
  # scripts/submit_emr_drain.sh: entry.py (= run_local.py) and gtfs_static_parse.py.
  # P3: city -> IANA tz map injected into Spark (EMR python has no yaml, so
  # session.py can't read the city configs — quirk 3 in docs/operations.md).
  # Keep in sync with scripts/submit_emr_drain.sh CITY_TZS and the city yamls.
  city_tzs           = "nyc=America/New_York,boston=America/New_York,toronto=America/Toronto,helsinki=Europe/Helsinki,dc=America/New_York,sf=America/Los_Angeles,zurich=Europe/Zurich"
  entry_point        = "s3://${var.artifacts_bucket}/code/entry.py"
  static_entry_point = "s3://${var.artifacts_bucket}/code/gtfs_static_parse.py"
  log_uri            = "s3://${var.artifacts_bucket}/emr-logs/"
  jars = join(",", [
    "s3://${var.artifacts_bucket}/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar",
    "s3://${var.artifacts_bucket}/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar",
    "s3://${var.artifacts_bucket}/jars/kafka-clients-3.4.1.jar",
    "s3://${var.artifacts_bucket}/jars/commons-pool2-2.11.1.jar",
    "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
  ])
  spark_params = join(" ", [
    "--py-files s3://${var.artifacts_bucket}/code/spark_jobs.zip",
    "--jars ${local.jars}",
    "--conf spark.driver.cores=2",
    "--conf spark.driver.memory=6g",
    "--conf spark.executor.cores=2",
    # 10g x2, up from 6g x1. The dedup state store keeps 2 hours of keys in
    # executor memory and it roughly tripled when DC and SF came online (SF
    # alone carries ~78k stop updates per poll); the old sizing OOM-killed the
    # executor mid-shuffle (exit 137), lost the shuffle output, failed the stage
    # its 4 allowed times and killed the drain — with the backlog growing on
    # each death. 10g not 12g because Spark adds ~10% memory overhead per
    # worker: 6g driver + 2x12g asks ~33 GB against the app's 32 GB ceiling, so
    # the second executor is refused ("Worker could not be allocated") and the
    # job silently runs on one. 6.6 + 2x11 = 28.6 GB fits.
    "--conf spark.executor.memory=10g",
    "--conf spark.executor.instances=2",
    "--conf spark.dynamicAllocation.initialExecutors=1",
    "--conf spark.dynamicAllocation.maxExecutors=2",
    "--conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.lake.type=hadoop",
    "--conf spark.sql.catalog.lake.warehouse=s3://${var.lakehouse_bucket}/iceberg",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Keep 5 state-store versions, not the default 100. The dedup state for
    # silver_stop_time_predictions is ~100 MB per version across the shuffle
    # partitions, so retaining 100 of them grew the checkpoint to 13 GB — larger
    # than every data table in the lakehouse combined (measured 2026-08-24;
    # offsets/ and commits/ were correctly bounded, state/ was 23,686 files).
    # Recovery only ever needs the last committed version; 5 is head-room.
    # NB: shuffle-partition count is deliberately NOT changed — a stateful
    # streaming query cannot change it without discarding its checkpoint.
    "--conf spark.sql.streaming.minBatchesToRetain=5",
    "--conf spark.emr-serverless.driverEnv.TP_CLOUD=1",
    "--conf spark.emr-serverless.driverEnv.TP_TRIGGER=available_now",
    "--conf spark.emr-serverless.driverEnv.TP_CITY_TZS=${local.city_tzs}",
    "--conf spark.emr-serverless.driverEnv.TP_LAKE_URI=s3://${var.lakehouse_bucket}",
    # silver reads config/<city>_route_allowlist.csv from here to filter the
    # national feeds (Zurich) down to the scored city — driver only, the filter
    # is built on the driver and broadcast with the plan
    "--conf spark.emr-serverless.driverEnv.ARTIFACTS_BUCKET=${var.artifacts_bucket}",
    "--conf spark.emr-serverless.driverEnv.KAFKA_BOOTSTRAP=${var.kafka_private_ip}:9092",
    "--conf spark.executorEnv.TP_CLOUD=1",
    "--conf spark.executorEnv.TP_TRIGGER=available_now",
    "--conf spark.executorEnv.TP_CITY_TZS=${local.city_tzs}",
    "--conf spark.executorEnv.TP_LAKE_URI=s3://${var.lakehouse_bucket}",
    "--conf spark.executorEnv.KAFKA_BOOTSTRAP=${var.kafka_private_ip}:9092",
  ])
}

resource "aws_sqs_queue" "drain_dlq" {
  name = "${var.prefix}-emr-drain-dlq"
}

resource "aws_iam_role_policy" "scheduler_dlq" {
  name = "dlq"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = aws_sqs_queue.drain_dlq.arn
    }]
  })
}

data "archive_file" "drain_lambda" {
  type        = "zip"
  source_file = "${path.module}/lambda/drain.py"
  output_path = "${path.module}/lambda/drain.zip"
}

resource "aws_iam_role" "drain_lambda" {
  name = "${var.prefix}-drain-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "drain_lambda" {
  name = "start-job-run"
  role = aws_iam_role.drain_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        # ListJobRuns so the handler can skip a fire while a drain is still in
        # flight — overlapping runs share one Spark checkpoint, and Structured
        # Streaming assumes a single writer per checkpoint location
        Action   = ["emr-serverless:StartJobRun", "emr-serverless:ListJobRuns"]
        Resource = "*"
      },
      {
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = aws_iam_role.execution.arn
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "drain" {
  function_name    = "${var.prefix}-emr-drain"
  role             = aws_iam_role.drain_lambda.arn
  handler          = "drain.handler"
  runtime          = "python3.12"
  timeout          = 30
  filename         = data.archive_file.drain_lambda.output_path
  source_code_hash = data.archive_file.drain_lambda.output_base64sha256
  environment {
    variables = {
      APP_ID        = aws_emrserverless_application.streaming.id
      EXEC_ROLE_ARN = aws_iam_role.execution.arn
      ENTRY_POINT   = local.entry_point
      SPARK_PARAMS  = local.spark_params
      LOG_URI       = local.log_uri
    }
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name = "invoke-drain-lambda"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = aws_lambda_function.drain.arn
    }]
  })
}

resource "aws_scheduler_schedule" "drain" {
  name                = "${var.prefix}-emr-drain"
  schedule_expression = var.drain_schedule
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.drain.arn
    role_arn = aws_iam_role.scheduler.arn
    dead_letter_config {
      arn = aws_sqs_queue.drain_dlq.arn
    }
    retry_policy {
      maximum_retry_attempts = 1
    }
    input = jsonencode({})
  }
}

output "application_id" { value = aws_emrserverless_application.streaming.id }
output "execution_role_arn" { value = aws_iam_role.execution.arn }
output "entry_point" { value = local.entry_point }
output "static_entry_point" { value = local.static_entry_point }
output "spark_params" { value = local.spark_params }
output "log_uri" { value = local.log_uri }
