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
  type    = number
  default = 4
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

# --- scheduled micro-batch drain: EventBridge fires StartJobRun every 15 min.
# Continuous EMR Serverless streaming costs ~10x the plan's budget; availableNow
# drains reuse the same checkpoints for exactly-once with <=15 min silver lag.

variable "artifacts_bucket" { type = string }
variable "lakehouse_bucket" { type = string }
variable "kafka_private_ip" { type = string }
variable "drain_schedule" {
  type    = string
  default = "rate(15 minutes)"
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
    "--conf spark.executor.memory=6g",
    "--conf spark.executor.instances=1",
    "--conf spark.dynamicAllocation.initialExecutors=1",
    "--conf spark.dynamicAllocation.maxExecutors=1",
    "--conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.lake.type=hadoop",
    "--conf spark.sql.catalog.lake.warehouse=s3://${var.lakehouse_bucket}/iceberg",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.emr-serverless.driverEnv.TP_CLOUD=1",
    "--conf spark.emr-serverless.driverEnv.TP_TRIGGER=available_now",
    "--conf spark.emr-serverless.driverEnv.TP_CITY_TZS=nyc=America/New_York",
    "--conf spark.emr-serverless.driverEnv.TP_LAKE_URI=s3://${var.lakehouse_bucket}",
    "--conf spark.emr-serverless.driverEnv.KAFKA_BOOTSTRAP=${var.kafka_private_ip}:9092",
    "--conf spark.executorEnv.TP_CLOUD=1",
    "--conf spark.executorEnv.TP_TRIGGER=available_now",
    "--conf spark.executorEnv.TP_CITY_TZS=nyc=America/New_York",
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

resource "aws_scheduler_schedule" "drain" {
  name                = "${var.prefix}-emr-drain"
  schedule_expression = var.drain_schedule
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = "arn:aws:scheduler:::aws-sdk:emrserverless:startJobRun"
    role_arn = aws_iam_role.scheduler.arn
    dead_letter_config {
      arn = aws_sqs_queue.drain_dlq.arn
    }
    retry_policy {
      maximum_retry_attempts = 1
    }
    input = jsonencode({
      ClientToken      = "<aws.scheduler.execution-id>"
      ApplicationId    = aws_emrserverless_application.streaming.id
      ExecutionRoleArn = aws_iam_role.execution.arn
      Name             = "transit-drain"
      JobDriver = {
        SparkSubmit = {
          EntryPoint            = "s3://${var.artifacts_bucket}/code/entry.py"
          SparkSubmitParameters = local.spark_params
        }
      }
      ConfigurationOverrides = {
        MonitoringConfiguration = {
          S3MonitoringConfiguration = { LogUri = "s3://${var.artifacts_bucket}/emr-logs/" }
        }
      }
    })
  }
}

output "application_id" { value = aws_emrserverless_application.streaming.id }
output "execution_role_arn" { value = aws_iam_role.execution.arn }
