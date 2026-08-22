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

output "application_id" { value = aws_emrserverless_application.streaming.id }
output "execution_role_arn" { value = aws_iam_role.execution.arn }
