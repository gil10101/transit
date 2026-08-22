# Snowflake: warehouse, database, schemas, transform role, and the S3 external volume
# for Iceberg-over-silver. External-volume trust is a two-step dance:
#   apply 1 -> external volume created; DESC EXTERNAL VOLUME yields Snowflake's IAM user
#   ARN + external id; set snowflake_iam_user_arn / snowflake_external_id vars
#   apply 2 -> IAM trust narrows from placeholder to Snowflake's principal.

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0"
    }
    aws = {
      source = "hashicorp/aws"
    }
  }
}

variable "prefix" { type = string }
variable "lakehouse_bucket" { type = string }
variable "lakehouse_bucket_arn" { type = string }
variable "account_id" { type = string }
variable "snowflake_iam_user_arn" {
  type    = string
  default = ""
}
variable "snowflake_external_id" {
  type    = string
  default = ""
}

# --- AWS side: role Snowflake assumes to read the lakehouse ---

resource "aws_iam_role" "snowflake_reader" {
  name = "${var.prefix}-snowflake-reader"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        AWS = var.snowflake_iam_user_arn != "" ? var.snowflake_iam_user_arn : "arn:aws:iam::${var.account_id}:root"
      }
      Condition = var.snowflake_external_id != "" ? {
        StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
      } : {}
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_reader" {
  name = "lakehouse-read"
  role = aws_iam_role.snowflake_reader.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [var.lakehouse_bucket_arn, "${var.lakehouse_bucket_arn}/*"]
      }
    ]
  })
}

# --- Snowflake side ---

resource "snowflake_warehouse" "transform" {
  name           = "TRANSFORM_XS"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_database" "transit" {
  name = "TRANSIT"
}

resource "snowflake_schema" "silver_ext" {
  database = snowflake_database.transit.name
  name     = "SILVER"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.transit.name
  name     = "GOLD"
}

resource "snowflake_external_volume" "lakehouse" {
  name = "TRANSIT_LAKEHOUSE"
  storage_location {
    storage_location_name = "lakehouse-us-east-1"
    storage_provider      = "S3"
    storage_base_url      = "s3://${var.lakehouse_bucket}/iceberg/"
    storage_aws_role_arn  = aws_iam_role.snowflake_reader.arn
  }
  allow_writes = false
}

output "external_volume_name" { value = snowflake_external_volume.lakehouse.name }
output "reader_role_arn" { value = aws_iam_role.snowflake_reader.arn }
output "warehouse" { value = snowflake_warehouse.transform.name }
output "database" { value = snowflake_database.transit.name }
