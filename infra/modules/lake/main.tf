# Lake buckets: raw (replay archive), lakehouse (Iceberg), artifacts (jars, images, dbt state).

variable "prefix" { type = string }
variable "account_id" { type = string }

locals {
  buckets = ["raw", "lakehouse", "artifacts"]
}

resource "aws_s3_bucket" "this" {
  for_each = toset(local.buckets)
  bucket   = "${var.prefix}-${var.account_id}-${each.key}"
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# raw archive: cheap storage after 30 days; history is the asset, never expire
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.this["raw"].id
  rule {
    id     = "to-ia"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# [rev 2026-08-25] EMR debug logs: 3.4 GB accumulated in three days and nothing reads
# them after the week they were produced. This is the one unbounded store that was an
# oversight rather than a decision — the raw archive above is deliberately kept forever
# (history is the asset) and lakehouse holds live Iceberg data.
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.this["artifacts"].id
  rule {
    id     = "expire-emr-debug-logs"
    status = "Enabled"
    filter { prefix = "emr-logs/" }
    expiration { days = 14 }
  }
  # a failed multipart upload otherwise lingers invisibly and is billed forever
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

output "raw_bucket" { value = aws_s3_bucket.this["raw"].bucket }
output "lakehouse_bucket" { value = aws_s3_bucket.this["lakehouse"].bucket }
output "artifacts_bucket" { value = aws_s3_bucket.this["artifacts"].bucket }
output "bucket_arns" { value = { for k, b in aws_s3_bucket.this : k => b.arn } }
