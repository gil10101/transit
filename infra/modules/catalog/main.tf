# Glue catalog databases. Iceberg tables currently use the Hadoop catalog on S3
# (no VPC endpoints / NAT needed from EMR Serverless); these databases stand ready
# for the Glue-catalog migration when private Glue API access is provisioned.

variable "prefix" { type = string }

resource "aws_glue_catalog_database" "bronze" {
  name = replace("${var.prefix}_bronze", "-", "_")
}

resource "aws_glue_catalog_database" "silver" {
  name = replace("${var.prefix}_silver", "-", "_")
}

output "bronze_db" { value = aws_glue_catalog_database.bronze.name }
output "silver_db" { value = aws_glue_catalog_database.silver.name }
