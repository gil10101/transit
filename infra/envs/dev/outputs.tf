output "raw_bucket" { value = module.lake.raw_bucket }
output "lakehouse_bucket" { value = module.lake.lakehouse_bucket }
output "artifacts_bucket" { value = module.lake.artifacts_bucket }
output "kafka_private_ip" { value = module.kafka.private_ip }
output "services_instance_id" { value = module.services.instance_id }
output "services_public_ip" { value = module.services.public_ip }
output "ecr_ingestion_url" { value = module.services.ecr_ingestion_url }
output "ecr_dagster_url" { value = module.services.ecr_dagster_url }
output "emr_application_id" { value = module.spark.application_id }
output "emr_execution_role_arn" { value = module.spark.execution_role_arn }
output "snowflake_reader_role_arn" {
  value = var.enable_snowflake ? module.snowflake[0].reader_role_arn : null
}
