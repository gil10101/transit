data "aws_caller_identity" "current" {}

module "lake" {
  source     = "../../modules/lake"
  prefix     = var.prefix
  account_id = data.aws_caller_identity.current.account_id
}

module "catalog" {
  source = "../../modules/catalog"
  prefix = var.prefix
}

module "monitoring" {
  source            = "../../modules/monitoring"
  alert_email       = var.alert_email
  monthly_limit_usd = var.budget_monthly_usd
}

module "network" {
  source = "../../modules/network"
  prefix = var.prefix
  region = var.region
}

module "kafka" {
  source    = "../../modules/kafka"
  prefix    = var.prefix
  subnet_id = module.network.subnet_ids[0]
  sg_id     = module.network.sg_kafka_id
}

module "spark" {
  source           = "../../modules/spark"
  prefix           = var.prefix
  subnet_ids       = module.network.subnet_ids
  sg_id            = module.network.sg_emr_id
  bucket_arns      = module.lake.bucket_arns
  artifacts_bucket = module.lake.artifacts_bucket
  lakehouse_bucket = module.lake.lakehouse_bucket
  kafka_private_ip = module.kafka.private_ip
}

module "services" {
  source           = "../../modules/services"
  prefix           = var.prefix
  region           = var.region
  subnet_id        = module.network.subnet_ids[0]
  sg_id            = module.network.sg_services_id
  bucket_arns      = module.lake.bucket_arns
  raw_bucket       = module.lake.raw_bucket
  lakehouse_bucket = module.lake.lakehouse_bucket
  kafka_private_ip = module.kafka.private_ip
  # P5: Dagster submits the same EMR jobs as the drain Lambda — single-source
  # the job-submit strings from the spark module.
  emr_application_id     = module.spark.application_id
  emr_execution_role_arn = module.spark.execution_role_arn
  emr_entry_point        = module.spark.entry_point
  emr_static_entry_point = module.spark.static_entry_point
  emr_odpt_static_entry_point = module.spark.odpt_static_entry_point
  emr_spark_params       = module.spark.spark_params
  emr_log_uri            = module.spark.log_uri
  # pipeline health alerting: Dagster's run-failure sensor publishes here
  pipeline_alerts_topic_arn = module.monitoring.pipeline_alerts_topic_arn
  # P3 batch 2: poller API keys -> SSM SecureStrings via write-only args
  # (TF_VAR_wmata_api_key etc. at apply time; never in state)
  wmata_api_key      = var.wmata_api_key
  bay511_api_token   = var.bay511_api_token
  swiss_otd_token    = var.swiss_otd_token
  swiss_otd_sa_token = var.swiss_otd_sa_token
}

# External box watchdog (separate module: see the cycle note in its header).
# Alarms already exist live (CLI, 2026-08-28) — import before the next apply.
module "box_alarms" {
  source               = "../../modules/box_alarms"
  prefix               = var.prefix
  topic_arn            = module.monitoring.pipeline_alerts_topic_arn
  services_instance_id = module.services.instance_id
  kafka_instance_id    = module.kafka.instance_id
}

module "snowflake" {
  count                  = var.enable_snowflake ? 1 : 0
  source                 = "../../modules/snowflake"
  prefix                 = var.prefix
  lakehouse_bucket       = module.lake.lakehouse_bucket
  lakehouse_bucket_arn   = module.lake.bucket_arns["lakehouse"]
  account_id             = data.aws_caller_identity.current.account_id
  snowflake_iam_user_arn = var.snowflake_iam_user_arn
  snowflake_external_id  = var.snowflake_external_id
  dagster_public_key     = var.dagster_public_key
}
