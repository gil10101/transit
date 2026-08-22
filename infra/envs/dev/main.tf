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
}
