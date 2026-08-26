terraform {
  required_version = ">= 1.11" # value_wo (write-only args) on the SSM key param
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.77" # aws_ssm_parameter value_wo support
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "transit-pulse", Env = "dev", ManagedBy = "terraform" }
  }
}

# [rev 2026-08-25] Credentials via env, KEY-PAIR not password:
#   SNOWFLAKE_ORGANIZATION_NAME  SNOWFLAKE_ACCOUNT_NAME  SNOWFLAKE_USER
#   SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
#   SNOWFLAKE_PRIVATE_KEY="$(cat ~/.snowflake/keys/transit_terraform_key.p8)"
#   and SNOWFLAKE_PASSWORD must be UNSET.
#
# This comment used to say "SNOWFLAKE_PASSWORD" and "unused until enable_snowflake =
# true". Both were wrong, and together they cost real incidents. TERRAFORM_SVC is
# TYPE=SERVICE with key-pair auth and no password, so password auth could never work.
# enable_snowflake is TRUE — this provider manages live grants. And `.env` defines
# SNOWFLAKE_PASSWORD, which the provider reads automatically and which errors with
# "password: conflicts with private_key".
#
# The provider authenticates EAGERLY, so a bad config makes `terraform plan` fail
# outright — drift becomes invisible, and the workaround people reach for is `-target`,
# which silently skips everything unnamed. That is exactly how the drain Lambda's
# concurrency fix sat undeployed while the apply reported success.
#
# Use scripts/tf.sh (make infra-plan / infra-apply); it sets all of this correctly.
provider "snowflake" {
  role = "ACCOUNTADMIN"
}
