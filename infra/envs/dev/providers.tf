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

# Credentials via env: SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME,
# SNOWFLAKE_USER, SNOWFLAKE_PASSWORD. Unused until enable_snowflake = true.
provider "snowflake" {
  role = "ACCOUNTADMIN"
}
