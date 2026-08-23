# Snowflake: warehouse, database, schemas, transform role, and the S3 external volume
# for Iceberg-over-silver. External-volume trust is a two-step dance:
#   apply 1 -> external volume created; DESC EXTERNAL VOLUME yields Snowflake's IAM user
#   ARN + external id; set snowflake_iam_user_arn / snowflake_external_id vars
#   apply 2 -> IAM trust narrows from placeholder to Snowflake's principal.
# P5 adds the Dagster pipeline identity (TRANSIT_PIPELINE role + DAGSTER_SVC service
# user) — ownership design rationale at the "P5: Dagster pipeline identity" section below.

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
variable "dagster_public_key" {
  # RSA public key for DAGSTER_SVC — PEM body only, no BEGIN/END lines. Public
  # half of the pair, so not sensitive. Empty string skips creating the user
  # (and only the user), keeping plans clean until the operator generates a key.
  type = string
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
    storage_location_name = "lakehouse-us-east-2"
    storage_provider      = "S3"
    storage_base_url      = "s3://${var.lakehouse_bucket}/iceberg/"
    storage_aws_role_arn  = aws_iam_role.snowflake_reader.arn
  }
  allow_writes = false
}

# --- P5: Dagster pipeline identity -----------------------------------------
#
# TRANSIT_PIPELINE is the automated-pipeline role (Dagster: iceberg refresh ->
# dbt build -> checks). Privilege research (docs.snowflake.com "ALTER ICEBERG
# TABLE", checked 2026-08-22): ALTER ICEBERG TABLE ... REFRESH requires
# OWNERSHIP of the table — Snowflake has no lesser ALTER/REBUILD privilege that
# covers it ("Only the table owner ... or higher can execute this command").
# Consequences, all implemented below:
#   1. Ownership of schema SILVER and of all+future ICEBERG TABLES in it moves
#      to TRANSIT_PIPELINE (outbound_privileges COPY preserves existing grants).
#      Schema ownership also lets the Dagster refresh asset CREATE ICEBERG
#      TABLE IF NOT EXISTS (registration path) and create SILVER.WEATHER_HOURLY.
#   2. TRANSIT_PIPELINE is granted to ACCOUNTADMIN, so TERRAFORM_SVC's existing
#      paths (laptop dbt --target prod, scripts/snowflake_register_iceberg.py,
#      this terraform module itself) inherit ownership transitively and keep
#      working unchanged.
#   3. GOLD: dbt as DAGSTER_SVC must CREATE OR REPLACE tables/views that earlier
#      laptop runs created as ACCOUNTADMIN, and replacing an existing object
#      also requires OWNERSHIP — plain ALL-privileges grants would fail on the
#      first replace. So ownership of all+future TABLES and VIEWS in GOLD moves
#      to TRANSIT_PIPELINE too; future grants keep the invariant even for
#      objects a laptop run creates later.
# Fresh-account ordering caveat: the TRANSIT_OBJ_STORE catalog integration is
# created by scripts/snowflake_register_iceberg.py, not terraform — on a
# from-scratch account run that script once before the integration USAGE grant
# below can apply.

locals {
  silver_fq = "\"${snowflake_database.transit.name}\".\"${snowflake_schema.silver_ext.name}\""
  gold_fq   = "\"${snowflake_database.transit.name}\".\"${snowflake_schema.gold.name}\""
}

resource "snowflake_account_role" "transit_pipeline" {
  name    = "TRANSIT_PIPELINE"
  comment = "Automated pipeline role (Dagster): owns SILVER iceberg refresh + GOLD dbt builds"
}

# Hierarchy first: ACCOUNTADMIN inherits TRANSIT_PIPELINE before any ownership
# below transfers, so TERRAFORM_SVC never loses access mid-apply.
resource "snowflake_grant_account_role" "pipeline_to_accountadmin" {
  role_name        = snowflake_account_role.transit_pipeline.name
  parent_role_name = "ACCOUNTADMIN"
}

resource "snowflake_service_user" "dagster_svc" {
  count             = var.dagster_public_key == "" ? 0 : 1
  name              = "DAGSTER_SVC"
  comment           = "Dagster orchestration (key-pair auth; private key delivered via SSM on the services box)"
  default_warehouse = snowflake_warehouse.transform.name
  default_role      = snowflake_account_role.transit_pipeline.name
  rsa_public_key    = var.dagster_public_key
  # Quirk 1 (docs/operations.md): account session default is America/Los_Angeles,
  # which silently skews NTZ-vs-TZ delay math. Every service user pins UTC.
  timezone = "UTC"
}

resource "snowflake_grant_account_role" "pipeline_to_dagster" {
  count     = var.dagster_public_key == "" ? 0 : 1
  role_name = snowflake_account_role.transit_pipeline.name
  user_name = snowflake_service_user.dagster_svc[0].name
}

# --- plain privileges (per P5 brief) ---

resource "snowflake_grant_privileges_to_account_role" "pipeline_db_usage" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  # CREATE SCHEMA: dbt materializes store_failures audit schemas (<schema>_dbt_test__audit)
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.transit.name
  }
}

# dbt's existing store_failures audit schema predates TRANSIT_PIPELINE; without
# ownership the pipeline's dbt runs fail on the first store_failures test.
resource "snowflake_grant_ownership" "audit_schema" {
  account_role_name   = snowflake_account_role.transit_pipeline.name
  outbound_privileges = "COPY"
  on {
    object_type = "SCHEMA"
    object_name = "\"${snowflake_database.transit.name}\".\"GOLD_DBT_TEST__AUDIT\""
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "audit_all_tables" {
  account_role_name   = snowflake_account_role.transit_pipeline.name
  outbound_privileges = "COPY"
  on {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.transit.name}\".\"GOLD_DBT_TEST__AUDIT\""
    }
  }
  depends_on = [snowflake_grant_ownership.audit_schema]
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_wh_usage" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_silver_usage" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.silver_fq
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_gold_schema_all" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  all_privileges    = true
  on_schema {
    schema_name = local.gold_fq
  }
}

# SELECT on all+future in SILVER: TABLES (e.g. WEATHER_HOURLY) and ICEBERG
# TABLES granted separately — bulk TABLES grants don't reliably cover iceberg.
resource "snowflake_grant_privileges_to_account_role" "pipeline_silver_select_all_tables" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.silver_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_silver_select_future_tables" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.silver_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_silver_select_all_iceberg" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "ICEBERG TABLES"
      in_schema          = local.silver_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_silver_select_future_iceberg" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "ICEBERG TABLES"
      in_schema          = local.silver_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_gold_all_tables" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.gold_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_gold_future_tables" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.gold_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_gold_all_views" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = local.gold_fq
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_gold_future_views" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = local.gold_fq
    }
  }
}

# Iceberg registration/refresh dependencies: the external volume (terraform-
# managed) and the object-store catalog integration (script-created; see the
# fresh-account caveat in the section comment).
resource "snowflake_grant_privileges_to_account_role" "pipeline_extvol_usage" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "EXTERNAL VOLUME"
    object_name = snowflake_external_volume.lakehouse.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeline_catalog_int_usage" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = "TRANSIT_OBJ_STORE"
  }
}

# --- ownership transfers (see section comment for why) ---

resource "snowflake_grant_ownership" "silver_schema" {
  account_role_name   = snowflake_account_role.transit_pipeline.name
  outbound_privileges = "COPY"
  on {
    object_type = "SCHEMA"
    object_name = local.silver_fq
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "silver_iceberg_all" {
  account_role_name   = snowflake_account_role.transit_pipeline.name
  outbound_privileges = "COPY"
  on {
    all {
      object_type_plural = "ICEBERG TABLES"
      in_schema          = local.silver_fq
    }
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "silver_iceberg_future" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  on {
    future {
      object_type_plural = "ICEBERG TABLES"
      in_schema          = local.silver_fq
    }
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "gold_all_tables" {
  account_role_name   = snowflake_account_role.transit_pipeline.name
  outbound_privileges = "COPY"
  on {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.gold_fq
    }
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "gold_future_tables" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  on {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.gold_fq
    }
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "gold_all_views" {
  account_role_name   = snowflake_account_role.transit_pipeline.name
  outbound_privileges = "COPY"
  on {
    all {
      object_type_plural = "VIEWS"
      in_schema          = local.gold_fq
    }
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

resource "snowflake_grant_ownership" "gold_future_views" {
  account_role_name = snowflake_account_role.transit_pipeline.name
  on {
    future {
      object_type_plural = "VIEWS"
      in_schema          = local.gold_fq
    }
  }
  depends_on = [snowflake_grant_account_role.pipeline_to_accountadmin]
}

output "external_volume_name" { value = snowflake_external_volume.lakehouse.name }
output "reader_role_arn" { value = aws_iam_role.snowflake_reader.arn }
output "warehouse" { value = snowflake_warehouse.transform.name }
output "database" { value = snowflake_database.transit.name }
output "pipeline_role" { value = snowflake_account_role.transit_pipeline.name }
