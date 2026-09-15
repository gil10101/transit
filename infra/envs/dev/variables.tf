variable "region" {
  type    = string
  default = "us-east-2"
}

variable "prefix" {
  type    = string
  default = "transit-pulse"
}

variable "alert_email" { type = string }

variable "enable_snowflake" {
  type    = bool
  default = false
}

variable "snowflake_iam_user_arn" {
  type    = string
  default = ""
}

variable "snowflake_external_id" {
  type    = string
  default = ""
}

variable "budget_monthly_usd" {
  type    = number
  default = 50
}

variable "dagster_public_key" {
  # RSA public key for the DAGSTER_SVC Snowflake user (PEM body, no BEGIN/END
  # lines). Public half only — the private key goes to SSM out-of-band, never
  # here. Empty string skips creating the user so plans stay clean pre-keygen.
  type    = string
  default = ""
}

# --- P3 batch 2: poller API keys. Export from .env right before apply and
# unset after (docs/operations.md "P3 batch 2 rollout") — values flow through
# write-only SSM args, so they reach neither terraform state nor the repo.
# Empty defaults keep plan/validate working without the secrets; the services
# module then seeds PLACEHOLDER and the value is set out-of-band. NEVER echo
# or commit a key value; the Swiss tokens cannot be re-issued if revoked.
variable "wmata_api_key" {
  type      = string
  default   = ""
  sensitive = true
}
variable "bay511_api_token" {
  type      = string
  default   = ""
  sensitive = true
}
variable "swiss_otd_token" {
  type      = string
  default   = ""
  sensitive = true
}
variable "swiss_otd_sa_token" {
  type      = string
  default   = ""
  sensitive = true
}

# Cities whose poller is intentionally stopped. A city is retired once it has the
# 20 closed judged days the scorecard requires: fct_city_scorecard reads stored
# history with no recency filter, so the score survives the feed being switched
# off, and the raw-feed freshness tripwire skips these prefixes instead of
# reporting them as killed feeds four times an hour.
variable "polling_retired" {
  type = string
  # Retired 2026-09-14/15 once each city banked its 20 judged days. Left as the
  # default rather than a tfvars entry so `terraform apply` reproduces the live
  # state instead of silently restarting six pollers that were stopped on purpose.
  default = "nyc,boston,toronto,helsinki,dc,sf,zurich"
}
