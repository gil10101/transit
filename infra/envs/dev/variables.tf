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
