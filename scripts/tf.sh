#!/bin/bash
# Terraform wrapper. Exists because two separate traps bit this project on 2026-08-25,
# and both are easy to hit again by hand:
#
# 1. A SAVED PLAN EMBEDS ROOT-MODULE VARIABLE VALUES IN CLEARTEXT. `sensitive = true`
#    redacts CLI display only, never the plan archive. `make infra-plan` used to write
#    -out=tfplan INSIDE the repo, and a sibling `-out=tfplan.zurich` put four live API
#    keys into git. Plans now go to a temp dir outside the working tree, always.
#
# 2. `terraform plan` COULD NOT RUN AT ALL, so nobody could see drift. The snowflake
#    provider authenticates eagerly even though enable_snowflake = false, and it was
#    configured for password auth while TERRAFORM_SVC is key-pair only (TYPE=SERVICE,
#    MFA-exempt). Worse, `.env` defines SNOWFLAKE_PASSWORD, which the provider picks up
#    automatically and which CONFLICTS with private_key ("password: conflicts with
#    private_key"). So it has to be explicitly unset.
#
#    The workaround people reached for was `-target`, which silently skips everything
#    else — that is how the drain Lambda's concurrency fix sat undeployed while the
#    apply reported success. Now that plan works, -target should be unnecessary.
#
# Usage:
#   scripts/tf.sh plan            # plan everything, print the summary
#   scripts/tf.sh apply           # plan then apply, after showing what changes
#   scripts/tf.sh plan -target=module.spark.aws_lambda_function.drain
set -euo pipefail
cd "$(dirname "$0")/.."

ACTION=${1:-plan}
shift || true

set -a
[ -f .env ] && . ./.env
set +a

# poller API keys: write-only args, never stored in state
export TF_VAR_wmata_api_key="${WMATA_API_KEY:-}"
export TF_VAR_bay511_api_token="${BAY511_API_TOKEN:-}"
export TF_VAR_swiss_otd_token="${SWISS_OTD_TOKEN:-}"
export TF_VAR_swiss_otd_sa_token="${SWISS_OTD_SA_TOKEN:-}"
export TF_VAR_cta_api_key="${CTA_API_KEY:-}"

# snowflake provider: key-pair, not password. Both of these matter.
unset SNOWFLAKE_PASSWORD
export SNOWFLAKE_ORGANIZATION_NAME="${SNOWFLAKE_ORGANIZATION_NAME:-WQTEQYY}"
export SNOWFLAKE_ACCOUNT_NAME="${SNOWFLAKE_ACCOUNT_NAME:-IB47757}"
export SNOWFLAKE_USER="${SNOWFLAKE_USER:-TERRAFORM_SVC}"
export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_PATH:-$HOME/.snowflake/keys/transit_terraform_key.p8}"
if [ ! -f "$KEY_PATH" ]; then
  echo "snowflake key not found at $KEY_PATH — terraform plan cannot run without it" >&2
  exit 1
fi
export SNOWFLAKE_PRIVATE_KEY="$(cat "$KEY_PATH")"

# OUTSIDE the repo, always. Never -out= into the working tree.
PLAN_DIR=$(mktemp -d)
trap 'rm -rf "$PLAN_DIR"' EXIT
PLAN="$PLAN_DIR/plan"

case "$ACTION" in
  plan)
    terraform -chdir=infra/envs/dev plan -out="$PLAN" "$@"
    ;;
  apply)
    terraform -chdir=infra/envs/dev plan -out="$PLAN" "$@"
    echo
    echo "--- applying the plan above; ctrl-c within 5s to abort ---"
    sleep 5
    terraform -chdir=infra/envs/dev apply "$PLAN"
    # -target silently skips everything not named; always confirm nothing is left
    if [ $# -gt 0 ]; then
      echo
      echo "--- targeted apply: re-planning UNTARGETED to show what was skipped ---"
      terraform -chdir=infra/envs/dev plan -out="$PLAN_DIR/verify"
    fi
    ;;
  *)
    echo "usage: scripts/tf.sh {plan|apply} [terraform args]" >&2
    exit 2
    ;;
esac
