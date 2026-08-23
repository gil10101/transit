#!/bin/bash
# Build linux/arm64 images for the services box and push to ECR.
# Usage: scripts/deploy_images.sh [all|ingestion|dagster]   (default: all)
# Run after terraform apply; reads ECR URLs from terraform outputs.
# Both images build with repo-root context (.dockerignore keeps it small):
# the dagster image copies dbt/transit for its baked dbt manifest (P5), which
# an orchestration/-scoped context cannot reach.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET=${1:-all}
REGION=${AWS_REGION:-us-east-2}
INGESTION_URL=$(terraform -chdir=infra/envs/dev output -raw ecr_ingestion_url)
DAGSTER_URL=$(terraform -chdir=infra/envs/dev output -raw ecr_dagster_url)
REGISTRY=${INGESTION_URL%%/*}

aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

pushed=()
if [[ "$TARGET" == "all" || "$TARGET" == "ingestion" ]]; then
  docker buildx build --platform linux/arm64 -f ingestion/Dockerfile \
    -t "$INGESTION_URL:latest" --push .
  pushed+=("$INGESTION_URL:latest")
fi
if [[ "$TARGET" == "all" || "$TARGET" == "dagster" ]]; then
  docker buildx build --platform linux/arm64 -f orchestration/Dockerfile \
    -t "$DAGSTER_URL:latest" --push .
  pushed+=("$DAGSTER_URL:latest")
fi

echo "pushed: ${pushed[*]}"
