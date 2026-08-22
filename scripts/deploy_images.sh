#!/bin/bash
# Build linux/arm64 images for the services box and push to ECR.
# Usage: scripts/deploy_images.sh  (run after terraform apply; reads outputs)
set -euo pipefail
cd "$(dirname "$0")/.."

REGION=${AWS_REGION:-us-east-1}
INGESTION_URL=$(terraform -chdir=infra/envs/dev output -raw ecr_ingestion_url)
DAGSTER_URL=$(terraform -chdir=infra/envs/dev output -raw ecr_dagster_url)
REGISTRY=${INGESTION_URL%%/*}

aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

docker buildx build --platform linux/arm64 -f ingestion/Dockerfile \
  -t "$INGESTION_URL:latest" --push .
docker buildx build --platform linux/arm64 -f orchestration/Dockerfile \
  -t "$DAGSTER_URL:latest" --push orchestration

echo "pushed: $INGESTION_URL:latest, $DAGSTER_URL:latest"
