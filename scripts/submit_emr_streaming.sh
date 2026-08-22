#!/bin/bash
# Package spark jobs, stage jars (no internet inside VPC-attached EMR Serverless),
# and start the streaming job (bronze + silver) in STREAMING mode.
# Usage: scripts/submit_emr_streaming.sh
set -euo pipefail
cd "$(dirname "$0")/.."

REGION=${AWS_REGION:-us-east-2}
APP_ID=$(terraform -chdir=infra/envs/dev output -raw emr_application_id)
EXEC_ROLE=$(terraform -chdir=infra/envs/dev output -raw emr_execution_role_arn)
ARTIFACTS=$(terraform -chdir=infra/envs/dev output -raw artifacts_bucket)
LAKEHOUSE=$(terraform -chdir=infra/envs/dev output -raw lakehouse_bucket)
KAFKA_IP=$(terraform -chdir=infra/envs/dev output -raw kafka_private_ip)

SPARK_VER=3.5.4 # EMR 7.5 Spark line; kafka connector must match the major.minor
KAFKA_JARS=(
  "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VER}"
  "org.apache.spark:spark-token-provider-kafka-0-10_2.12:${SPARK_VER}"
  "org.apache.kafka:kafka-clients:3.4.1"
  "org.apache.commons:commons-pool2:2.11.1"
)

# fetch connector jars once from maven central, stage to the artifacts bucket
mkdir -p .jars
for coord in "${KAFKA_JARS[@]}"; do
  IFS=: read -r g a v <<<"$coord"
  jar="${a}-${v}.jar"
  gpath=$(echo "$g" | tr . /)
  [ -f ".jars/$jar" ] || curl -sfL -o ".jars/$jar" \
    "https://repo1.maven.org/maven2/${gpath}/${a}/${v}/${jar}"
done
aws s3 sync .jars "s3://${ARTIFACTS}/jars/" --region "$REGION"

# ship the code
zip -qr .jars/spark_jobs.zip spark_jobs ingestion -x '*__pycache__*'
aws s3 cp .jars/spark_jobs.zip "s3://${ARTIFACTS}/code/spark_jobs.zip" --region "$REGION"
aws s3 cp spark_jobs/run_local.py "s3://${ARTIFACTS}/code/entry.py" --region "$REGION"

JARS=""
for coord in "${KAFKA_JARS[@]}"; do
  IFS=: read -r g a v <<<"$coord"
  JARS+="s3://${ARTIFACTS}/jars/${a}-${v}.jar,"
done
JARS+="/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"

aws emr-serverless start-job-run \
  --region "$REGION" \
  --application-id "$APP_ID" \
  --execution-role-arn "$EXEC_ROLE" \
  --mode STREAMING \
  --name transit-streaming \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${ARTIFACTS}/code/entry.py\",
      \"sparkSubmitParameters\": \"--py-files s3://${ARTIFACTS}/code/spark_jobs.zip --jars ${JARS} --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.lake.type=hadoop --conf spark.sql.catalog.lake.warehouse=s3://${LAKEHOUSE}/iceberg --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.emr-serverless.driverEnv.TP_CLOUD=1 --conf spark.emr-serverless.driverEnv.TP_CITY_TZS=nyc=America/New_York --conf spark.emr-serverless.driverEnv.TP_LAKE_URI=s3://${LAKEHOUSE} --conf spark.emr-serverless.driverEnv.KAFKA_BOOTSTRAP=${KAFKA_IP}:9092 --conf spark.executorEnv.TP_CLOUD=1 --conf spark.executorEnv.TP_CITY_TZS=nyc=America/New_York --conf spark.executorEnv.TP_LAKE_URI=s3://${LAKEHOUSE} --conf spark.executorEnv.KAFKA_BOOTSTRAP=${KAFKA_IP}:9092\"
    }
  }"
