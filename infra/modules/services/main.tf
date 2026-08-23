# Services box (t4g.medium): poller container + Dagster (webserver, daemon, postgres)
# via docker compose, images from ECR. Admin access via SSM Session Manager, no SSH keys.

variable "prefix" { type = string }
variable "region" { type = string }
variable "subnet_id" { type = string }
variable "sg_id" { type = string }
variable "bucket_arns" { type = map(string) }
variable "raw_bucket" { type = string }
variable "lakehouse_bucket" { type = string }
variable "kafka_private_ip" { type = string }
variable "instance_type" {
  type    = string
  default = "t4g.medium"
}

# --- P5: EMR job-submit strings for the Dagster containers, single-sourced from
# the spark module's outputs (identical to what the drain Lambda receives).
variable "emr_application_id" { type = string }
variable "emr_execution_role_arn" { type = string }
variable "emr_entry_point" { type = string }
variable "emr_static_entry_point" { type = string }
variable "emr_spark_params" { type = string }
variable "emr_log_uri" { type = string }

# --- P5: Snowflake identity for Dagster (key-pair auth; private key via SSM, below).
variable "snowflake_account" {
  type    = string
  default = "wqteqyy-ib47757"
}
variable "snowflake_user" {
  type    = string
  default = "DAGSTER_SVC"
}
variable "snowflake_role" {
  type    = string
  default = "TRANSIT_PIPELINE"
}

data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "al2023_arm" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "aws_ecr_repository" "ingestion" {
  name         = "${var.prefix}/ingestion"
  force_delete = true
}

resource "aws_ecr_repository" "dagster" {
  name         = "${var.prefix}/dagster"
  force_delete = true
}

# DAGSTER_SVC's Snowflake private key. Terraform creates only a placeholder; the
# real PEM is set out-of-band by the operator and NEVER enters git or tf state:
#   aws ssm put-parameter --name /<prefix>/dagster/snowflake_key \
#     --type SecureString --value "file://dagster_key.p8" --overwrite
# The Dagster container entrypoint fetches it at start into
# SNOWFLAKE_PRIVATE_KEY_PATH (chmod 600). value_wo (write-only) keeps the value
# out of terraform state entirely — the provider never reads it back, so the
# operator-set PEM is neither reverted nor persisted (a plain `value` would be
# stored decrypted in raw state even with ignore_changes).
resource "aws_ssm_parameter" "dagster_snowflake_key" {
  name             = "/${var.prefix}/dagster/snowflake_key"
  type             = "SecureString"
  value_wo         = "PLACEHOLDER"
  value_wo_version = 1
}

resource "aws_iam_role" "services" {
  name = "${var.prefix}-services"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "services_ssm" {
  role       = aws_iam_role.services.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "services" {
  name = "lake-ecr"
  role = aws_iam_role.services.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = flatten([for arn in values(var.bucket_arns) : [arn, "${arn}/*"]])
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
        Resource = [aws_ecr_repository.ingestion.arn, aws_ecr_repository.dagster.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:ListJobRuns"]
        Resource = "*"
      },
      {
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = "*"
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = aws_ssm_parameter.dagster_snowflake_key.arn
      },
      {
        # SecureString decrypt: default aws/ssm key only, via SSM only. All
        # containers on the box share this role through IMDS (hop_limit 2) —
        # box-wide by design on this single-instance setup.
        Effect    = "Allow"
        Action    = ["kms:Decrypt"]
        Resource  = data.aws_kms_alias.ssm.target_key_arn
        Condition = { StringEquals = { "kms:ViaService" = "ssm.${var.region}.amazonaws.com" } }
      }
    ]
  })
}

resource "aws_iam_instance_profile" "services" {
  name = "${var.prefix}-services"
  role = aws_iam_role.services.name
}

locals {
  registry = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com"
  compose  = <<-YAML
    services:
      poller:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["nyc"]
        restart: always
        environment:
          KAFKA_BOOTSTRAP: ${var.kafka_private_ip}:9092
          RAW_BUCKET: ${var.raw_bucket}
          LAKE_BUCKET: ${var.lakehouse_bucket}
      postgres:
        image: postgres:16-alpine
        restart: always
        environment:
          POSTGRES_USER: dagster
          POSTGRES_PASSWORD: dagster
          POSTGRES_DB: dagster
        volumes: ["pg-data:/var/lib/postgresql/data"]
      dagster-webserver:
        image: ${local.registry}/${aws_ecr_repository.dagster.name}:latest
        command: ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace.yaml"]
        restart: always
        environment: &denv
          DAGSTER_PG_HOST: postgres
          DAGSTER_PG_USER: dagster
          DAGSTER_PG_PASSWORD: dagster
          DAGSTER_PG_DB: dagster
          AWS_REGION: ${var.region}
          RAW_BUCKET: ${var.raw_bucket}
          LAKE_BUCKET: ${var.lakehouse_bucket}
          KAFKA_BOOTSTRAP: ${var.kafka_private_ip}:9092
          APP_ID: ${var.emr_application_id}
          EXEC_ROLE_ARN: ${var.emr_execution_role_arn}
          ENTRY_POINT: ${var.emr_entry_point}
          STATIC_ENTRY_POINT: ${var.emr_static_entry_point}
          SPARK_PARAMS: '${var.emr_spark_params}'
          LOG_URI: ${var.emr_log_uri}
          SNOWFLAKE_ACCOUNT: ${var.snowflake_account}
          SNOWFLAKE_USER: ${var.snowflake_user}
          SNOWFLAKE_ROLE: ${var.snowflake_role}
          SNOWFLAKE_KEY_SSM_PARAM: ${aws_ssm_parameter.dagster_snowflake_key.name}
          SNOWFLAKE_PRIVATE_KEY_PATH: /home/appuser/.snowflake/dagster_key.p8
        ports: ["127.0.0.1:3000:3000"]  # loopback only; SSM port-forward reaches localhost
      dagster-daemon:
        image: ${local.registry}/${aws_ecr_repository.dagster.name}:latest
        command: ["dagster-daemon", "run", "-w", "/opt/dagster/app/workspace.yaml"]
        restart: always
        environment: *denv
    volumes:
      pg-data:
  YAML

  unit = <<-UNIT
    [Unit]
    Description=Transit Pulse services
    Requires=docker.service
    After=docker.service
    [Service]
    Type=oneshot
    RemainAfterExit=yes
    ExecStartPre=/bin/bash -c 'aws ecr get-login-password --region ${var.region} | docker login --username AWS --password-stdin ${local.registry}'
    ExecStart=/usr/bin/docker compose -f /opt/transit/docker-compose.yml up -d
    ExecStop=/usr/bin/docker compose -f /opt/transit/docker-compose.yml down
    [Install]
    WantedBy=multi-user.target
  UNIT
}

resource "aws_instance" "services" {
  ami                    = data.aws_ssm_parameter.al2023_arm.value
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.sg_id]
  iam_instance_profile   = aws_iam_instance_profile.services.name

  # containers reach instance-role creds through IMDS (hop 2)
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  # NOTE: user_data changes update in place (stop/start, instance id kept) but
  # cloud-init runs once per instance, so an updated compose/unit does NOT land
  # on the box by itself. After an apply that changes them, re-land with:
  #   aws ssm send-command ... 'cloud-init clean --logs && reboot'
  # (procedure in docs/operations.md "Dagster (P5)").
  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    dnf install -y docker
    systemctl enable --now docker
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -sL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-aarch64" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    mkdir -p /opt/transit
    echo '${base64encode(local.compose)}' | base64 -d > /opt/transit/docker-compose.yml
    echo '${base64encode(local.unit)}' | base64 -d > /etc/systemd/system/transit.service
    systemctl daemon-reload
    systemctl enable --now transit.service
  EOF

  tags = { Name = "${var.prefix}-services" }
}

output "instance_id" { value = aws_instance.services.id }
output "public_ip" { value = aws_instance.services.public_ip }
output "ecr_ingestion_url" { value = aws_ecr_repository.ingestion.repository_url }
output "ecr_dagster_url" { value = aws_ecr_repository.dagster.repository_url }

data "aws_kms_alias" "ssm" {
  name = "alias/aws/ssm"
}
