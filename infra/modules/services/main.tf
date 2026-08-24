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

# --- P3 batch 2: poller API keys (dc/sf/zurich). Values arrive as TF_VAR_* at
# apply time (integrator exports them from .env — never in repo) and land in
# SSM SecureStrings through value_wo, so they never enter terraform state.
# Empty (a plan/apply without the TF_VARs exported) writes PLACEHOLDER; the
# real value is then set out-of-band exactly like the dagster key. Rotation is
# always out-of-band (docs/operations.md "Poller API keys"):
#   aws ssm put-parameter --name /<prefix>/keys/<name> --type SecureString \
#     --value "$FROM_ENV" --overwrite     # never echo the value
# then `systemctl restart transit.service` (fetch runs at service start).
# value_wo never reverts an out-of-band value: the provider only re-sends on a
# value_wo_version bump.
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
  # Swiss tokens are irreplaceable if revoked (opentransportdata.swiss) — SSM
  # SecureString + local .env are the ONLY homes. One token per API product:
  # this one is for /la/gtfs-rt.
  type      = string
  default   = ""
  sensitive = true
}
variable "swiss_otd_sa_token" {
  # Separate token for the /la/gtfs-sa service-alerts product. Irreplaceable.
  type      = string
  default   = ""
  sensitive = true
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

# P3 batch 2 poller API keys, same value_wo pattern as the dagster key (values
# never in state). Seeded from TF_VAR_* at apply time; coalesce writes
# PLACEHOLDER when a TF_VAR is missing so a plain plan/apply still converges.
# The box fetches these into /opt/transit/secrets.env at every transit.service
# start (local.fetch_secrets below).
locals {
  poller_secret_values = {
    wmata    = var.wmata_api_key
    bay511   = var.bay511_api_token
    swiss_rt = var.swiss_otd_token
    swiss_sa = var.swiss_otd_sa_token
  }
}

resource "aws_ssm_parameter" "poller_key" {
  for_each         = toset(["wmata", "bay511", "swiss_rt", "swiss_sa"])
  name             = "/${var.prefix}/keys/${each.key}"
  type             = "SecureString"
  value_wo         = coalesce(local.poller_secret_values[each.key], "PLACEHOLDER")
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
        Effect = "Allow"
        Action = ["ssm:GetParameter"]
        Resource = concat(
          [aws_ssm_parameter.dagster_snowflake_key.arn],
          [for p in values(aws_ssm_parameter.poller_key) : p.arn],
        )
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
      # P3: one poller container per live city, same image, city as the only arg
      # (python -m ingestion.poller <city>). Keep in sync with LIVE_CITIES in
      # orchestration/transit_dagster/lib.py. `poller` stays nyc so existing
      # health checks / container names survive.
      poller:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["nyc"]
        restart: always
        environment: &penv
          KAFKA_BOOTSTRAP: ${var.kafka_private_ip}:9092
          RAW_BUCKET: ${var.raw_bucket}
          LAKE_BUCKET: ${var.lakehouse_bucket}
      poller-boston:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["boston"]
        restart: always
        environment: *penv
      poller-toronto:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["toronto"]
        restart: always
        environment: *penv
      poller-helsinki:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["helsinki"]
        restart: always
        environment: *penv
      # P3 batch 2 — keyed cities. API keys come from /opt/transit/secrets.env
      # (WMATA_API_KEY / BAY511_API_TOKEN / SWISS_OTD_TOKEN / SWISS_OTD_SA_TOKEN),
      # written from SSM by fetch_secrets.sh at every transit.service start, so
      # `systemctl restart transit.service` picks up rotated keys. env_file only
      # on the keyed pollers — key values never land in this compose file.
      poller-dc:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["dc"]
        restart: always
        environment: *penv
        env_file: [/opt/transit/secrets.env]
      poller-sf:
        image: ${local.registry}/${aws_ecr_repository.ingestion.name}:latest
        command: ["sf"]
        restart: always
        environment: *penv
        env_file: [/opt/transit/secrets.env]
      # poller-zurich is deliberately absent until the Zurich allow-list ships.
      # /la/gtfs-rt is the NATIONAL feed: without the silver-side filter every
      # Swiss operator would be stored as city='zurich' (wrong reliability
      # answers), and archiving the unfiltered feed at 30s runs ~0.5-1 TB/month
      # against the <$100/mo budget. Sequence in docs/04 [rev P3 batch 2b]:
      # static parse -> generate zurich_route_allowlist -> silver filter ->
      # add this service. The SSM key params and its yaml are already in place.
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
          # the image's botocore (pinned old by a dependency) reads only AWS_DEFAULT_REGION
          AWS_DEFAULT_REGION: ${var.region}
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
        # the weekly static refresh downloads key-gated schedule zips (WMATA
        # rail+bus need the api_key header), so Dagster reads the same
        # secrets.env the keyed pollers do — same box, same instance role
        env_file: &dsecrets [/opt/transit/secrets.env]
        ports: ["127.0.0.1:3000:3000"]  # loopback only; SSM port-forward reaches localhost
      dagster-daemon:
        image: ${local.registry}/${aws_ecr_repository.dagster.name}:latest
        command: ["dagster-daemon", "run", "-w", "/opt/dagster/app/workspace.yaml"]
        restart: always
        environment: *denv
        env_file: *dsecrets
    volumes:
      pg-data:
  YAML

  # P3 batch 2: SSM SecureStrings -> /opt/transit/secrets.env (0600), run as the
  # first transit.service ExecStartPre so a restart re-fetches rotated values.
  # SECURITY: values are captured into a shell variable and printf'd (a bash
  # builtin) straight into the 0600 file — never echoed to stdout/journal, never
  # passed to an external process's argv. Failures log the parameter NAME only.
  # A missing/PLACEHOLDER param skips its var (that city's poller crash-loops on
  # auth, visible in docker logs) but the file is always written, so compose and
  # the keyless cities never stall on it.
  fetch_secrets = <<-SCRIPT
    #!/bin/bash
    set -uo pipefail
    umask 077
    out=/opt/transit/secrets.env
    tmp="$out.tmp"
    : > "$tmp"
    fetch() {
      local v
      if v=$(aws ssm get-parameter --region ${var.region} --name "$2" \
               --with-decryption --query Parameter.Value --output text 2>/dev/null) \
         && [ -n "$v" ] && [ "$v" != "PLACEHOLDER" ]; then
        printf '%s=%s\n' "$1" "$v" >> "$tmp"
      else
        echo "WARN: $2 unavailable or placeholder; $1 not written" >&2
      fi
    }
    fetch WMATA_API_KEY ${aws_ssm_parameter.poller_key["wmata"].name}
    fetch BAY511_API_TOKEN ${aws_ssm_parameter.poller_key["bay511"].name}
    fetch SWISS_OTD_TOKEN ${aws_ssm_parameter.poller_key["swiss_rt"].name}
    fetch SWISS_OTD_SA_TOKEN ${aws_ssm_parameter.poller_key["swiss_sa"].name}
    chmod 600 "$tmp"
    mv "$tmp" "$out"
  SCRIPT

  unit = <<-UNIT
    [Unit]
    Description=Transit Pulse services
    Requires=docker.service
    After=docker.service
    [Service]
    Type=oneshot
    RemainAfterExit=yes
    ExecStartPre=/bin/bash /opt/transit/fetch_secrets.sh
    ExecStartPre=/bin/bash -c 'aws ecr get-login-password --region ${var.region} | docker login --username AWS --password-stdin ${local.registry}'
    ExecStartPre=/usr/bin/docker compose -f /opt/transit/docker-compose.yml pull --quiet
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
    # compose treats a missing env_file as fatal, and on a re-landed boot the
    # already-enabled transit.service can reach `compose up` before this script
    # has written the real secrets.env. Create it empty first (0600) so the
    # stack always starts: fetch_secrets.sh overwrites it on the same start, and
    # a poller that briefly sees no key crash-loops visibly instead of taking
    # every other city down with it.
    touch /opt/transit/secrets.env
    chmod 600 /opt/transit/secrets.env
    echo '${base64encode(local.compose)}' | base64 -d > /opt/transit/docker-compose.yml
    echo '${base64encode(local.fetch_secrets)}' | base64 -d > /opt/transit/fetch_secrets.sh
    chmod 700 /opt/transit/fetch_secrets.sh
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
