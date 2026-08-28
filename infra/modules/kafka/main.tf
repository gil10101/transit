# Single-broker Kafka (KRaft) on a t4g.small, docker-run from the official image.
# MSK is the documented swap when a managed line item is worth $40+/mo.

variable "prefix" { type = string }
variable "subnet_id" { type = string }
variable "sg_id" { type = string }
variable "instance_type" {
  type    = string
  default = "t4g.small"
}

data "aws_ssm_parameter" "al2023_arm" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "aws_iam_role" "kafka" {
  name = "${var.prefix}-kafka"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "kafka_ssm" {
  role       = aws_iam_role.kafka.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "kafka" {
  name = "${var.prefix}-kafka"
  role = aws_iam_role.kafka.name
}

resource "aws_instance" "kafka" {
  ami                    = data.aws_ssm_parameter.al2023_arm.value
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.sg_id]
  iam_instance_profile   = aws_iam_instance_profile.kafka.name

  root_block_device {
    # [rev 2026-08-28] 30GB filled to 100% after 5.8 days at the old 168h
    # retention and took the broker down (both 16:05Z chain reds). 48 matches
    # the live volume, grown online during the incident.
    volume_size = 48
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    dnf install -y docker
    systemctl enable --now docker
    TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
    PRIVATE_IP=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
    mkdir -p /var/kafka-data && chown 1000:1000 /var/kafka-data
    docker run -d --name kafka --restart always \
      -p 9092:9092 \
      -v /var/kafka-data:/var/lib/kafka/data \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$${PRIVATE_IP}:9092 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
      -e KAFKA_LOG_RETENTION_HOURS=24 \
      -e KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=300000 \
      apache/kafka:3.8.0
    # Retention is a DISK budget, not an archive: raw S3 is the archive and the
    # 2-hourly drains never read past a few hours back. 168h filled a 30GB disk
    # in 5.8 days and killed the broker (2026-08-28). 24h ≈ 5-6GB steady state.
  EOF

  tags = { Name = "${var.prefix}-kafka" }
}

output "private_ip" { value = aws_instance.kafka.private_ip }
