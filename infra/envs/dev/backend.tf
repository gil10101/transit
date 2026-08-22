# Bucket name is stamped by `make` / deploy script after bootstrap creates it:
#   terraform init -backend-config="bucket=transit-pulse-tfstate-<account-id>"
terraform {
  backend "s3" {
    key          = "envs/dev/terraform.tfstate"
    region       = "us-east-2"
    use_lockfile = true
  }
}
