# 11 — Teardown runbook

Everything that costs money on AWS and Snowflake, removed in an order that never strands a
resource. The public site (https://transit.gillu.me, Vercel project `transit-pulse`) reads only
committed JSON and stays up; it costs nothing. No warehouse archive is kept (owner's decision,
2026-09-26): after step 3 the data exists only as the site snapshot and docs/06.

Run from the repo root with the usual AWS profile (`AWS_REGION=us-east-2`) and the Snowflake
key-pair env that `scripts/tf.sh` expects. Saved plans stay outside the repo. Destructive
commands are marked **[destroys]**; the checks between them are read-only.

## 0. Preconditions
- The resume case study is live and the screenshots in `assets/final/` are handed off (they are
  impossible to retake afterwards).
- `.env` still holds every key, including both Swiss OTD tokens (not re-issuable). Step 3
  deletes the SSM copies.

## 1. Freeze the orchestrator
```sh
aws ssm send-command --instance-ids i-0f0d6e32cb15ce471 --document-name AWS-RunShellScript \
  --parameters 'commands=["docker stop transit-dagster-daemon-1 transit-dagster-webserver-1"]'
```
Stops the twice-daily chains, the 2-hourly drains, the hourly weather MERGE (the only thing
still waking the Snowflake warehouse) and the freshness tripwire.

## 2. Empty the three data buckets  **[destroys]**
```sh
B=transit-pulse-$(aws sts get-caller-identity --query Account --output text)
for s in raw lakehouse artifacts; do aws s3api get-bucket-versioning --bucket $B-$s; done
for s in raw lakehouse artifacts; do aws s3 rm s3://$B-$s --recursive; done
```
If `get-bucket-versioning` prints `"Status": "Enabled"` for any of them, also delete every
object version and delete marker in that bucket (same loop as step 5). The buckets have no
`force_destroy`, so Terraform cannot remove them while anything is left inside.

## 3. Destroy the dev stack: AWS + Snowflake  **[destroys]**
```sh
scripts/tf.sh apply -destroy
```
`tf.sh apply` plans into a temp dir, prints the plan, waits 5 seconds and applies it. The plan
should list only `transit-pulse` resources:
- EC2: kafka t4g.small and services t4g.medium, with their EBS volumes
- EMR Serverless app `00g86oj9urdank0d`, drain Lambda, SQS DLQ, EventBridge Scheduler
- ECR repos (force_delete), SSM parameters, IAM roles and instance profiles
- VPC with subnets, security groups, IGW and the S3 endpoint
- Glue databases, SNS topic and subscription, the $90 budget
- Snowflake: database TRANSIT, warehouse TRANSFORM_XS, external volume TRANSIT_LAKEHOUSE,
  role TRANSIT_PIPELINE, user DAGSTER_SVC, grants
- the `transit-pulse-snowflake-reader` IAM role

If Snowflake ownership grants error on objects created outside Terraform, drop those in
Snowsight (step 4) and re-run.

Expect the first run to stop at 86 of 91: `snowflake_external_volume.lakehouse` cannot drop
while the dropped database's Iceberg tables sit in Time Travel (`DATA_RETENTION_TIME_IN_DAYS`
was 1), and the reader role and buckets depend on it, so Terraform skips them too. Nothing left
costs money. Re-run `scripts/tf.sh apply -destroy` once `show databases history like 'TRANSIT'`
returns nothing (about a day later); it removes the last 5.

## 4. Leftovers Terraform never owned  **[destroys]**
AWS:
```sh
aws cloudwatch describe-alarms --alarm-name-prefix transit-pulse --query 'MetricAlarms[].AlarmName'
aws cloudwatch delete-alarms --alarm-names transit-pulse-services-status-check \
  transit-pulse-services-autoreboot transit-pulse-services-cpu-wedge transit-pulse-kafka-status-check
aws logs delete-log-group --log-group-name /aws/lambda/transit-pulse-emr-drain
aws iam list-attached-role-policies --role-name transit-pulse-reminder-scheduler   # detach each, then:
aws iam delete-role --role-name transit-pulse-reminder-scheduler
aws ec2 describe-snapshots --owner-ids self --query 'Snapshots[].SnapshotId'           # delete any listed
```
Snowflake, as ACCOUNTADMIN in a worksheet:
```sql
drop catalog integration if exists TRANSIT_OBJ_STORE;
drop database if exists TRANSIT;            -- no-op if step 3 already did it
drop warehouse if exists TRANSFORM_XS;
drop user if exists DASHBOARD_SVC;
drop role if exists TRANSIT_READER;
show integrations; show users; show roles;  -- expect nothing TRANSIT-related
drop user if exists TERRAFORM_SVC;          -- last: the key in ~/.snowflake/keys belongs to it
```
The Snowflake account itself stays open (owner's decision). Dropped storage sits in Fail-safe
for 7 days at a few cents.

## 5. Terraform state bucket, then the IAM user  **[destroys]**
```sh
S=transit-pulse-tfstate-$(aws sts get-caller-identity --query Account --output text)
aws s3api list-object-versions --bucket $S --output json \
  --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][] }' \
  > /tmp/tfstate-versions.json
aws s3api delete-objects --bucket $S --delete file:///tmp/tfstate-versions.json
terraform -chdir=infra/bootstrap init     # only if infra/bootstrap/.terraform is gone
terraform -chdir=infra/bootstrap destroy
```
From a shell that cannot answer prompts (Claude Code's `!`), add `-auto-approve` after reading
the plan; otherwise the confirmation hits EOF and nothing is destroyed.
Finally, as the account root user in the console, delete IAM user `terraform-transit` (it holds
AdministratorAccess) with its access keys.

## 6. Verify zero spend
Every one of these should come back empty:
```sh
aws resourcegroupstaggingapi get-resources --region us-east-2 --query 'ResourceTagMappingList[].ResourceARN'
aws s3 ls | grep transit-pulse
aws ec2 describe-instances --filters Name=tag:Name,Values='transit-pulse*' --query 'Reservations[].Instances[].State.Name'
aws ec2 describe-volumes --query 'Volumes[].VolumeId'
aws ec2 describe-addresses --query 'Addresses[].PublicIp'
aws emr-serverless list-applications --query 'applications[].name'
aws ecr describe-repositories --query 'repositories[].repositoryName'
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/transit --query 'logGroups[].logGroupName'
```
Snowflake: `show databases like 'TRANSIT'; show warehouses like 'TRANSFORM%';` should return
nothing. About 48 hours later, Cost Explorer daily cost should read $0.00.

## After
- https://transit.gillu.me keeps serving the final snapshot (`site/data/*.json`, as_of 2026-09-26).
- `make site-data`, `make dashboard` and every `dbt` target need a warehouse that no longer
  exists; rebuilding means re-running `infra/` from bootstrap and re-accruing data (GTFS-RT
  history cannot be backfilled — docs/09).

## Run log
- 2026-09-26: step 1 (Dagster stopped), step 2 (raw/lakehouse/artifacts emptied; none were
  versioned), step 3 first pass (86 destroyed, 5 held by Time Travel), AWS leftovers in step 4
  (drain log group, reminder-scheduler role). The 4 CloudWatch alarms were already in state.
- 2026-09-28: step 3 second pass destroyed the last 5; dev state empty.
- 2026-10-02: tfstate bucket emptied (131 versions, 92 delete markers) and bootstrap destroyed.
  Final sweep: no buckets, instances, volumes, snapshots, addresses, EMR apps, ECR repos, log
  groups, Lambdas or transit IAM roles. The tagging API still lists the terminated EMR app for
  a while; it costs nothing.
