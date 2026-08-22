# Operations runbook

State as of 2026-08-22 ~23:00Z. AWS account 622221238588, region **us-east-2**, prefix
`transit-pulse`. Snowflake `WQTEQYY-IB47757` (us-east-2). Repo github.com/gil10101/transit.
The laptop is optional; everything below runs in the cloud.

## System map

| Piece | Where | Identity |
|---|---|---|
| Poller (NYC, 30s) + Dagster + postgres | EC2 t4g.medium, docker compose via systemd `transit.service` | instance `i-0f0d6e32cb15ce471` |
| Kafka (KRaft single broker) | EC2 t4g.small, docker `apache/kafka:3.8.0` | private `10.20.0.34:9092` |
| Bronze/silver Spark drains | EMR Serverless app `00g86oj9urdank0d`, EventBridge schedule `transit-pulse-emr-drain` every 15 min, `availableNow` trigger, exits when caught up | exec role `transit-pulse-emr-exec` |
| Lake (Iceberg, Hadoop catalog) | `s3://transit-pulse-622221238588-lakehouse/iceberg/{bronze,silver}` | version-hint.text per table |
| Raw archive | `s3://transit-pulse-622221238588-raw/<city>/<endpoint>/<date>/<hour>/` | includes migrated local history |
| Jars + code + EMR logs | `s3://transit-pulse-622221238588-artifacts/{jars,code,emr-logs}` | code zip re-staged by `make emr-drain` |
| Warehouse | Snowflake `TRANSIT.SILVER` (external Iceberg) + `TRANSIT.GOLD` (dbt) | warehouse `TRANSFORM_XS`, auto-suspend 60s |
| Budget | AWS Budgets $90/mo, email alerts 80% actual / 100% forecast | |

## Credentials (never in git)

- AWS: `~/.aws/credentials` (IAM user `terraform-transit`, AdministratorAccess)
- Snowflake: service user `TERRAFORM_SVC` (TYPE=SERVICE, ACCOUNTADMIN, MFA-exempt),
  RSA key `~/.snowflake/keys/transit_terraform_key.p8`; snow CLI connection `transit-svc`;
  user defaults: `TIMEZONE='UTC'`, `DEFAULT_WAREHOUSE='TRANSFORM_XS'` (both load-bearing)
- Terraform Snowflake provider env comes from `.env` + `SNOWFLAKE_PRIVATE_KEY="$(cat ~/.snowflake/keys/transit_terraform_key.p8)"`

## Health checks

```sh
# poller alive + polling
aws ssm send-command --instance-ids i-0f0d6e32cb15ce471 --document-name AWS-RunShellScript \
  --parameters 'commands=["docker logs transit-poller-1 2>&1 | tail -3"]' ...  # then get-command-invocation

# raw archive fresh (should be < 1 min old)
aws s3 ls s3://transit-pulse-622221238588-raw/nyc/base/$(date -u +%Y-%m-%d)/ --recursive | tail -1

# drains healthy (expect transit-drain SUCCESS every ~15 min)
aws emr-serverless list-job-runs --application-id 00g86oj9urdank0d \
  --query 'jobRuns[0:4].[name,state,createdAt]' --output text

# iceberg commits advancing
aws s3 cp s3://transit-pulse-622221238588-lakehouse/iceberg/silver/stop_time_predictions/metadata/version-hint.text -

# warehouse sees fresh data
snow sql -q "select count(*), max(fetched_at) from TRANSIT.SILVER.STOP_TIME_PREDICTIONS" --connection transit-svc
```

## Routine operations

| Task | Command |
|---|---|
| Manual drain now (also re-stages code zip + jars) | `make emr-drain` |
| Re-pin Snowflake Iceberg metadata after drains (manual until Dagster P5) | `make snowflake-refresh` |
| Build gold on Snowflake | `cd dbt/transit && uv run dbt build --target prod --profiles-dir .` |
| Local dev loop | `make up` → `make poll-nyc` / `make spark-local` → `make dbt-build` (duckdb) |
| Rebuild/push service images (then restart via SSM `systemctl restart transit.service`) | `make deploy-images` |
| Infra change | edit `infra/`, `make infra-plan` → review → `make infra-apply` (export Snowflake env first) |
| Dagster UI (not internet-exposed by design) | `aws ssm start-session --target i-0f0d6e32cb15ce471 --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["3000"],"localPortNumber":["3070"]}'` |
| Shell on a box | `aws ssm start-session --target <instance-id>` |

## Known quirks (cost real debugging time — do not rediscover)

1. **Snowflake session timezone defaults to America/Los_Angeles.** NTZ-vs-TZ comparisons
   silently skew delays by hours (OTP read 3.9% instead of 74%). `TERRAFORM_SVC` has
   `TIMEZONE='UTC'` set; any new user/tool needs the same.
2. **This Snowflake account rejects dbt's SHOW metadata grammar** (`show terse schemas ... limit`,
   `show objects in <schema>`). `dbt/transit/macros/snowflake_overrides.sql` reimplements the
   adapter contracts from information_schema. Remove only if a future dbt-snowflake works.
3. **EMR Serverless**: python has no yaml/dotenv (session.py lazy-imports; tz map injected via
   `TP_CITY_TZS`); VPC ENIs have no internet (Kafka connector jars staged from artifacts bucket);
   default driver is 4 vCPU (starves a 4 vCPU app cap — drains pin 2+2); STREAMING mode rejects
   retry-policy; continuous streaming costs ~10x drains.
4. **EventBridge → StartJobRun universal target requires `ClientToken` =
   `<aws.scheduler.execution-id>`** in the input JSON.
5. **MTA quirks** live in `docs/01-data-dictionary.md` §B (L-feed delay exception, trip_id
   matching, direction from `..N/..S`, weekend service changes cause the audited
   delay-bounds warns → supplemented GTFS at P5).
6. **Docker Desktop on this laptop**: `credsStore: desktop` deadlocked all pulls once;
   removed (backup `~/.docker/config.json.bak-transit`).
7. Iceberg tables in Snowflake are object-store cataloged: they do NOT auto-follow new
   snapshots — `make snowflake-refresh` re-pins to the latest version-hint.

## Cost model

EC2 ~$41 (kafka+services+EBS), EMR drains ~$15-30 (96 short runs/day), S3+Glue+scheduler <$5,
Snowflake trial credits now (~$20-25/mo post-trial). Total ~$60-75 now, ~$80-95 post-trial.
Ceiling: budget alarm $90.

## Next (P5 per docs/04)

Dagster assets: drain→refresh→dbt chain, GTFS weekly refresh, freshness/completeness checks;
supplemented GTFS; headways/EWT; fan-out cities (keys in .env.example).
