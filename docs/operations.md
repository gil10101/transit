# Operations runbook

State as of 2026-08-22 ~23:00Z. AWS account 622221238588, region **us-east-2**, prefix
`transit-pulse`. Snowflake `WQTEQYY-IB47757` (us-east-2). Repo github.com/gil10101/transit.
The laptop is optional; everything below runs in the cloud.

## System map

| Piece | Where | Identity |
|---|---|---|
| Poller (NYC, 30s) + Dagster + postgres | EC2 t4g.medium, docker compose via systemd `transit.service` | instance `i-0f0d6e32cb15ce471` |
| Kafka (KRaft single broker) | EC2 t4g.small, docker `apache/kafka:3.8.0` | private `10.20.0.34:9092` |
| Bronze/silver Spark drains | EMR Serverless app `00g86oj9urdank0d`; EventBridge schedule `transit-pulse-emr-drain` (15 min) -> Lambda `transit-pulse-emr-drain` -> StartJobRun (`availableNow` trigger, exits when caught up); failures land in SQS DLQ `transit-pulse-emr-drain-dlq` | exec role `transit-pulse-emr-exec` |
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
- Snowflake (P5): service user `DAGSTER_SVC` (TYPE=SERVICE, role `TRANSIT_PIPELINE`,
  `TIMEZONE='UTC'`, warehouse `TRANSFORM_XS`) — used only by Dagster on the services box;
  private key lives in SSM, never on the laptop (see "Dagster (P5)" below)

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
| Re-pin Snowflake Iceberg metadata (automated: Dagster 2h chain since P5; manual fallback) | `make snowflake-refresh` |
| Build gold on Snowflake (automated: Dagster 2h chain since P5; manual fallback) | `cd dbt/transit && uv run dbt build --target prod --profiles-dir .` |
| Local dev loop | `make up` → `make poll-nyc` / `make spark-local` → `make dbt-build` (duckdb) |
| Rebuild/push service images (then restart via SSM `systemctl restart transit.service`) | `make deploy-images` (both) / `make dagster-deploy` (Dagster image only) |
| One-off 2yr weather backfill (manual by design) | `make p5-backfill-weather` (see Dagster section for env prereqs) |
| Infra change | edit `infra/`, `make infra-plan` → review → `make infra-apply` (export Snowflake env first) |
| Dagster UI (not internet-exposed by design) | `aws ssm start-session --target i-0f0d6e32cb15ce471 --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["3000"],"localPortNumber":["3070"]}'` |
| Shell on a box | `aws ssm start-session --target <instance-id>` |

## Dagster (P5)

Dagster (webserver + daemon containers on the services box) owns the warehouse-side
pipeline. The EventBridge 15-min drain stays — Dagster's chain drains again on its own
cadence (drains are idempotent `availableNow` catch-ups).

| Schedule (UTC) | What runs |
|---|---|
| Every 2h at :05 | `emr_drain` → `snowflake_iceberg_refresh` (re-pin metadata) → dbt build (gold) → warehouse asset checks (gold row growth during service hours; silver `max(fetched_at)` < 3h) |
| Every 15 min (:10 :25 :40 :55) | `raw_feed_freshness` — boto3-only S3 listing of the 8 NYC endpoint prefixes; fails if any endpoint's newest object is older than 40 min (worst-case detection ~55 min after a kill). **This is the killed-feed tripwire**; it never wakes the warehouse |
| Weekly Sun 09:00 (off the 2h :05 grid) | `gtfs_static_nyc` — EMR parse of the supplemented static zip → refresh `gtfs_static_*` iceberg tables → dbt build --select "stg_gtfs__*+" |
| Hourly at :20 | `weather_hourly` — Open-Meteo forecast MERGE into `TRANSIT.SILVER.WEATHER_HOURLY` |
| Manual only | `weather_backfill_2yr` (`make p5-backfill-weather`) — Open-Meteo archive API, chunked by year |

- **UI**: not internet-exposed; port-forward via SSM (row above), then http://localhost:3070.
- **Identity**: Snowflake `DAGSTER_SVC` (TYPE=SERVICE, key-pair), default role
  `TRANSIT_PIPELINE`, warehouse `TRANSFORM_XS`, `TIMEZONE='UTC'` (quirk 1 — pinned by
  terraform). `TRANSIT_PIPELINE` **owns** schema `SILVER` + its iceberg tables and GOLD
  tables/views (`ALTER ICEBERG TABLE … REFRESH` is OWNERSHIP-only; dbt `CREATE OR REPLACE`
  likewise) and is granted to ACCOUNTADMIN, so `TERRAFORM_SVC` paths keep working.
  EMR/S3/SSM access rides the instance role (`transit-pulse-services`) via IMDS.
- **How the key reaches the box**: private key is NOT in git, tf state, or the image.
  SSM SecureString `/transit-pulse/dagster/snowflake_key` holds it (terraform writes a
  `PLACEHOLDER` via a write-only arg, so the real value never enters tf state); the
  container entrypoint runs
  `aws ssm get-parameter --with-decryption` at start and writes
  `/home/appuser/.snowflake/dagster_key.p8` (chmod 600, env `SNOWFLAKE_PRIVATE_KEY_PATH`).
- **Enable once** (operator): generate pair, register public half, upload private half:
  ```sh
  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out dagster_key.p8 -nocrypt
  openssl rsa -in dagster_key.p8 -pubout -out dagster_key.pub
  # 1) paste dagster_key.pub body (no BEGIN/END lines) into infra/envs/dev/terraform.tfvars
  #    dagster_public_key = "MIIB..." ; make infra-plan / infra-apply creates DAGSTER_SVC
  aws ssm put-parameter --name /transit-pulse/dagster/snowflake_key \
    --type SecureString --value "file://dagster_key.p8" --overwrite
  # 2) shred the local private copy; SSM is the only home
  # 3) restart containers: aws ssm ... 'systemctl restart transit.service'
  ```
- **user_data changed?** cloud-init runs once per instance, so an updated compose/systemd
  unit does not land by itself. Re-land with SSM shell:
  `cloud-init clean --logs && reboot` (or copy the rendered files by hand and
  `systemctl daemon-reload && systemctl restart transit.service`).
- **Local compose**: dagster UI must come up with an empty `.env` — cloud-touching assets
  fail lazily at materialize time only.

## Known quirks (cost real debugging time — do not rediscover)

1. **Snowflake session timezone defaults to America/Los_Angeles.** NTZ-vs-TZ comparisons
   silently skew delays by hours (OTP read 3.9% instead of 74%). `TERRAFORM_SVC` has
   `TIMEZONE='UTC'` set; any new user/tool needs the same (`DAGSTER_SVC` gets it pinned
   by terraform).
2. **This Snowflake account rejects dbt's SHOW metadata grammar** (`show terse schemas ... limit`,
   `show objects in <schema>`). `dbt/transit/macros/snowflake_overrides.sql` reimplements the
   adapter contracts from information_schema. Remove only if a future dbt-snowflake works.
3. **EMR Serverless**: python has no yaml/dotenv (session.py lazy-imports; tz map injected via
   `TP_CITY_TZS`); VPC ENIs have no internet (Kafka connector jars staged from artifacts bucket);
   default driver is 4 vCPU (starves a 4 vCPU app cap — drains pin 2+2); STREAMING mode rejects
   retry-policy; continuous streaming costs ~10x drains.
4. **EventBridge Scheduler cannot call emrserverless:startJobRun directly**: create-time
   validation demands PascalCase incl. ClientToken, but `<aws.scheduler.execution-id>` is NOT
   substituted at fire time -> literal angle brackets fail the API token regex on every
   invocation (visible only via TargetErrorCount metric / DLQ). Hence the Lambda invoker
   (`infra/modules/spark/lambda/drain.py`) generating a uuid clientToken per run.
5. **MTA quirks** live in `docs/01-data-dictionary.md` §B (L-feed delay exception, trip_id
   matching, direction from `..N/..S`, weekend service changes cause the audited
   delay-bounds warns → supplemented GTFS at P5).
6. **Docker Desktop on this laptop**: `credsStore: desktop` deadlocked all pulls once;
   removed (backup `~/.docker/config.json.bak-transit`).
7. Iceberg tables in Snowflake are object-store cataloged: they do NOT auto-follow new
   snapshots — re-pin to the latest version-hint required (Dagster's 2h chain does it
   since P5; `make snowflake-refresh` is the manual fallback).

## Cost model

EC2 ~$41 (kafka+services+EBS), EMR drains ~$15-30 (96 short runs/day), S3+Glue+scheduler <$5,
Snowflake trial credits now (~$20-25/mo post-trial). Total ~$60-75 now, ~$80-95 post-trial.
Ceiling: budget alarm $90.

## Next (post-P5, per docs/04)

P5 landed: Dagster chain + checks, supplemented GTFS, headways/EWT + P5 marts, weather.
To enable in prod: generate the DAGSTER_SVC key pair (procedure above), apply, deploy the
dagster image. Next phases: P3 GTFS-RT fan-out (keys in .env.example), P4 Tokyo,
P6 scorecard/dashboard + SCD2 dims.
