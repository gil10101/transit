# Operations runbook

State as of 2026-08-22 ~23:00Z. AWS account 622221238588, region **us-east-2**, prefix
`transit-pulse`. Snowflake `WQTEQYY-IB47757` (us-east-2). Repo github.com/gil10101/transit.
The laptop is optional; everything below runs in the cloud.

## System map

| Piece | Where | Identity |
|---|---|---|
| Pollers (nyc/boston/toronto/helsinki/dc/zurich at 30s, sf at 200s — 511's 60 req/hr cap; one container per city) + Dagster + postgres | EC2 t4g.medium, docker compose via systemd `transit.service` | instance `i-0f0d6e32cb15ce471` |
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
- Poller API keys (P3 batch 2): laptop `.env` holds `WMATA_API_KEY`, `BAY511_API_TOKEN`,
  `SWISS_OTD_TOKEN`, `SWISS_OTD_SA_TOKEN` (+ `CTA_API_KEY`, beta activation pending); the box
  reads them from SSM SecureStrings `/transit-pulse/keys/{wmata,bay511,swiss_rt,swiss_sa}`
  (see "Poller API keys" below). **The two Swiss tokens cannot be re-issued if revoked** —
  their only homes are `.env` (local) and SSM (cloud). Never print/echo/commit a key value.

## Health checks

```sh
# poller alive + polling (per city: transit-poller-1 is nyc; the P3 cities are
# transit-poller-{boston,toronto,helsinki,dc,sf,zurich}-1)
aws ssm send-command --instance-ids i-0f0d6e32cb15ce471 --document-name AWS-RunShellScript \
  --parameters 'commands=["docker logs transit-poller-1 2>&1 | tail -3"]' ...  # then get-command-invocation

# raw archive fresh (should be < 1 min old; endpoint names per city = the
# feed_groups keys in ingestion/config/cities/<city>.yaml, e.g. boston/trip_updates)
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
| Every 15 min (:10 :25 :40 :55) | `raw_feed_freshness` — boto3-only S3 listing of every **polled** city's endpoint prefixes (config-driven from the `feed_groups` keys in `ingestion/config/cities/*.yaml`: nyc 8, boston 3, toronto 3, helsinki 2, dc 6, sf 3, zurich 2 = 27). **[rev 2026-08-25]** The list is now `POLLED_CITIES`, not `LIVE_CITIES` (lib.py). Zurich sat in `LIVE_CITIES` from batch 2 so its national static would parse, but its poller was withheld pending the route allow-list — the tripwire asserted on two prefixes nothing ever wrote to and failed every 15 min for a day. Zurich's allow-list shipped 2026-08-25 and its poller deployed, so it is back to 27; the split remains so the next withheld poller cannot repeat it. Fails if any endpoint's newest object is older than 40 min (worst-case detection ~55 min after a kill; sf's 200s cadence still clears the 40-min bar by 12×). **[rev 2026-08-25] This 40-min bar is now a ceiling on per-endpoint cadence.** `feed_groups.<name>.poll_seconds` lets one feed poll slower than its city (zurich alerts at 600s = 10 min, clearing the bar by 4×); anything at or above 2400s would make the tripwire fail permanently on a healthy feed. Raise the threshold in `evaluate_feed_freshness` before setting a cadence that slow. **This is the killed-feed tripwire**; it never wakes the warehouse |
| Weekly Sun 09:00 (off the 2h :05 grid) | `gtfs_static` — EMR parse of each live city's static zip (7 cities since batch 2; sequential, one at a time on the 4 vCPU app; a failing city is skipped and reported, the rest still refresh) → refresh `gtfs_static_*` iceberg tables → dbt build --select "stg_gtfs__*+". Typically done well before the 10:05 chain; worst case (per-city 25-min timeouts) spills past it — that chain run fails on capacity and self-heals at its next 2h tick. Zurich's zip is the Swiss NATIONAL static (235 MB, largest of the set) — watch its first weekly run against the 25-min per-city EMR timeout. **Known-red until integrator work lands**: dc (WMATA static needs GET + `api_key` header — downloader has no auth support) and sf (no verified static URL yet) fail per-run as skipped-and-reported; docs/01 §D "OPEN" items |
| Hourly at :20 | `weather_hourly` — Open-Meteo forecast MERGE into `TRANSIT.SILVER.WEATHER_HOURLY` (all live cities: nyc, boston, toronto, helsinki, dc, sf, zurich) |
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

## P3 batch 1 rollout (boston / toronto / helsinki — built 2026-08-23)

Code and infra definitions are in the repo; nothing is live until the operator runs,
in this order:

1. `make deploy-images` — ingestion image picks up the new city yamls; dagster image
   bakes `ingestion/config` (freshness tripwire reads it) + the fan-out assets.
2. `make infra-plan` → review → `make infra-apply` — 4-city `TP_CITY_TZS` into the
   drain Lambda's `SPARK_PARAMS`/services env, and the services compose gains
   `poller-boston`/`poller-toronto`/`poller-helsinki`. Applying before the images are
   pushed leaves the new pollers crash-looping on the old image — push first.
3. Re-land user_data on the services box (compose changed): SSM shell →
   `cloud-init clean --logs && reboot` (procedure in "Dagster (P5)" above).
4. `make emr-drain` once — re-stages `spark_jobs.zip` so EMR static parses see the
   new city yamls (the zip bundles `ingestion/`).
5. Verify: per-city raw prefixes advancing (health checks above), `raw_feed_freshness`
   green on 16 endpoints, weekly `gtfs_static` run covers 4 cities, dbt marts show the
   new cities. Completeness ≥85% after 48h is the docs/04 acceptance gate.

The Dagster asset `gtfs_static_nyc` was renamed `gtfs_static` (multi-city); its
materialization history starts fresh under the new key.

## Poller API keys (P3 batch 2)

How a key reaches a poller container — no key value ever touches git, terraform
state, images, or logs:

1. Laptop `.env` (gitignored) holds the values. At apply time the operator exports
   them as `TF_VAR_*`; terraform writes them to SSM SecureStrings
   `/transit-pulse/keys/{wmata,bay511,swiss_rt,swiss_sa}` through **write-only args**
   (`value_wo`, same pattern as the dagster Snowflake key) so they never enter state.
   Without the `TF_VAR`s a plan/apply still converges — the parameter is seeded
   `PLACEHOLDER` and the value is set out-of-band (step below).
2. On the box, `transit.service` runs `/opt/transit/fetch_secrets.sh` as its first
   `ExecStartPre`: SSM `get-parameter --with-decryption` →
   `/opt/transit/secrets.env` (root, chmod 600). A missing/PLACEHOLDER parameter is
   skipped with a WARN naming the parameter only — keyless cities never stall on it.
3. The keyed pollers (`poller-dc`/`poller-sf`/`poller-zurich`) load the file via
   compose `env_file`; env names: `WMATA_API_KEY`, `BAY511_API_TOKEN`,
   `SWISS_OTD_TOKEN`, `SWISS_OTD_SA_TOKEN` (Swiss = one token per API product:
   gtfs-rt vs gtfs-sa).

**Rotate (or set) a key** — reference values only via `$VAR` expansion, NEVER echo:

```sh
set -a; source .env; set +a          # loads values into the shell, prints nothing
aws ssm put-parameter --name /transit-pulse/keys/wmata --type SecureString \
  --value "$WMATA_API_KEY" --overwrite       # bay511 / swiss_rt / swiss_sa likewise
aws ssm send-command --instance-ids i-0f0d6e32cb15ce471 --document-name AWS-RunShellScript \
  --parameters 'commands=["systemctl restart transit.service"]'   # re-fetch + restart
```

`value_wo` never reverts an out-of-band value (the provider doesn't read it back);
a terraform-side push requires bumping `value_wo_version` in
`infra/modules/services/main.tf` — the put-parameter path above is the normal one.
**The two Swiss tokens are irreplaceable** (opentransportdata.swiss does not re-issue):
"rotation" for them means only re-putting the same value if the parameter is lost.

## P3 batch 2 rollout (dc / sf / zurich — built 2026-08-23)

Keyed cities. Same shape as batch 1, plus the secrets step. Nothing is live until
the operator runs, in this order:

1. `make deploy-images` — ingestion image picks up the dc/sf/zurich yamls + keyed-auth
   adapter support; dagster image bakes `ingestion/config` (27-endpoint tripwire) and
   the 7-city fan-out. Push BEFORE applying or the new pollers crash-loop on the old image.
2. Export the key `TF_VAR`s from `.env` (values stay out of scrollback):
   ```sh
   set -a; source .env; set +a
   export TF_VAR_wmata_api_key="$WMATA_API_KEY" TF_VAR_bay511_api_token="$BAY511_API_TOKEN" \
          TF_VAR_swiss_otd_token="$SWISS_OTD_TOKEN" TF_VAR_swiss_otd_sa_token="$SWISS_OTD_SA_TOKEN"
   ```
   then `make infra-plan` → review → `make infra-apply`. This creates the 4 SSM
   SecureStrings (real values, write-only), grants the instance role read on them,
   extends `TP_CITY_TZS` to 7 cities in the drain Lambda's `SPARK_PARAMS`/services env,
   and adds `poller-dc`/`poller-sf`/`poller-zurich` (+ `fetch_secrets.sh`) to the
   services compose. `unset TF_VAR_wmata_api_key TF_VAR_bay511_api_token \
   TF_VAR_swiss_otd_token TF_VAR_swiss_otd_sa_token` afterwards.
3. Re-land user_data on the services box (compose + unit + fetch script changed):
   SSM shell → `cloud-init clean --logs && reboot` (procedure in "Dagster (P5)").
4. `make emr-drain` once — re-stages `spark_jobs.zip` so EMR sees the new city yamls.
5. **One-time backfill of the SKIPPED fix** (do this once, after the first post-deploy
   chain run): `dbt build --full-refresh --select fct_stop_events fct_route_reliability_daily`
   against `--target prod`. Both marts are incremental (`delete+insert` over a
   service_date lookback), and batch 2 changed a rule that applies to every city —
   a SKIPPED stop now scores no `otp_band` and drops out of route-day OTP. Without the
   full refresh, rows older than the lookback keep the old definition and the OTP series
   has a silent step at the deploy boundary. `assert_skipped_stops_have_no_otp_band`
   locks the invariant going forward.
6. Verify: `/opt/transit/secrets.env` exists on the box with mode 600 (`stat -c '%a'`,
   never `cat` it); per-city raw prefixes advancing — sf writes every ~200s by design
   (511's 60 req/hr cap), dc every 30s; `raw_feed_freshness` green on its endpoints;
   weekly `gtfs_static` refreshes nyc/boston/toronto/helsinki/dc (rail+bus, one version)
   /sf; dbt marts show the new cities. Completeness ≥85% after 48h is the docs/04
   acceptance gate. `uv run python scripts/field_audit.py` samples every hop per city
   (source → raw → silver → static → gold) and is the fastest way to see a new city's
   fields land.

Zurich is intentionally NOT in this rollout — its national feed needs the silver
allow-list first (docs/04 [rev P3 batch 2b]). Its key params, yaml and matcher branch
ship here; only the poller service is withheld.

Chicago joins as batch 2b when the CTA beta key activates: yaml + fixtures + append
to `LIVE_CITIES`/`TP_CITY_TZS`/compose, same checklist.

## Cost shape (measured 2026-08-24)

AWS Cost Explorer reports ~$0 for this account, so these are list prices computed from
measured usage. Roughly $99/month of the original ~$152 was EMR Serverless, because cost
tracks the NUMBER of drains far more than the volume drained: each run billed ~0.47
vCPU-hour for ~7 minutes of a 4-vCPU app, most of it Spark start-up. Two changes followed:

- **Drains are hourly, not every 15 minutes** (`drain_schedule`). Nothing downstream wanted
  15-minute freshness — gold is rebuilt by the 2-hour Dagster chain, and the killed-feed
  tripwire reads raw S3 prefixes, not silver. Expect roughly a 40–55% cut rather than a
  clean 4× — each hourly run drains 4× the data, so only the start-up share disappears.
  Re-measure with the sampler in `scripts/` rather than trusting the projection.
- **State-store retention is 5 versions, not 100** (`spark.sql.streaming.minBatchesToRetain`).
  The dedup state under `checkpoints/silver_stop_time_predictions/state/` had grown to
  13 GB in 23,686 objects — larger than every data table combined — while `offsets/` and
  `commits/` stayed correctly bounded at 100 files. Spark's maintenance thread prunes the
  old versions on subsequent runs; **never delete a live checkpoint by hand or with an S3
  lifecycle rule**, since losing offsets replays Kafka from the beginning and duplicates
  silver rows. Shuffle-partition count is deliberately untouched: a stateful streaming
  query cannot change it without discarding its checkpoint.

Everything else is small: EC2 + EBS ~$42/month (two ARM instances 24/7), Snowflake ~$6
(2.8 credits/30d on an XS that only runs during the chain), S3 ~$5 (24 GB stored, but the
bill is mostly PUT requests), Lambda/ECR/SSM under $1.

## Single points of failure and how to recover (added 2026-08-25)

This is a deliberately cheap single-instance deployment. That is a reasonable choice at
this budget, but it is only reasonable if the failure modes are written down. They are
here. Nothing below is automatic — every one of these needs a human.

| Component | What it is | If it dies | Recovery |
|---|---|---|---|
| **Services box** (`i-0f0d6e32cb15ce471`) | Runs EVERY poller *and* all of Dagster (webserver, daemon, postgres) | All ingestion and all orchestration stop. Raw archive stops growing; the warehouse goes stale but is not damaged. | `terraform apply` recreates it. Data already in Kafka survives; data not yet polled is **lost forever** — agencies do not backfill GTFS-RT. |
| **Kafka box** (`10.20.0.34`) | Single broker, no replication | Pollers fail to publish and log errors; raw archive keeps growing (it is written before Kafka). | Recreate, then replay from the raw archive rather than accepting the gap. |
| **Dagster postgres** | Run history + schedule state, in a docker volume on the services box | Schedules and run history lost | Volume survives a container restart and a reboot. It does NOT survive instance replacement. History is not business data — losing it costs visibility, not numbers. |
| **EMR Serverless app** | Stateless; state lives in S3 checkpoints | Drains fail | Re-submit. Checkpoints in S3 are the durable state, so nothing is lost. |
| **Snowflake** | Gold + external tables | Analysis stops | GOLD is fully rebuildable from silver by dbt; silver is rebuildable from the raw archive. |

**The honest summary: the raw S3 archive is the only irreplaceable thing.** Everything
downstream of it can be rebuilt. It is versioned, lifecycle-managed to IA at 30 days, and
deliberately never expired.

**The gap that is NOT acceptable long-term:** the services box is a single point of
failure for both ingestion and orchestration, with no auto-recovery. If it terminates at
02:00 nobody finds out until someone looks — the freshness tripwire runs *on that same
box*, so it dies with it. An external heartbeat (a CloudWatch alarm on raw-bucket object
count, which is outside the box) is the smallest thing that would close this. Not built.

## Known quirks (cost real debugging time — do not rediscover)

0. **A `transit-drain` stuck in RUNNING for more than ~10 minutes means the EMR app is
   deadlocked, not busy.** Every job is a 2-vCPU driver plus a 2-vCPU executor, so the old
   4-vCPU `maximum_capacity` admitted two drivers and left nothing for either executor:
   both jobs spin forever re-requesting a worker ("Worker could not be allocated as the
   application has exceeded maximumCapacity" — 287 times in a 4.5-hour hang on 2026-08-24)
   and every later submit is rejected. Silver went 18 hours stale before anyone noticed.
   `max_vcpu` is 8 now so a drain and the weekly static parse coexist. If it ever recurs:
   `aws emr-serverless list-job-runs --states RUNNING` and cancel the oldest — the drain is
   checkpointed, so the next one resumes where it stopped. Watch it with
   `scripts/field_audit.py silver` (the `fresh_utc` column is the giveaway).
   **Changing `maximum_capacity` needs an idle, STOPPED application**, and the 15-minute
   drain schedule refills it faster than terraform can act. Open a window first:
   ```sh
   aws scheduler update-schedule --name transit-pulse-emr-drain --region us-east-2 \
     --state DISABLED --schedule-expression 'rate(15 minutes)' --flexible-time-window Mode=OFF \
     --target 'Arn=arn:aws:lambda:us-east-2:622221238588:function:transit-pulse-emr-drain,RoleArn=arn:aws:iam::622221238588:role/transit-pulse-emr-scheduler'
   ```
   wait for the running job to finish, `aws emr-serverless stop-application`, apply, then
   re-run the same command with `--state ENABLED`. The app restarts by itself on the next
   submit. Re-enabling the schedule is the step that is easy to forget — silver simply
   stops advancing, with nothing in the logs to say why.
0b. **`CONCURRENT_STREAM_LOG_UPDATE — Multiple streaming jobs detected` means two drains
   ran at once.** Every drain writes the same Spark checkpoint, and Structured Streaming
   allows exactly one writer per checkpoint location. On 2026-08-24 four drains stacked up
   (the 15-minute schedule kept firing while a catch-up run was still going) and the
   survivor died after 58 minutes of work with this error. The drain Lambda now lists
   active runs and skips the fire when one is in flight, so this should not recur; if it
   does, cancel all but the oldest — the checkpoint is contended, not corrupted, and a
   single fresh drain resumes from the last committed offset. Note the failure is reported
   as `Target log directory already exists` because EMR retries the driver in the same
   container: the retry's error masks the real one, so always read the driver log for the
   FIRST exception rather than trusting `stateDetails`.
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

## Next (per docs/04)

P5 landed: Dagster chain + checks, supplemented GTFS, headways/EWT + P5 marts, weather.
To enable in prod: generate the DAGSTER_SVC key pair (procedure above), apply, deploy the
dagster image. P3 batch 1 (boston/toronto/helsinki, keyless) and batch 2 (dc/sf/zurich,
keyed) are built — rollout checklists above; each pending its 48h completeness gate.
Next: chicago (batch 2b, once the CTA GTFS-RT beta key activates), P4 Tokyo,
P6 scorecard/dashboard + SCD2 dims.
