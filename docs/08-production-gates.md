# 08 — Production readiness gates

The bar this project has to clear before it is "in production" rather than "running".

Each gate has a **threshold that can be measured**, not a feeling. A gate is PASS only
when someone has actually measured it — "no known problems" is not a pass, it is an
unmeasured gate.

Status column: **PASS** / **FAIL** / **UNMEASURED**.

Established 2026-08-25. Re-measure before declaring any phase complete.

---

## A — Correctness

| # | Gate | Threshold | Status |
|---|---|---|---|
| A1 | Every canonical rule (CLAUDE.md) has exactly one implementation | 1 live implementation each; no dead duplicates | **PASS** |
| A2 | dbt tests clean on data that is fair to judge | 0 errors on closed local days at/after `metrics_from` | **PASS** (145 pass / 5 warn / 0 error) |
| A3 | CI gates green | `ruff check` + `ruff format --check` + `pytest` all pass | **PASS** |
| A4 | Every city verified field-by-field at every hop | raw → silver → gold sampled per city | **PASS** |
| A5 | No test that cannot fail | every test has a case that would trip it | **PARTIAL** |
| A6 | dbt runs in CI | modified models built against duckdb on PR | **PASS** (parse+compile+seed) |

**A1** — the service-date rule exists three times: Spark SQL in `silver_normalize.py`
(live), Python in `spark_jobs/timeutils.py` (tested, never imported by production), and
the dbt macro `service_date_of` (unused). The tests exercise the dead copy, so they prove
nothing about the live path. Fix: delete the dead copies or make production use the shared
one.

**A2** — 1 error, on 15 Helsinki route-days. 11 are genuine zero-coverage routes (ferries,
tram 100H) that no code change can fix. The error is honest but permanently red, which
collides with D2. Fix: exclude routes with a documented structural gap from the assertion,
so the test measures what we control.

**A6** — CI runs Python only. `docs/03-dbt-spec.md` specifies a slim dbt CI against duckdb
and it was never built, so a broken model reaches production undetected.

---

## B — Data quality

| # | Gate | Threshold | Status |
|---|---|---|---|
| B1 | Completeness per city on closed days | ≥85% (P3 acceptance criterion) | **UNMEASURED** |
| B2 | Share of events carrying a delay | ≥90% per city | **PASS** |
| B3 | Every scheduled route observed, or documented as a known gap | 100% accounted for | **PASS** |
| B4 | No future-dated service days in judged marts | 0 rows | **PASS** (measured 0) |
| B5 | Grain uniqueness on the atomic fact | 0 duplicates | **PASS** |

**B1** — only Helsinki (95.9%) and NYC (95.2%) have a finished judged day. Boston,
Toronto, DC, SF and Zurich have none, so four of seven cities have never been measured
against the criterion they were accepted on.

**B2** — nyc 100%, dc 100%, helsinki 100%, sf 98.1%, toronto 96.8%, boston 94.5%.

**B4** — `fct_service_delivery_daily` carries Helsinki service dates up to 2026-08-26, a
day in the future, generated from the static calendar with zero observed trips. They are
excluded from tests by `service_day_closed`, so they cause no false failure — but a future
date in a fact table is wrong on its face and will mislead any chart built on it.

---

## C — Reliability

| # | Gate | Threshold | Status |
|---|---|---|---|
| C1 | Scheduled jobs succeed | ≥95% over a rolling 7 days | **FAIL** |
| C2 | No permanently-red check | 0 chronically failing jobs | **PASS** |
| C3 | Transient failures self-heal | no manual step for a single-run failure | **PASS** |
| C4 | Single points of failure documented with a tested recovery path | every SPOF has a runbook | **PARTIAL** (documented, untested) |

**C1/C2** — `raw_feed_freshness_job` failed every 15 minutes for roughly a day (Zurich
was asserted on before its poller existed). Fixed 2026-08-25. The rate needs re-measuring
over a clean 7-day window before this can pass.

**C4** — Kafka and the services box are both standalone EC2 instances with no auto-scaling
group and no automatic replacement. If either terminates, ingestion stops until a human
intervenes. The services box additionally runs *every* poller plus all of Dagster, so it
is a single point of failure for the entire pipeline. This may well be an acceptable
tradeoff at this cost target — but it has to be a written decision with a recovery
runbook, not an accident.

---

## D — Observability

| # | Gate | Threshold | Status |
|---|---|---|---|
| D1 | A human is notified when the pipeline actually breaks | alert within 60 min of a real failure | **PASS** (confirmed 2026-08-26) |
| D2 | No alert fatigue | 0 chronically failing checks | **FAIL** |
| D3 | Every hop's health is measurable without a human reading logs | one command per hop | **PASS** |

**D1 is the most serious gap in this document.** There is no run-failure sensor, no SNS
topic for pipeline health, no Slack or email hook — nothing. The only alert in the entire
system is an AWS **budget** email. Detection exists (the tripwire works); notification does
not. The proof is that the tripwire failed every 15 minutes for a full day and it was found
by manual inspection, not by anyone being told.

---

## E — Cost

| # | Gate | Threshold | Status |
|---|---|---|---|
| E1 | Billed spend | <$100/month with headroom | **PASS** |
| E2 | Every growing store has a retention policy | 0 unbounded stores | **PASS** (applied 2026-08-25 12:04Z) |
| E3 | No known systematic waste | 0 identified-and-unfixed | **PASS** |

**E1** — gross usage $22.38 for 1–25 August, fully covered by credits, **$0.00 billed**.

**E2** — **corrected 2026-08-25.** An earlier draft of this gate claimed the raw archive
had no policy. It does: `infra/modules/lake/main.tf` transitions it to STANDARD_IA at 30
days with an explicit "history is the asset, never expire" decision. That is a deliberate
choice, correctly recorded, and it stands.

The one genuine oversight was EMR debug logs — 3.4 GB in three days, read by nobody after
the week they were produced. Now expiring at 14 days, plus an abort rule for incomplete
multipart uploads, which otherwise linger invisibly and are billed forever.

**E3** — Zurich's duplicate-alerts waste (29 GB/day of a byte-identical file) was found
and fixed on 2026-08-25.

---

## F — Security

| # | Gate | Threshold | Status |
|---|---|---|---|
| F1 | No secret in Terraform state | 0 occurrences | **PASS** |
| F2 | Secrets stored encrypted with least-privilege access | all SecureString | **PASS** |
| F3 | No secret printed by any code path | 0 | **PASS** |
| F4 | No secret in git history or the working tree | 0 objects | **PASS** (after remediation) |
| F5 | Least-privilege IAM | no wildcard on privilege-granting actions | **PASS** (applied 2026-08-25 12:04Z) |

All five deployed secrets are SSM `SecureString`, and the write-only (`value_wo`) pattern
means no plaintext value appears in Terraform **state** — verified by parsing it. City YAML
files carry only environment-variable *names*, never values.

**F4 — an incident, found by review on 2026-08-25 and remediated the same hour.** Saved
Terraform plan files (`tfplan.zurich`, `tfplan.zurich2`) were committed to git carrying the
live `WMATA_API_KEY`, `BAY511_API_TOKEN`, `SWISS_OTD_TOKEN` and `SWISS_OTD_SA_TOKEN` in
cleartext. Two things caused it:

1. **A saved plan embeds root-module variable values verbatim.** `sensitive = true` redacts
   CLI *display* only; it does nothing to the plan archive. This is not widely known and is
   the actual trap.
2. **`.gitignore` said `tfplan`, which matches only that exact filename** — not
   `tfplan.zurich`. Now `tfplan*`.

Remediation: untracked, history rewritten across the 25 unpushed commits, reflog expired,
`git gc --prune=now`, and **verified 0 of 1,031 remaining git objects contain any token**.
Plan files deleted from disk. `origin/main` was `d89d601`, which predates all of it, so
**nothing ever left the machine** — the exposure was local-only.

**Standing rule from this:** never write a saved plan inside the repo. Use
`-out=$(mktemp -d)/plan`, or at minimum confirm `git check-ignore` covers it first.

**F5** — `infra/modules/services/main.tf` grants `iam:PassRole` on `Resource = "*"`. The
same grant is correctly scoped to the EMR execution role ARN in the spark module, and that
ARN is already a variable in this module, so it is a one-line fix.

---

## G — Structure and maintainability

| # | Gate | Threshold | Status |
|---|---|---|---|
| G1 | No dead code or dead config | 0 unreferenced symbols | **PARTIAL** |
| G2 | Docs match reality | 0 contradictions | **UNMEASURED** |
| G3 | Reproducible from a clean clone | documented, and the doc is correct | **UNMEASURED** |
| G4 | No orphaned warehouse objects | every table maps to a live model | **PASS** |

**G1** — known dead: dbt vars `completeness_enforced_from` and
`completeness_exclusion_threshold`; the dbt macro `service_date_of`; the Python module
`spark_jobs/timeutils.py`; the `stg_gtfs__stops` model; and two silver tables with no
consumer (`gtfs_static_shapes`, `weather_hourly`). The last three are deliberate
(pre-built for a later phase) and should be *labelled* as such rather than deleted, so
the distinction between "dead" and "not wired up yet" is explicit.

**G4** — all 23 gold objects map to a live dbt model; no orphans.

---

## Summary

| Area | Pass | Fail | Unmeasured |
|---|---:|---:|---:|
| A Correctness | 2 | 3 | 1 |
| B Data quality | 3 | 1 | 1 |
| C Reliability | 1 | 3 | 0 |
| D Observability | 1 | 2 | 0 |
| E Cost | 2 | 1 | 0 |
| F Security | 3 | 0 | 0 |
| G Structure | 1 | 1 | 2 |
| **Total** | **13** | **11** | **4** |

**Not production-ready.** Security and cost are in good shape. The blocking themes are:

1. **Nobody gets told when it breaks** (D1) — the single most important fix.
2. **Permanently-red checks** (A2, C2) train everyone to ignore failures.
3. **Four of seven cities have never been measured** against the criterion they were
   accepted on (B1).
4. **The canonical service-date rule has three implementations** and the tests cover the
   dead one (A1).

---

## Verification log — 2026-08-25

What was actually measured, not asserted. Every figure came from a live query or a real
build after the fixes shipped.

### Fixed and verified in production

| What was wrong | Evidence it is fixed |
|---|---|
| Zurich's allow-list applied to realtime only; the schedule side carried the whole Swiss network | `stg_gtfs__routes` for zurich: **5,122 → 615 routes** |
| `fct_service_delivery_daily` carried future-dated rows | rows with `service_date > current_date`: **0** |
| Completeness error blocked every chain run | prod `dbt build`: **145 pass / 5 warn / 0 error / 0 skip** |
| Freshness tripwire failed every 15 min | SUCCESS at 03:25, 03:40, 03:55, 04:10, 04:25 |
| Service-date rule had three Spark implementations, two wrong for Toronto | one helper, constants in `spark_jobs/timeutils.py`, pinned by test |
| `field_audit.py` under-reported OTP by up to 4.3 points | corrected formula now tracks the canonical mart; it had shown healthy Zurich as **18.5% instead of 95.8%** |
| Live API tokens committed in Terraform plan files | **0 of 1,031** git objects contain a token; nothing was ever pushed |

### Zurich, verified end to end
124 gold events looked alarming against 165k silver predictions. It is **124 of 124
eligible — 100% conversion**. Zurich's feed publishes ~2 hours ahead and it started at
03:08Z, so nearly everything is still ahead of the 20-minute finalize horizon. Trip
matching is 4,672 trips, and **0 rows outside the allow-list** reached silver.

### Known and bounded: the SF phantom service day

**8,214 events remain in `fct_stop_events` under `sf` / `2026-08-23`, a day on which SF
produced no data at all.** Caused by the −12h cutover default; fixed at source, but the
fix corrects new silver writes only and the historical rows persist.

Contained, and worth being precise about why:
- `sf.metrics_from` is **2026-08-25**, so the phantom day is excluded from every scored
  metric and every completeness assertion.
- It is *not* excluded from all-time aggregates, so any SF figure quoted without a
  `metrics_from` filter — including those in `docs/06` — is contaminated by it.

A new guard, `assert_prev_day_events_are_overnight`, catches this class permanently. The
signal is unambiguous: SF has **7,119 previous-day rows, every one arriving between 09:00
and 20:00 local**, where Helsinki (34,282), NYC (4,980) and Boston (14,128) all cluster in
hours 0–4 and Toronto has 2 daytime rows in 97,445. It ships at **warn**, because at error
severity it would block every chain for a defect already fixed at source.

**Open decision — needs a human.** Three ways to clear the backlog, each with a real cost:
1. **Delete** the misdated silver rows. Simple, but loses ~375 trips of genuine morning
   observations that exist nowhere else in silver.
2. **Re-date** them in place. Correct in principle, but 321 trip_ids already carry both
   service dates, so a merge could collide on the atomic fact's unique key.
3. **Replay from the raw archive.** The only lossless option, and the most work.

Until one is chosen, the rows stay, excluded from scoring and flagged by the new test.

---

## Deploy-completeness: two traps that both produced silent staleness

Both of these were found by *checking*, never by anything going red. They share a shape —
the code is correct and committed, and production is running something else.

**1. The dbt project is baked into the dagster image.** A dbt change committed after
`make deploy-images` does not reach production; the chain keeps running the previous
tests. On 2026-08-25 a test fixed at 04:44 was pushed at 04:33, so the 06:05 and 08:05
chains both failed at the dbt step *while every EMR drain reported SUCCESS* and gold
silently stopped updating for four hours. Nothing alerted, because nothing was watching
the right thing.

  **Check after any dbt change:** `max(last_altered)` on `TRANSIT.GOLD.FCT_STOP_EVENTS`
  should advance within one chain tick. A green drain says nothing about gold.

**2. `terraform apply` with `-target` silently skips everything else.** Terraform warns
"Applied changes may be incomplete" and it means it. The 12:04Z apply landed SNS, the
lifecycle rule and the IAM scope — and skipped `module.spark.aws_lambda_function.drain`
entirely, leaving the running Lambda on the OLD `ACTIVE_STATES` (missing `QUEUED` and
`CANCELLING`) and the old non-idempotent `clientToken`. That is the guard against two
drains sharing one Spark checkpoint, which previously destroyed 58 minutes of data.

  **[rev 2026-08-25] `-target` is no longer needed.** It was only ever required because
  the Snowflake provider failed to authenticate, which made `terraform plan` fail outright
  and left drift invisible. Two things were wrong, both recorded here because the comment
  in `providers.tf` asserted the opposite: the provider was configured for PASSWORD auth
  while `TERRAFORM_SVC` is key-pair only, and `.env` defines `SNOWFLAKE_PASSWORD`, which
  the provider reads automatically and which errors "password: conflicts with private_key".
  The provider is also NOT unused — `enable_snowflake` is true and it manages live grants.
  `make infra-plan` (scripts/tf.sh) now sets this correctly and reports drift. **Always
  finish with an
  untargeted `terraform plan` and confirm it reports no changes.** That is the only way
  to know what targeting skipped.

## D1 — PASSES as of 2026-08-26

Every link in the chain verified independently, not inferred from the others:

| Link | How it was verified |
|---|---|
| Topic exists | `arn:aws:sns:us-east-2:622221238588:transit-pulse-pipeline-alerts` |
| Subscription is real, not pending | `list-subscriptions-by-topic` returns a subscription ARN, not `PendingConfirmation` |
| The box may publish to it | `iam simulate-principal-policy` on `transit-pulse-services` → `sns:Publish` **allowed**, scoped to that one ARN |
| The container can see the topic | `PIPELINE_ALERTS_TOPIC_ARN` read back from inside `transit-dagster-daemon-1` |
| The sensor is registered and on | dagster `jobs` table: `SENSOR \| DECLARED_IN_CODE \| pipeline_failure_alert`, taking `default_status=RUNNING` from code |

The permission was checked with a policy *simulation* rather than a live publish, so
verifying the alerting did not itself send an alert.

**The confirmation step deserves remembering.** It sat in spam. Every component was
correct and the path still ended in silence — which is indistinguishable from having no
alerting at all, and is precisely the failure this gate exists to close. When adding a
subscriber, confirm delivery reached a human before calling it done.

---

## Handoff: the two steps still outstanding

Both need a human. Neither can be done by an agent — `terraform apply` is blocked by the
permission classifier, and the SNS confirmation is a link in someone's inbox.

### 1. Deploy the drain Lambda fix

The `-target` apply on 2026-08-25 landed SNS, the lifecycle rule and the IAM scope but
**skipped `module.spark.aws_lambda_function.drain`**. Production is still running the old
`ACTIVE_STATES` (missing the non-terminal `QUEUED` and `CANCELLING`) and the old
non-idempotent `clientToken`. That is the guard against two drains writing one Spark
checkpoint — the overlap that destroyed 58 minutes of drained data on 2026-08-24.

Do NOT write the plan inside the repo: a saved plan embeds root-module variable values in
cleartext, which is how four live API tokens reached git on 2026-08-25.

```sh
cd /Users/gil/Stuff/Transit
set -a; . ./.env; set +a
export TF_VAR_wmata_api_key="$WMATA_API_KEY" TF_VAR_bay511_api_token="$BAY511_API_TOKEN" \
       TF_VAR_swiss_otd_token="$SWISS_OTD_TOKEN" TF_VAR_swiss_otd_sa_token="$SWISS_OTD_SA_TOKEN" \
       TF_VAR_cta_api_key="$CTA_API_KEY"
PD=$(mktemp -d)
terraform -chdir=infra/envs/dev plan -target=module.spark.aws_lambda_function.drain -out="$PD/plan"
terraform -chdir=infra/envs/dev apply "$PD/plan"
```

Prefer `make infra-plan` / `make infra-apply` (scripts/tf.sh), which handles the provider
auth and writes the plan outside the repo. **Finish with an untargeted `terraform plan` and
confirm it reports no changes** — that is the only way to see what targeting skipped. It is how this
Lambda gap was found in the first place.

Verify (the hash should stop being `Jvt6sbDQGIzzPr2AK2KednIB0o1gfmm43joFUm1VLqA=`):

```sh
aws lambda get-function-configuration --region us-east-2 \
  --function-name transit-pulse-emr-drain --query '[LastModified,CodeSha256]' --output text
```

No reboot needed — Lambda picks up new code immediately and this does not touch the
services box.

### 2. Confirm the alerting email

`lichorish@gmail.com` has an "AWS Notification - Subscription Confirmation" message. Until
the link is clicked, SNS accepts every publish and delivers none of it.

```sh
aws sns list-subscriptions-by-topic --region us-east-2 \
  --topic-arn arn:aws:sns:us-east-2:622221238588:transit-pulse-pipeline-alerts
```

`PendingConfirmation` means the alerting is decorative. A real subscription ARN means D1
finally passes. Everything else on that path is built and verified: topic created, IAM
scoped to `sns:Publish` on that one ARN, `PIPELINE_ALERTS_TOPIC_ARN` confirmed live inside
the dagster container, sensor registered with `default_status=RUNNING`.
