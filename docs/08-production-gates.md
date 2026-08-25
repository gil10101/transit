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
| A1 | Every canonical rule (CLAUDE.md) has exactly one implementation | 1 live implementation each; no dead duplicates | **FAIL** |
| A2 | dbt tests clean on data that is fair to judge | 0 errors on closed local days at/after `metrics_from` | **FAIL** |
| A3 | CI gates green | `ruff check` + `ruff format --check` + `pytest` all pass | **PASS** |
| A4 | Every city verified field-by-field at every hop | raw → silver → gold sampled per city | **PASS** |
| A5 | No test that cannot fail | every test has a case that would trip it | **UNMEASURED** |
| A6 | dbt runs in CI | modified models built against duckdb on PR | **FAIL** |

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
| B4 | No future-dated service days in judged marts | 0 rows | **FAIL** |
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
| C2 | No permanently-red check | 0 chronically failing jobs | **FAIL** |
| C3 | Transient failures self-heal | no manual step for a single-run failure | **PASS** |
| C4 | Single points of failure documented with a tested recovery path | every SPOF has a runbook | **FAIL** |

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
| D1 | A human is notified when the pipeline actually breaks | alert within 60 min of a real failure | **FAIL** |
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
| E2 | Every growing store has a retention policy | 0 unbounded stores | **FAIL** |
| E3 | No known systematic waste | 0 identified-and-unfixed | **PASS** |

**E1** — gross usage $22.38 for 1–25 August, fully covered by credits, **$0.00 billed**.

**E2** — two unbounded stores: the raw archive (~27 GB and growing for every city) and
EMR debug logs (3.4 GB in three days, read by nobody after the week they are produced).
Raw retention is a genuine tradeoff — it is the replay layer — and needs a decision.
EMR logs are close to free money.

**E3** — Zurich's duplicate-alerts waste (29 GB/day of a byte-identical file) was found
and fixed on 2026-08-25.

---

## F — Security

| # | Gate | Threshold | Status |
|---|---|---|---|
| F1 | No secret in Terraform state, logs, or the repo | 0 occurrences | **PASS** |
| F2 | Secrets stored encrypted with least-privilege access | all SecureString | **PASS** |
| F3 | No secret printed by any code path | 0 | **PASS** |

All five deployed secrets are SSM `SecureString`, and the write-only (`value_wo`) pattern
means **no plaintext value appears in Terraform state** — verified by parsing the state.
City YAML files carry only environment-variable *names*, never values.

---

## G — Structure and maintainability

| # | Gate | Threshold | Status |
|---|---|---|---|
| G1 | No dead code or dead config | 0 unreferenced symbols | **FAIL** |
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
