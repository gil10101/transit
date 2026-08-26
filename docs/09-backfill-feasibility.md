# 09 — Can we backfill to the start of the year?

Asked 2026-08-26: get data back to 2026-01-01, gold only, no raw retention — how much
data and what does it cost?

**Short answer: the cost is trivial and irrelevant, because the data does not exist to
fetch.** For the seven cities we run, January–August 2026 realtime is gone. Nobody
captured it in a form we can retrieve.

---

## Why it cannot be backfilled

GTFS-Realtime is a **live snapshot, not a log**. Each endpoint returns "here is the state
of the network right now". It is overwritten every few seconds and nothing retains the
previous state. There is no `?since=` parameter, no pagination into the past, no archive
endpoint. When we poll at 30s we are *creating* the historical record — we are not
reading one.

This project's own source research reached the same conclusion before any code was
written (`docs/transit-pulse-plan.md` §on history):

> no one publishes free granular multi-year real-time archives (Sydney's TfNSW is the
> rare exception and is on the stretch list partly for that reason). The plan: your own
> S3 bronze layer becomes the granular archive from day one … After 3 months of running,
> you own a dataset that doesn't exist publicly — that's a feature, not a gap.

So the 238 days from Jan 1 to Aug 26 are not recoverable for NYC, Boston, Toronto,
Helsinki, DC, SF or Zurich. Not for money, not with more engineering.

**A caveat worth stating honestly:** "no free public archive" is the project's finding as
of the planning phase, and it is a claim about the *general* case. Individual agencies do
sometimes publish historical performance data in their own formats — aggregated
punctuality reports, or occasionally raw archives. Those would need verifying per agency
against their own documentation before being relied on, and none is currently in
`docs/01-data-dictionary.md`. If history matters enough, that verification is the work —
not a backfill job.

---

## What CAN be backfilled

| Source | Depth available | Status | Value |
|---|---|---|---|
| **Weather** (Open-Meteo archive) | decades | built, never run — `make p5-backfill-weather` | free, and the only true multi-year series we can get today |
| **Sydney TfNSW** | multi-year GTFS-RT archives | not built; city #10 on the P8 stretch list | the one documented route to instant history — but it is a *new city*, not history for existing ones |
| **Tokyo MLIT** | multi-year official delay stats | P4, not started | an aggregate benchmark to validate our numbers against, not granular events |
| **Static schedules** | some historical archives exist | n/a | schedules are what we compare *against*; alone they contain no reliability signal |

---

## What it would cost if the data existed

Worth computing anyway, because it shows cost is not the constraint. All figures measured
on 2026-08-26, not estimated.

**Measured production rate.** 2026-08-24, the one complete day across six cities:
**3,441,104 stop events**. At seven cities, call it **3.5M events/day**.

**Measured storage per row** (Snowflake `information_schema`, compressed):

| Table | Rows | Size | Bytes/row |
|---|---:|---:|---:|
| `fct_stop_events` | 7,205,224 | 634.1 MB | 88 |
| `int_stop_events_finalized` | 7,205,224 | 469.9 MB | 65 |
| `fct_headways` | 3,707,015 | 317.0 MB | 85 |
| all of GOLD | 35,382,777 | 1.52 GB | — |

Across every GOLD table, ~**211 bytes per stop event** end to end.

**Projection for 238 days at 3.5M events/day = 833M events:**

| Item | Size |
|---|---:|
| `fct_stop_events` | ~73 GB |
| `int_stop_events_finalized` | ~54 GB |
| `fct_headways` | ~36 GB |
| schedule, matching, rollups | ~8 GB |
| **GOLD total** | **~171 GB** |

Snowflake storage at on-demand list (~$23/TB/month) → **≈ $4/month**. That is the whole
storage cost of eight months of seven-city history. Compute to build it would be a few
warehouse-hours, single-digit dollars.

**Cost is not the blocker. Availability is.**

---

## The forward version, which is the real answer

Since history cannot be bought, it has to be accrued. The good news is that it is cheap.

**Measured steady-state cost, 2026-08-25** — the first clean day after the checkpoint,
cadence and drain fixes landed:

| Day | Gross AWS |
|---|---:|
| 2026-08-23 | $13.99 |
| 2026-08-24 | $12.59 |
| **2026-08-25** | **$1.98** |

The first two days were heavy development — fan-out, debugging, repeated full refreshes.
**$1.98/day ≈ $60/month** is the real running rate, inside the <$100 target.

At that rate, GOLD grows ~0.74 GB/day. So:

| Elapsed | Events | GOLD | AWS spend |
|---|---:|---:|---:|
| 1 month | ~105M | ~22 GB | ~$60 |
| 3 months | ~315M | ~66 GB | ~$180 |
| 8 months | ~840M | ~172 GB | ~$480 |

Three months is the point the plan names as when the dataset becomes something that does
not exist publicly.

---

## On dropping the raw archive

The question assumed "transfer end to end to gold, don't hold the raw". Raw currently
accrues ~6.75 GB/day against GOLD's ~0.74 GB/day, so dropping it saves real money
(~$37/month in S3 Standard after eight months).

**[rev 2026-08-26] An earlier version of this section argued to keep raw as insurance
against gold being wrong. That reasoning was backwards and is withdrawn.**

The goal is a pipeline engineered so gold is never wrong — not one with a good recovery
story for when it is. Every defect found while hardening this pipeline was our own code
(a wrong cutover default, a missing filter call, `concat_ws` returning `''` instead of
NULL), and **not one of them would have been prevented by keeping bytes**. They were
preventable by contract tests, which is where that effort belongs. Using replay as the
answer tolerates a pipeline that emits bad numbers and repairs them afterwards.

What raw is genuinely for is narrower and worth stating precisely:

* **Upstream reality changing under us** — an agency renames a field, changes an id
  namespace, or alters what a value means. That is not a bug we can test our way out of,
  because the contract we would be asserting is the one that just changed. Toronto's
  wrong-schedule-file episode was this shape.
* **Answering questions the current models do not ask.** Raw holds every field the feed
  carried, not just the ones the reliability question needed.

Neither justifies it as a correctness crutch. And note what we actually chose for the SF
phantom day: we **dropped** the misdated rows rather than replaying, because those days
sat before `sf.metrics_from` and were never scored. Replay was available and was still
the wrong tool.

**Recommendation:** keep raw, but for the reasons above rather than as a safety net, and
let the existing lifecycle do the cost work — it already transitions to STANDARD_IA at 30
days (`infra/modules/lake/main.tf`), which covers most of the volume. If the archive is
ever dropped, drop it as a deliberate scope decision about what questions the project
wants to be able to answer later, not as a bet that the pipeline is now correct enough
not to need it.

**[rev 2026-08-26, DECIDED — Jake]: raw expires after 30 days.** The scope decision
above got made: the mission is a month-plus of *gold* per city at far lower cost, and
raw had become the only compounding line (59 GB at ~14 GB/day, vs gold's 2.4 GB
holding full granularity). Thirty days preserves the two genuine uses in their useful
window — debugging an upstream change the tripwires surface, and reparsing recent
bytes for a new question — while capping the archive at roughly a month's footprint
(~$10/mo steady state). The un-reparseable long tail is the price, accepted
deliberately. Enacted in `infra/modules/lake/main.tf` (expire-raw-30d).
