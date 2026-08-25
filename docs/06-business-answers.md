# 06 — What the warehouse currently answers

Every number on this page came out of `analysis/business_questions.sql` run
against `TRANSIT.GOLD` on **2026-08-25**, over the data collected since
2026-08-22. Re-run that file rather than editing numbers here by hand.

**Read the caveats before quoting anything.** This is 2–3 days of data, not a
season. Cities onboarded on different days (`dim_city.metrics_from`), so the
denominators are not equal, and a "most reliable city" ranking off this window
would be dishonest. What the numbers *do* establish is that the pipeline
produces plausible, internally consistent, cross-city-comparable measurements —
which was the point of P1–P5.

---

## The headline question

> Which cities run the most reliable public transit — and what makes them
> reliable?

Not answerable yet as a single score: `fct_city_scorecard_monthly` is P6 and
does not exist. What exists is every input to it. Below, each of the eight
sub-questions from `docs/transit-pulse-plan.md` §1, with its current answer.

---

## Q1 — On-time performance

Share of scored stop arrivals per band. `on_time` is −60s to +299s; `early` is
more than 60s ahead; `very_late` is 15 min or worse.

| City | Scored events | On time | Early | Late | Very late |
|---|---:|---:|---:|---:|---:|
| Helsinki | 675,492 | **81.3%** | 13.3% | 5.2% | 0.2% |
| New York | 341,117 | 69.9% | 19.3% | 9.5% | 1.3% |
| Boston | 441,056 | 59.0% | 17.5% | 20.4% | 3.0% |
| SF Bay Area | 414,651 | 58.9% | 23.3% | 14.8% | 3.0% |
| Washington DC | 259,051 | 53.8% | 24.0% | 18.2% | 4.0% |
| Toronto | 1,210,201 | 49.7% | **41.0%** | 7.5% | 1.9% |

**Toronto's 41% early is the finding here, not its 49.7% on-time.** Toronto is
not late — it is *ahead of schedule* four times out of ten. For a bus network
that is a genuine service defect (a bus that leaves a timepoint early strands
riders who arrived on time), which is exactly why the early band is tracked
separately instead of being folded into "not late". It is also worth one more
look before it goes in a scorecard: 41% is high enough to suspect the TTC
static's timepoints as well as TTC's driving.

By local hour, the 8am and 5pm peaks (routes with ≥500 events in the hour):

| City | 8am OTP | 5pm OTP |
|---|---:|---:|
| Helsinki | 79.0% | 76.2% |
| New York | 64.2% | 69.8% |
| Boston | 61.5% | 54.4% |
| SF Bay Area | — | 53.6% |
| Washington DC | — | 48.5% |
| Toronto | 46.9% | 48.3% |

SF and DC have no 8am row yet — they onboarded on 2026-08-24/25 and have not
accumulated a full morning peak past their `metrics_from`.

## Q2 — Average delay

Signed seconds, positive = late.

| City | Mean | Median | p90 | p99 |
|---|---:|---:|---:|---:|
| Toronto | 11 | **−23** | 283 | 1,237 |
| New York | **−98** | 14 | 303 | 995 |
| Helsinki | 53 | 31 | 216 | 554 |
| SF Bay Area | 149 | 52 | 447 | 2,373 |
| Washington DC | 156 | 81 | 543 | 1,762 |
| Boston | 164 | 112 | 516 | 1,431 |

**Use the median, not the mean.** New York's mean of −98s against a median of
+14s is not a fast subway; it is a long negative tail. NYC delay is computed
against the static schedule (the feed does not set `arrival.delay` except on
the L), so a trip matched to a schedule that ran at a different time produces a
large negative number. The median is robust to that; the mean is not. Boston's
mean and median agree, which is what a well-behaved city looks like.

## Q3 — Service volume

Peak hour per mode. `vehicle_id_reliable = false` means the feed does not
identify vehicles, so `peak_vehicles` there is a floor.

| City | Mode | Peak vehicles | Peak trips | Peak routes |
|---|---|---:|---:|---:|
| SF Bay Area | bus | 1,664 | 2,965 | 450 |
| Toronto | bus | 1,371 | 3,099 | 183 |
| Washington DC | bus | 1,027 | 1,790 | 128 |
| New York | metro | 981 *(id unreliable)* | 981 | 26 |
| Boston | bus | 698 | 1,435 | 148 |
| Toronto | tram | 204 | 406 | 16 |
| SF Bay Area | tram | 126 | 206 | 10 |
| Washington DC | metro | 120 | 225 | 6 |
| Boston | tram | 72 | 177 | 5 |
| Boston | rail | 60 | 80 | 13 |
| Boston | metro | 42 | 137 | 3 |
| SF Bay Area | rail | 43 | 46 | 7 |
| SF Bay Area | ferry | 11 | 21 | 6 |
| Toronto | ferry | 2 | 2 | 1 |

**Helsinki is absent from this table entirely.** HSL publishes no
VehiclePositions feed — trip updates only — so there is nothing to count
vehicles from. Helsinki's OTP and headway numbers are unaffected (both are
derived from stop-time predictions), but any "how many vehicles are moving"
question is structurally unanswerable for Helsinki from GTFS-RT.

## Q4 — Frequency, headways, bunching

Bunched = actual gap under half the scheduled gap; big gap = over double.

| City | Gaps measured | Mean gap | Median gap | Bunched | Big gap |
|---|---:|---:|---:|---:|---:|
| Toronto | 1,137,057 | 815s | 687s | **11.7%** | 4.8% |
| Boston | 429,831 | 1,528s | 1,130s | 10.7% | 2.3% |
| Washington DC | 241,362 | 1,188s | 1,069s | 9.4% | 4.6% |
| SF Bay Area | 377,100 | 1,377s | 1,075s | 7.7% | 4.9% |
| New York | 348,346 | 642s | 529s | 7.7% | 3.5% |
| Helsinki | 634,641 | 1,476s | 1,012s | **2.6%** | 1.8% |

Helsinki bunches roughly a quarter as often as Toronto. This is the single
clearest cross-city separation in the data so far, and it is consistent with
Helsinki also topping OTP.

## Q5 — Excess Wait Time

Only defined for frequent service (scheduled headway ≤ 10 min), where riders
turn up without a timetable. EWT is how much longer they actually wait than the
schedule promises.

| City | Route-days | Mean EWT | Median EWT |
|---|---:|---:|---:|
| Helsinki | 102 | 94s | **22s** |
| New York | 103 | 74s | 29s |
| SF Bay Area | 65 | 105s | 55s |
| Boston | 73 | 51s | 58s |
| Washington DC | 44 | 137s | 76s |
| Toronto | 267 | 289s | **372s** |

Toronto's median EWT of 6 minutes on frequent routes is an order of magnitude
worse than Helsinki's 22 seconds, and it lines up with Toronto's bunching rate.
Treat the magnitude as provisional until Toronto has several closed days.

## Q6 — Cancellations and disruptions

Two separate aggregates on purpose — joining alerts to route-days fans out and
inflates the cancel rate.

| City | Mean cancel % | Scheduled trips |
|---|---:|---:|
| Boston | 2.264% | 52,397 |
| SF Bay Area | 1.298% | 50,784 |
| Helsinki | 0.100% | 60,324 |
| Washington DC | 0.011% | 33,563 |
| Toronto | 0.000% | 153,343 |
| New York | 0.000% | 39,150 |

| City | Alerts | Alert-hours | Routes alerted |
|---|---:|---:|---:|
| New York | 1,441 | 17,911 | 10 |
| Boston | 493 | 7,094 | 116 |
| SF Bay Area | 419 | 7,372 | 103 |
| Toronto | 262 | 1,768 | 99 |
| Helsinki | 112 | 921 | 42 |
| Washington DC | 96 | 461 | 63 |

A zero cancel rate for Toronto and NYC means *their feeds never emit
CANCELED*, not that nothing was cancelled. Cancellation rate is a measure of
feed behaviour as much as of service, and must not be scored across cities
without that caveat. NYC's alert profile is the opposite extreme: 1,441 alerts
across only 10 routes — the MTA raises long-lived, line-level alerts, while
MBTA and 511 raise many short route-level ones.

## Q7 — Weather sensitivity

**Not answerable today.** Weather has been landing hourly in
`TRANSIT.SILVER.WEATHER_HOURLY` (1,776 rows: city, local_date, local_hour,
temp_c, precip_mm, snowfall_cm, wind_kph, weather_code) since P5, but **no dbt
model reads it** — there is no `stg_weather__hourly`, so nothing joins it to
`fct_stop_events`. The query is written out and commented in
`analysis/business_questions.sql` so the gap is visible rather than silently
absent. The 2-year backfill (`make p5-backfill-weather`) has also not been run.

## Q8 — Data completeness

Only closed local days at or after each city's `metrics_from` are judged.

| City | Route-days judged | Mean completeness | Scheduled | Observed | Cancelled |
|---|---:|---:|---:|---:|---:|
| Helsinki | 437 | 95.9% | 23,752 | 22,777 | 43 |
| New York | 23 | 95.2% | 5,660 | 5,350 | 0 |

**Only two cities appear.** Boston, Toronto, DC and SF have no closed day at or
after their `metrics_from` yet, so P3's "completeness ≥85% after 48h"
acceptance criterion is still unverified for four of six cities. First real
reads land 2026-08-26.

---

## Where coverage is actually missing, by mode

`never_sched` isolates dead variant rows in the static — Boston publishes 368
bus routes of which 215 never run — so **`sched_not_obs` is the honest gap**:
service that is scheduled and that we never saw in realtime.

| City | Mode (`route_type`) | Static routes | Never scheduled | Scheduled, never seen | Observed |
|---|---|---:|---:|---:|---:|
| Boston | tram 0 (Green Line) | 5 | 0 | 0 | 5 |
| Boston | metro 1 | 3 | 0 | 0 | 3 |
| Boston | rail 2 (commuter) | 14 | 1 | 0 | 13 |
| Boston | bus 3 | 368 | 215 | 2 | 151 |
| Boston | **ferry 4** | 9 | 0 | **9** | **0** |
| DC | metro 1 | 6 | 0 | 0 | 6 |
| DC | bus 3 | 128 | 0 | 0 | 128 |
| Helsinki | tram 0 | 29 | 5 | 1 | 23 |
| Helsinki | metro 1 | 4 | 0 | 0 | 4 |
| Helsinki | **ferry 4** | 3 | 0 | **3** | **0** |
| Helsinki | suburban rail 109 | 13 | 0 | 0 | 13 |
| Helsinki | regional bus 701 | 354 | 12 | 15 | 327 |
| Helsinki | express bus 702 | 14 | 0 | 0 | 14 |
| Helsinki | local bus 704 | 46 | 0 | 2 | 44 |
| Helsinki | light rail 900 | 1 | 0 | 0 | 1 |
| NYC | metro 1 | 28 | 1 | 1 | 26 |
| NYC | rail 2 (SIR) | 1 | 0 | 0 | 1 |
| SF | tram 0 | 16 | 4 | 2 | 10 |
| SF | metro 1 | 12 | 0 | 2 | 10 |
| SF | rail 2 | 8 | 0 | 1 | 7 |
| SF | bus 3 | 614 | 10 | **109** | 495 |
| SF | ferry 4 | 14 | 0 | 8 | 6 |
| SF | cable car 5 | 3 | 0 | 0 | 3 |
| Toronto | tram 0 (streetcar) | 16 | 0 | 0 | 16 |
| Toronto | bus 3 | 204 | 6 | 0 | 198 |
| Toronto | ferry 4 | 1 | 0 | 0 | 1 |

**DC and Toronto are complete** — every scheduled route is observed.

**Ferries are the systematic hole.** Boston 9 routes, Helsinki 3, SF 8 of 14 —
all scheduled, none (or few) observed. Helsinki's is confirmed agency-side:
HSL's Suomenlinna ferry realtime is an AIS→MQTT stream
(`HSLdevcom/suomenlinna-ferry-hfp`), not part of their GTFS-RT at all. Boston's
and SF's have the same shape and have not been separately confirmed.

**SF's 109 unobserved bus routes** are not one failure but many small ones. The
511 feed aggregates ~30 operators under a single `city_key`, and they do not
all publish realtime:

| Operator prefix | Routes scheduled | Observed |
|---|---:|---:|
| AC (AC Transit) | 123 | 112 |
| SM (SamTrans) | 75 | 69 |
| SF (Muni) | 68 | 58 |
| SC (VTA) | 67 | 55 |
| CC (County Connection) | 51 | 42 |
| WH | 30 | 30 |
| SO | 26 | 26 |
| **CM** | 20 | **0** |
| **PE** | 11 | **0** |
| **UC** | 6 | **0** |
| **GF** | 5 | **0** |
| **MB** | 4 | **0** |
| **SS** | 3 | **0** |

The large operators run 84–100% coverage. Six small operators publish schedule
and no realtime whatsoever. Treating "SF Bay Area" as one city therefore
averages a well-instrumented core with a handful of blind operators — worth
stating explicitly wherever an SF number is quoted.

---

## What modes each source actually gives us

| Source | Feed | Modes in the schedule | Modes we actually observe | Notes |
|---|---|---|---|---|
| **MTA** (NYC) | 8 GTFS-RT line-group feeds, keyless | Subway (28 routes), Staten Island Railway | Subway 26/27, SIR | Buses deliberately out of scope. No vehicle ids and no lat/lon on subway VP. `arrival.delay` absent except on the L feed. |
| **MBTA** (Boston) | GTFS-RT TU/VP/Alerts, keyless | Green Line (tram), subway, commuter rail, bus, **ferry** | All but ferry | Cleanest feed of the set. 215 of 368 static bus routes are non-running variants. |
| **TTC** (Toronto) | `bustime.ttc.ca` GTFS-RT | Streetcar, bus, 1 ferry route | All of them, 100% | **Surface only — the subway publishes alerts, never positions or predictions.** Toronto's score is a surface-mode score, by design. Static must be `SurfaceGTFS.zip`. |
| **HSL** (Helsinki) | GTFS-RT TU + alerts, keyless | Tram, metro, suburban rail, regional/express/local bus, light rail, **ferry** | All but ferry | **No VehiclePositions feed at all.** Ferry realtime is AIS→MQTT, outside GTFS-RT. `trip_id` is empty; trips resolve on (route, direction, date, start time). |
| **WMATA** (DC) | GTFS-RT rail + bus, keyed | Metrorail, Metrobus | Both, 100% | Two static zips (rail + bus) landed under one `gtfs_version_id`. |
| **511.org** (SF Bay) | Regional aggregated GTFS-RT, `agency=RG`, keyed | ~30 operators: bus, tram, metro, regional rail, ferry, **cable car** | Most, with real per-operator gaps | 60 req/hr cap → 200s cadence. Cable car (`route_type` 5) is unique to this feed. |
| **opentransportdata.swiss** (Zurich) | National `/la/gtfs-rt`, keyed | Whole Swiss network — filtered to 615 Zurich routes: S-Bahn, tram, bus, boat, aerial lift, funicular | **Nothing yet** | Poller staged, not yet live. National rail classes (TGV/ICE/IC/IR/RE/EXT) are excluded from the allow-list on purpose; S-Bahn (`route_type` 109) is kept. |
| **CTA** (Chicago) | GTFS-RT, keyed | 'L' rail + bus | **Nothing** | Key issued, beta activation still returns `errCd 101`. |
| **ODPT** (Tokyo) | `odpt:Train` JSON | Rail | **Nothing** | Not yet applied for. Its GTFS-RT is alerts-only, so the JSON API is the path. |
| **Open-Meteo** | Hourly forecast JSON | n/a | Collected, unused | 1,776 rows in silver, no dbt model reads them. |

Two feeds carry a mode nobody else does: 511's **cable car** and Zurich's
**funicular/aerial lift**. Two modes are systematically missing everywhere they
exist: **ferries** (agencies publish them outside GTFS-RT) and **Toronto's
subway** (TTC publishes no realtime for it).
