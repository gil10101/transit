# 06 — What the warehouse currently answers

Every number on this page came out of `analysis/business_questions.sql` run
against `TRANSIT.GOLD` on **2026-08-26 04:40Z**, over the data collected since
2026-08-22. Re-run that file rather than editing numbers here by hand.

**Read the caveats before quoting anything.** This is 4–5 days of data, not a
season. Cities onboarded on different days (`dim_city.metrics_from`), so the
denominators are not equal, and a "most reliable city" ranking off this window
would be dishonest — which is precisely why `fct_city_scorecard_monthly`
refuses to emit one until a city has 20 closed judged days. What the numbers
*do* establish is that the pipeline produces plausible, internally consistent,
cross-city-comparable measurements. This run is also the first with the 7-line
regex casualty repaired (NYC gained ~350 northbound trips/day) and with weather
joined into gold (Q7 is live).

---

## The headline question

> Which cities run the most reliable public transit — and what makes them
> reliable?

Not answerable yet as a single score — and now that is a *design guarantee*
rather than a gap: `fct_city_scorecard_monthly` exists, is built by every
chain run, and deliberately holds ZERO rows until a city accrues 20 closed
judged days in a month. Every input to it exists and is measured below, each
of the eight sub-questions from `docs/transit-pulse-plan.md` §1 with its
current answer.

---

## Q1 — On-time performance

Share of scored stop arrivals per band. `on_time` is −60s to +299s; `early` is
more than 60s ahead; `very_late` is 15 min or worse.

| City | Scored events | On time | Early | Late | Very late |
|---|---:|---:|---:|---:|---:|
| Zurich | 3,377 | **83.9%** | 4.7% | 7.4% | 4.0% |
| Helsinki | 1,378,897 | **81.0%** | 12.7% | 6.0% | 0.3% |
| New York | 604,145 | 67.5% | 22.4% | 9.0% | 1.2% |
| SF Bay Area | 1,394,139 | 58.5% | 21.9% | 16.1% | 3.5% |
| Boston | 874,422 | 57.9% | 17.3% | 21.8% | 3.1% |
| Washington DC | 884,318 | 53.9% | 23.0% | 18.9% | 4.3% |
| Toronto | 2,365,483 | 49.2% | **41.1%** | 7.9% | 1.8% |

Zurich's 83.9% leads the table on 3,377 events — treat it as a first reading,
not a result; its first judged day only just closed.

**Toronto's 41% early is the finding here, not its 49.2% on-time.** Toronto is
not late — it is *ahead of schedule* four times out of ten. For a bus network
that is a genuine service defect (a bus that leaves a timepoint early strands
riders who arrived on time), which is exactly why the early band is tracked
separately instead of being folded into "not late". It is also worth one more
look before it goes in a scorecard: 41% is high enough to suspect the TTC
static's timepoints as well as TTC's driving.

By local hour, the 8am and 5pm peaks (routes with ≥500 events in the hour):

| City | 8am OTP | 5pm OTP |
|---|---:|---:|
| Helsinki | 78.7% | 74.5% |
| New York | 62.6% | 67.4% |
| Boston | 59.9% | 53.0% |
| SF Bay Area | 58.1% | 53.7% |
| Washington DC | 49.5% | 48.5% |
| Toronto | 47.1% | 49.0% |

Every US/CA city now has both peaks; Zurich has a single ≥500-event hour so
far (11:00, 73.0%). The near-universal pattern: the evening peak is worse than
the morning one everywhere except New York.

## Q2 — Average delay

Signed seconds, positive = late.

| City | Mean | Median | p90 | p99 |
|---|---:|---:|---:|---:|
| Toronto | −293 | **−24** | 293 | 1,214 |
| Zurich | 47 | 0 | 216 | 1,872 |
| New York | −320 | 5 | 288 | 938 |
| Helsinki | 62 | 34 | 232 | 652 |
| SF Bay Area | **−19,027** | 63 | 487 | 2,340 |
| Washington DC | 166 | 88 | 559 | 1,752 |
| Boston | 174 | 121 | 534 | 1,405 |

**Use the median, not the mean — SF just proved it in the extreme.** SF's mean
of −19,027s against a median of +63s is a handful of predictions filed absurdly
far ahead of a schedule, not a time-travelling bus; the delay-bounds test warns
on exactly these rows and stores them for audit. Same story milder in New York
and Toronto (schedule-computed delay with mismatched trips makes long negative
tails). The median is robust to all of it; the mean is not. Boston's mean and
median agree, which is what a well-behaved city looks like.

## Q3 — Service volume

Peak hour per mode. `vehicle_id_reliable = false` means the feed does not
identify vehicles, so `peak_vehicles` there is a floor.

| City | Mode | Peak vehicles | Peak trips | Peak routes |
|---|---|---:|---:|---:|
| SF Bay Area | bus | 1,664 | 2,965 | 460 |
| Toronto | bus | 1,400 | 3,147 | 184 |
| Washington DC | bus | 1,031 | 1,821 | 128 |
| New York | metro | 1,004 *(id unreliable)* | 1,004 | 26 |
| Boston | bus | 710 | 1,472 | 148 |
| Toronto | tram | 204 | 406 | 16 |
| SF Bay Area | tram | 128 | 206 | 10 |
| Washington DC | metro | 127 | 226 | 6 |
| Boston | tram | 79 | 187 | 5 |
| Boston | rail | 60 | 80 | 13 |
| Boston | metro | 42 | 139 | 3 |
| SF Bay Area | rail | 43 | 46 | 7 |
| SF Bay Area | ferry | 12 | 25 | 7 |
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
| Toronto | 2,296,324 | 903s | 690s | **11.9%** | 5.0% |
| New York | 635,065 | 598s | 475s | 11.0% | 3.1% |
| Boston | 885,090 | 1,528s | 1,146s | 10.8% | 2.4% |
| Washington DC | 858,085 | 1,284s | 1,162s | 8.8% | 4.9% |
| SF Bay Area | 1,339,430 | 1,439s | 1,125s | 7.5% | 5.1% |
| Helsinki | 1,288,827 | 1,466s | 1,007s | **2.8%** | 1.8% |

New York's bunching jumped from 7.7% to 11.0% with the 7-line repair — the
recovered northbound trips were exactly the dense-headway service where
bunching lives, a reminder that a silent data loss biases metrics, not just
volumes. Zurich has one rated gap so far; too young to tabulate.

Helsinki bunches roughly a quarter as often as Toronto. This is the single
clearest cross-city separation in the data so far, and it is consistent with
Helsinki also topping OTP.

## Q5 — Excess Wait Time

Only defined for frequent service (scheduled headway ≤ 10 min), where riders
turn up without a timetable. EWT is how much longer they actually wait than the
schedule promises.

| City | Route-days | Mean EWT | Median EWT |
|---|---:|---:|---:|
| Helsinki | 203 | 83s | **20s** |
| New York | 157 | 60s | 25s |
| Toronto | 427 | 23s | 56s |
| Boston | 129 | 58s | 62s |
| SF Bay Area | 145 | 204s | 74s |
| Washington DC | 96 | 170s | 85s |

Toronto's median EWT collapsed from the provisional 372s to 56s once the
grain-and-evidence rewrite weighted route-days properly and more days closed —
the earlier magnitude was exactly the partial-day artifact the docs warned
about. The Helsinki-vs-everyone gap (20s median) survives and still lines up
with its bunching rate. Zurich: one frequent route-day so far (900s — noise).

## Q6 — Cancellations and disruptions

Two separate aggregates on purpose — joining alerts to route-days fans out and
inflates the cancel rate.

| City | Mean cancel % | Scheduled trips |
|---|---:|---:|
| Boston | 2.615% | 84,466 |
| Zurich | 2.263% | 25,905 |
| SF Bay Area | 1.324% | 101,690 |
| Helsinki | 0.092% | 149,479 |
| Washington DC | 0.049% | 67,335 |
| Toronto | 0.000% | 246,463 |
| New York | 0.000% | 58,394 |

| City | Alerts | Alert-hours | Routes alerted |
|---|---:|---:|---:|
| New York | 4,440 | 80,307 | 9 |
| Zurich | 1,060 | 23,771 | 605 |
| Boston | 828 | 11,842 | 135 |
| SF Bay Area | 734 | 13,672 | 112 |
| Toronto | 530 | 4,126 | 128 |
| Washington DC | 183 | 820 | 76 |
| Helsinki | 182 | 1,581 | 51 |
| Washington DC | 96 | 461 | 63 |

A zero cancel rate for Toronto and NYC means *their feeds never emit
CANCELED*, not that nothing was cancelled. Cancellation rate is a measure of
feed behaviour as much as of service, and must not be scored across cities
without that caveat. NYC's alert profile is the opposite extreme: 1,441 alerts
across only 10 routes — the MTA raises long-lived, line-level alerts, while
MBTA and 511 raise many short route-level ones.

## Q7 — Weather sensitivity

**LIVE as of 2026-08-26.** `dim_weather` (WMO code → condition bucket) +
`stg_weather__hourly` + `fct_weather_hourly` join `fct_stop_events` on exactly
(city_key, local_date, local_hour); the 2-year backfill ran the same night
(124,738 hourly rows, 2024-08-19 → today, all seven cities), so the moment a
snowy day happens, the comparison exists.

First reading — OTP by condition (cells under 200 scored events suppressed):

| City | Condition | Scored | OTP | vs its dry OTP |
|---|---|---:|---:|---:|
| New York | rain | 6,584 | **75.5%** | +8.0pp |
| New York | heavy_rain | 4,399 | 74.0% | +6.5pp |
| Zurich | rain | 1,258 | **91.5%** | +9.3pp |
| Helsinki | cloudy | 791,457 | 81.2% | +0.5pp |
| SF Bay Area | fog | 526,309 | 57.5% | +0.3pp |
| Boston | cloudy | 81,596 | 55.6% | −2.5pp |

The counterintuitive early signal — NYC and Zurich run *better* in rain — is
exactly why `n_scored` rides on every cell: a few thousand events from one
rainy evening is an anecdote with a denominator, not a finding. The question
this table is really waiting for (Helsinki-in-snow vs Boston-in-snow) needs
winter; the machinery for it is now fully wired and tested.

## Q8 — Data completeness

Only closed local days at or after each city's `metrics_from` are judged.

| City | Route-days judged | Mean completeness | Scheduled | Observed | Cancelled |
|---|---:|---:|---:|---:|---:|
| Toronto | 216 | 102.0% | 35,759 | 36,194 | 0 |
| New York | 52 | 99.0% | 14,351 | 14,657 | 0 |
| Helsinki | 874 | 96.0% | 47,544 | 45,707 | 81 |
| Boston | 181 | 93.6% | 17,370 | 16,708 | 824 |

**P3's "completeness ≥85%" acceptance criterion: PASSED by all four cities
with closed judged days.** Toronto over 100% is ADDED service running beyond
the schedule, counted honestly. DC, SF and Zurich still have no closed day at
or after `metrics_from`; their first reads land over the next two days.

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
| SF | bus 3 | 614 | 10 | **75** | 529 |
| SF | ferry 4 | 14 | 0 | 7 | 7 |
| SF | cable car 5 | 3 | 0 | 0 | 3 |
| Toronto | tram 0 (streetcar) | 16 | 0 | 0 | 16 |
| Toronto | bus 3 | 204 | 6 | 0 | 198 |
| Toronto | ferry 4 | 1 | 0 | 0 | 1 |
| Zurich | S-Bahn 109 | 63 | 24 | 32 | 7 |
| Zurich | bus 700 | 440 | 67 | 294 | 79 |
| Zurich | tram 900 | 22 | 1 | 10 | 11 |
| Zurich | boat 1000 | 10 | 1 | 9 | 0 |

**DC and Toronto are complete** — every scheduled route is observed. Zurich's
large "scheduled, never seen" counts are one partial day of data, not a
verdict — its coverage read matures with its first few closed days.

**Ferries are the systematic hole.** Boston 9 routes, Helsinki 3, Zurich's
boats 9 of 10, SF 7 of 14 — scheduled, none (or few) observed. Helsinki's is
confirmed agency-side (AIS→MQTT, `HSLdevcom/suomenlinna-ferry-hfp`); Boston's
was proven upstream on 2026-08-26 — **zero silver rows ever** for any Boat-%
route — and is now a seeded known coverage gap, alongside its Orange-Line
rail-replacement shuttles (same proof, 0 of 2,791 scheduled shuttle trips ever
published in the feed).

**SF's 75 unobserved bus routes** (down from 109 as more days accrued) are not
one failure but many small ones. The
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
| **opentransportdata.swiss** (Zurich) | National `/la/gtfs-rt`, keyed | Whole Swiss network — filtered to 615 Zurich routes: S-Bahn, tram, bus, boat, aerial lift, funicular | Bus, tram, S-Bahn observed (79/11/7 routes on day one); boats not yet | Live since 2026-08-25. National rail classes (TGV/ICE/IC/IR/RE/EXT) are excluded from the allow-list on purpose; S-Bahn (`route_type` 109) is kept. No VehiclePositions product on the Swiss LA API — trip updates only, like Helsinki. |
| **CTA** (Chicago) | GTFS-RT, keyed | 'L' rail + bus | **Nothing** | Key issued, beta activation still returns `errCd 101`. |
| **ODPT** (Tokyo) | `odpt:Train` JSON | Rail | **Nothing** | Not yet applied for. Its GTFS-RT is alerts-only, so the JSON API is the path. |
| **Open-Meteo** | Hourly forecast JSON | n/a | Collected, unused | 1,776 rows in silver, no dbt model reads them. |

Two feeds carry a mode nobody else does: 511's **cable car** and Zurich's
**funicular/aerial lift**. Two modes are systematically missing everywhere they
exist: **ferries** (agencies publish them outside GTFS-RT) and **Toronto's
subway** (TTC publishes no realtime for it).
