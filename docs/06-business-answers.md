# 06 — What the warehouse currently answers

Every number on this page came out of `analysis/business_questions.sql` run
against `TRANSIT.GOLD` on **2026-09-01 13:45Z**, over the data collected since
2026-08-22. Re-run that file rather than editing numbers here by hand.

**This run is the first whose MEANS are trustworthy.** Two defects were fixed
on 2026-08-31 and the whole history rebuilt behind them (docs/08):
* ~520,000 stop events had been silently deleted — the origin stop of nearly
  every trip, where GTFS publishes a departure and no arrival, discarded by a
  NULL-swallowing filter. They are back, so event counts here are ~5% higher
  than any earlier run and early-departure figures moved.
* Trip ids re-keyed by a static refresh are recovered (2026-09-01): agencies
  re-number trips when they publish a new timetable, which stranded 98,456 trips
  across all cities — a third of Zurich's 08-31 alone — because the id no longer
  matched anything the calendar ran. Matched now by route + origin departure.
* Delays are now bounded: feeds publish corrupt `arrival.delay` values (511
  stated −16,245,480s on trips whose schedule and actual were two minutes
  apart), and a delay that cannot be determined plausibly is now NULL rather
  than a fabricated number. SF's mean arrival delay read −35,559s in the
  2026-08-26 run and reads +194s here. Earlier editions of this page told you
  to distrust the mean; you no longer have to.

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
| Tokyo* | 285,976 | **97.1%** | 0.0% | 2.1% | 0.8% |
| Zurich | 7,817,015 | **96.2%** | 1.1% | 2.4% | 0.2% |
| Helsinki | 10,055,404 | **81.0%** | 12.0% | 6.6% | 0.4% |
| New York | 3,381,705 | 67.8% | 21.4% | 9.3% | 1.5% |
| SF Bay Area | 13,017,111 | 59.7% | 20.6% | 15.8% | 3.8% |
| Boston | 6,332,661 | 57.4% | 14.6% | 23.3% | 4.7% |
| Washington DC | 8,539,020 | 54.7% | 21.7% | 19.2% | 4.4% |
| Toronto | 14,122,636 | 50.0% | **37.0%** | 10.0% | 2.9% |

*Tokyo's number is operator-stated and minute-rounded (see the provenance
section at the bottom of this page): sub-minute lateness reads as on-time by
construction, and its 0.0% early is an artifact of the same quantization —
Toei never states a negative delay. First place with an asterisk that must
travel with it everywhere.

Zurich's 96.1% on 547k events is the real number — and a lesson. Until
2026-08-27 its whole network was silently reduced to ~3k events because the Swiss
feed publishes delay-only predictions (no timestamps) and finalization demanded
timestamps; the completeness gate caught it on Zurich's FIRST judged day and the
delay_plus_schedule finalization method fixed it (docs/08). Swiss punctuality
lives up to its reputation: first place, by fifteen points, and its band split
is now measured rather than pending — only 1.1% early and 0.2% very late, the
tightest distribution of any city here.

**Toronto's 38% early is the finding here, not its 50.4% on-time.** Toronto is
not late — it is *ahead of schedule* four times out of ten. For a bus network
that is a genuine service defect (a bus that leaves a timepoint early strands
riders who arrived on time), which is exactly why the early band is tracked
separately instead of being folded into "not late". It is also worth one more
look before it goes in a scorecard: 38% is high enough to suspect the TTC
static's timepoints as well as TTC's driving.

By local hour, the 8am and 5pm peaks (routes with ≥500 events in the hour):

| City | 8am OTP | 5pm OTP |
|---|---:|---:|
| Tokyo* | 93.0% | 99.3% |
| Zurich | 95.9% | 91.9% |
| Helsinki | 79.7% | 75.4% |
| New York | 65.3% | 68.6% |
| Boston | 57.9% | 50.4% |
| SF Bay Area | 59.3% | 54.3% |
| Washington DC | 53.6% | 49.3% |
| Toronto | 50.2% | 46.1% |

The near-universal pattern holds: the evening peak is worse than the morning
one everywhere except New York — and now Tokyo, whose stated-delay feed reads
*better* at 5pm than 8am (99.3% vs 93.0%); with minute-rounded operator
numbers, treat that as what Toei asserts, not an independent measurement.
Zurich stays above 91% in both peaks.

## Q2 — Average delay

Signed seconds, positive = late.

| City | Mean | Median | p90 | p99 |
|---|---:|---:|---:|---:|
| Tokyo* | 28 | **0** | 0 | 720 |
| Toronto | 52 | **−5** | 377 | 1,495 |
| New York | 59 | 3 | 300 | 1,071 |
| Helsinki | 69 | 38 | 245 | 680 |
| Zurich | 82 | 60 | 180 | 420 |
| SF Bay Area | 174 | 61 | 491 | 2,485 |
| Washington DC | 173 | 92 | 564 | 1,824 |
| Boston | 191 | 133 | 615 | 1,748 |

**The means took two rounds to become usable, and the second round is this
week's.** The 2026-08-26 edition reported SF at a mean of −19,027s and told you
to distrust the mean; 2026-08-31 bounded corrupt *stated* delays and the means
looked healed. They were not: a second artifact family — trips matched to the
wrong calendar day, yielding delays of ±(86400 − true delay), just *under* the
1-day bound — kept 57,153 events polluting the means while every median held
(docs/08, 2026-09-10). With the bound tightened to 12h and history healed
through the fact layer, New York's mean moved from −193s to +59s and Toronto's
from −35s to +52s **without either median moving more than a second** — the
fingerprint of removing an artifact rather than reshaping the distribution.
Every city's mean now sits above its median by an amount a right tail of real
late vehicles explains.

Still prefer the median when quoting a single figure. Toronto's negative median
(−5s) is real early-running — see Q1. Tokyo's 0/0/0 row is the quantization
caveat made visible: 97% of its stated delays are exactly zero, so mean 28s
comes almost entirely from its p99 tail.

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
| Toronto | 7,358,651 | 1,041s | 696s | **12.6%** | 5.5% |
| New York | 1,708,243 | 591s | 468s | 12.5% | 3.5% |
| Boston | 2,796,811 | 1,577s | 1,166s | 11.5% | 2.9% |
| Washington DC | 3,475,309 | 1,357s | 1,192s | 8.3% | 4.9% |
| Zurich | 3,263,249 | 1,295s | 900s | 7.4% | 4.2% |
| SF Bay Area | 3,724,011 | 1,804s | 1,420s | 5.3% | 5.7% |
| Helsinki | 1,972,583 | 1,555s | 1,092s | **2.3%** | 2.5% |

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
| Helsinki | 185 | 107s | **23s** |
| New York | 362 | 56s | 28s |
| Boston | 342 | 102s | 70s |
| Zurich | 570 | 109s | 70s |
| Toronto | 1,149 | 576s | 74s |
| SF Bay Area | 217 | 194s | 78s |
| Washington DC | 253 | 176s | 97s |

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
| Boston | 3.606% | 257,552 |
| SF Bay Area | 1.664% | 289,449 |
| Zurich | 0.851% | 544,189 |
| Helsinki | 0.303% | 156,461 |
| Washington DC | 0.043% | 244,224 |
| Toronto | 0.000% | 749,257 |
| New York | 0.000% | 147,119 |

| City | Alerts | Alert-hours | Routes alerted |
|---|---:|---:|---:|
| New York | 19,516 | 426,835 | 8 |
| Boston | 2,426 | 33,685 | 162 |
| SF Bay Area | 2,414 | 45,303 | 141 |
| Toronto | 2,349 | 15,268 | 187 |
| Zurich | 1,355 | 29,340 | 621 |
| Helsinki | 799 | 12,471 | 98 |
| Washington DC | 625 | 2,551 | 113 |
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
(124,738 hourly rows, 2024-08-19 → today, all seven cities live at the time), so
the moment a snowy day happens, the comparison exists. **[rev 2026-09-03]** Tokyo
joined after that backfill and therefore has only the forward-filled hours (216
at the time of writing). That is not a gap: Tokyo's transit data starts
2026-09-03, and weather is only ever joined to days we actually scored, so its
weather covers its whole scoreable history. Re-running the 2-year backfill just
to square the row counts would spend 8 cities' worth of Open-Meteo calls to
populate hours no fact table can reach.

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
| Toronto | 1,481 | 98.9% | 238,404 | 236,175 | 1 |
| New York | 223 | 96.8% | 59,855 | 61,394 | 0 |
| Helsinki | 2,674 | 94.2% | 56,768 | 141,967 | 155 |
| Washington DC | 760 | 93.6% | 91,215 | 84,414 | 124 |
| Boston | 1,198 | 91.7% | 110,971 | 105,780 | 5,647 |
| Zurich | 2,283 | 90.0% | 180,266 | 168,266 | 3,013 |
| SF Bay Area | 3,210 | 83.9% | 120,291 | 134,073 | 3,483 |

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
| **ODPT** (Tokyo) | `odpt:Train` JSON + ToeiBus GTFS-RT (VP only) | Toei subway (4 lines, stated delay) + Arakawa tram (position only) + Toei bus (positions only) | Live since 2026-09-03: 4 subway lines scored, Arakawa tram volume-only, Toei bus positions. NipporiToneri publishes no realtime at all. | Key active. Center serves **no Metro odpt:Train** — Metro is line-status text only, so Tokyo OTP rests on Toei rail. Rail GTFS-RT is alerts-only, so the JSON API is the path. **`odpt:delay` is minute-quantized** (see caveat below). |
| **Open-Meteo** | Hourly forecast JSON | n/a | Collected, unused | 1,776 rows in silver, no dbt model reads them. |

### Tokyo's punctuality number is not measured the way everyone else's is

[added 2026-09-03, measured on the first day of live Toei data] Seven of the
eight cities get their delay from the canonical COALESCE — a feed-stated delay
where one exists, otherwise the arrival timestamp minus the static schedule —
and both sides of that resolve to the second. Tokyo does not. Toei states its
own delay in `odpt:delay`, and **every value it has ever published to us is a
whole minute**: 0 (13,506 events), 60 (284), 120 (69), 180 (20), 240 (12), 300
(2). 97.2% are exactly zero, because anything under a minute rounds to zero at
the source.

So a Tokyo on-time rate near 100% is partly a real fact about Toei and partly an
artifact of a coarser ruler. Any ranking that puts Tokyo against NYC or Helsinki
has to say so. Two rules follow, and both are already implemented rather than
just intended: the tram (`Toei.Arakawa`) publishes position but never a delay,
so its 2,710 events count as service volume and are excluded from OTP — the
denominators use `count(delay_arr_sec)`; and any chart or sentence that ranks
Tokyo alongside the others carries the operator-stated, minute-rounded caveat
inline, not in a footnote.

Two feeds carry a mode nobody else does: 511's **cable car** and Zurich's
**funicular/aerial lift**. Two modes are systematically missing everywhere they
exist: **ferries** (agencies publish them outside GTFS-RT) and **Toronto's
subway** (TTC publishes no realtime for it).
