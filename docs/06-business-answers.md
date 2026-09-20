# 06 — What the warehouse currently answers

Every number on this page came out of `analysis/business_questions.sql` run
against `TRANSIT.GOLD` on **2026-09-20 21:16Z**, over the data collected since
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

**Read the caveats before quoting anything.** This is about four weeks of data,
not a season. Cities onboarded on different days (`dim_city.metrics_from`), so the
denominators are not equal, and a "most reliable city" ranking off this window
would be dishonest — which is precisely why `fct_city_scorecard`
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
rather than a gap: `fct_city_scorecard` exists, is built by every
chain run, and deliberately holds ZERO rows until a city accrues 20 closed
judged days across its judged window ([rev 2026-09-10] window grain — the month
boundary was measuring the calendar, not the evidence; first cities cross 20
around Sep 12-14). Every input to it exists and is measured below, each
of the eight sub-questions from `docs/transit-pulse-plan.md` §1 with its
current answer.

---

## Q1 — On-time performance

Share of scored stop arrivals per band. `on_time` is −60s to +299s; `early` is
more than 60s ahead; `very_late` is 15 min or worse.

| City | Scored events | On time | Early | Late | Very late |
|---|---:|---:|---:|---:|---:|
| Tokyo* | 707,440 | **97.9%** | 0.0% | 1.6% | 0.5% |
| Zurich | 10,705,953 | **96.2%** | 1.2% | 2.4% | 0.2% |
| Helsinki | 12,715,052 | **81.3%** | 11.9% | 6.5% | 0.4% |
| New York | 4,204,996 | 67.9% | 20.8% | 9.6% | 1.7% |
| SF Bay Area | 16,485,117 | 59.8% | 20.7% | 15.8% | 3.8% |
| Boston | 8,027,941 | 56.8% | 14.1% | 23.9% | 5.3% |
| Washington DC | 9,391,638 | 54.9% | 21.5% | 19.1% | 4.4% |
| Toronto | 21,586,957 | 50.0% | **35.7%** | 11.0% | 3.3% |

*Tokyo's number is operator-stated and minute-rounded (see the provenance
section at the bottom of this page): sub-minute lateness reads as on-time by
construction, and its 0.0% early is an artifact of the same quantization —
Toei never states a negative delay. First place with an asterisk that must
travel with it everywhere.

Zurich's 96.2% — first among the schedule-MEASURED cities (Tokyo's asterisked
number above it is the operator's own claim) — is the real number, and a lesson. Until
2026-08-27 its whole network was silently reduced to ~3k events because the Swiss
feed publishes delay-only predictions (no timestamps) and finalization demanded
timestamps; the completeness gate caught it on Zurich's FIRST judged day and the
delay_plus_schedule finalization method fixed it (docs/08). Swiss punctuality
lives up to its reputation: fifteen points clear of third, with only 1.2% early
and 0.2% very late — the tightest measured distribution of any city here.

**Toronto's 36% early is the finding here, not its 50.0% on-time.** Toronto is
not late — it is *ahead of schedule* four times out of ten. For a bus network
that is a genuine service defect (a bus that leaves a timepoint early strands
riders who arrived on time), which is exactly why the early band is tracked
separately instead of being folded into "not late". It is also worth one more
look before it goes in a scorecard: 36% is high enough to suspect the TTC
static's timepoints as well as TTC's driving — and the timepoint-scoped cut
agrees: 18% of Toronto's measured bus timepoint departures leave more than 60s
early, ninety times Helsinki's rate (site: "Who leaves early?").
[rev 2026-09-17] The same cut now runs for every mode whose static marks
timepoints, not only bus, and the modes are the story: Toronto's streetcars leave
early at 22.5% of measured timepoint departures against 17.6% for its buses, and
Boston's commuter rail at 15.5% against 1.8% for its buses — a rail early-departure
rate no other city comes near (Helsinki rail 0.0%, SF rail 0.0%). The mart's
`early_departure_flag` keeps the locked bus rule; the finding widens the question.

By local hour, the 8am and 5pm peaks (routes with ≥500 events in the hour):

| City | 8am OTP | 5pm OTP |
|---|---:|---:|
| Tokyo* | 95.0% | 99.4% |
| Zurich | 96.1% | 92.1% |
| Helsinki | 80.4% | 76.0% |
| New York | 66.1% | 68.3% |
| Boston | 56.3% | 49.9% |
| SF Bay Area | 59.5% | 54.4% |
| Washington DC | 54.1% | 49.6% |
| Toronto | 50.7% | 46.2% |

The near-universal pattern holds: the evening peak is worse than the morning
one everywhere except New York — and now Tokyo, whose stated-delay feed reads
*better* at 5pm than 8am (99.4% vs 95.0%); with minute-rounded operator
numbers, treat that as what Toei asserts, not an independent measurement.
Zurich stays above 91% in both peaks.

## Q2 — Average delay

Signed seconds, positive = late.

| City | Mean | Median | p90 | p99 |
|---|---:|---:|---:|---:|
| Tokyo* | 20 | **0** | 0 | 540 |
| Toronto | 67 | **0** | 417 | 1,564 |
| New York | 64 | 4 | 307 | 1,127 |
| Helsinki | 67 | 38 | 242 | 659 |
| Zurich | 81 | 60 | 174 | 420 |
| SF Bay Area | 173 | 60 | 488 | 2,500 |
| Washington DC | 173 | 91 | 562 | 1,827 |
| Boston | 201 | 139 | 644 | 1,823 |

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

[rev 2026-09-12] These numbers are the first computed after two corrections that
both removed measurements rather than adding them. Arrivals that landed while a
city's feed was dark no longer score (1,004,952 events across every known outage —
docs/08 2026-09-11), and Toronto 09-03..05, which a TTC timetable rotation had left
with volume and no delays, were recomputed against the static that was live those
days (+3.1M scored events). **No city's on-time share moved more than 0.2 points**
through either change, which is the strongest evidence yet that the ranking reflects
service and not our collection artifacts.

Still prefer the median when quoting a single figure. Toronto's negative median
(−5s) is real early-running — see Q1. Tokyo's 0/0/0 row is the quantization
caveat made visible: 97% of its stated delays are exactly zero, so mean 20s
comes almost entirely from its p99 tail.

## Q3 — Service volume

Peak hour per mode. `vehicle_id_reliable = false` means the feed does not
identify vehicles, so `peak_vehicles` there is a floor.

| City | Mode | Peak vehicles | Peak trips | Peak routes |
|---|---|---:|---:|---:|
| SF Bay Area | bus | 1,667 | 3,004 | 468 |
| Toronto | bus | 1,480 | 3,281 | 194 |
| Washington DC | bus | 1,025 | 1,771 | 126 |
| New York | metro | 1,045 *(id unreliable)* | 1,045 | 28 |
| Boston | bus | 726 | 1,506 | 151 |
| Toronto | tram | 207 | 439 | 16 |
| SF Bay Area | tram | 145 | 221 | 13 |
| Washington DC | metro | 127 | 231 | 6 |
| Boston | tram | 87 | 206 | 5 |
| Boston | rail | 60 | 80 | 14 |
| Boston | metro | 53 | 154 | 3 |
| SF Bay Area | rail | 44 | 47 | 7 |
| SF Bay Area | ferry | 13 | 31 | 7 |
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
| Toronto | 18,955,094 | 970s | 689s | **14.0%** | 5.9% |
| New York | 4,424,454 | 584s | 463s | 12.0% | 3.3% |
| Boston | 7,779,063 | 1,533s | 1,139s | 12.0% | 2.8% |
| Washington DC | 9,518,823 | 1,322s | 1,178s | 8.4% | 4.6% |
| SF Bay Area | 13,591,450 | 1,554s | 1,208s | 6.7% | 5.1% |
| Zurich | 10,501,566 | 1,278s | 900s | 5.8% | 3.2% |
| Helsinki | 11,999,273 | 1,480s | 1,046s | **2.6%** | 1.8% |

New York's bunching jumped from 7.7% to 11.0% with the 7-line repair — the
recovered northbound trips were exactly the dense-headway service where
bunching lives, a reminder that a silent data loss biases metrics, not just
volumes.

Helsinki bunches roughly a quarter as often as Toronto. This is the single
clearest cross-city separation in the data so far, and it is consistent with
Helsinki also topping OTP.

## Q5 — Excess Wait Time

Only defined for frequent service (scheduled headway ≤ 10 min), where riders
turn up without a timetable. EWT is how much longer they actually wait than the
schedule promises.

| City | Route-days | Mean EWT | Median EWT |
|---|---:|---:|---:|
| Helsinki | 1,852 | 58s | **21s** |
| New York | 1,022 | 95s | 27s |
| Zurich | 2,168 | 98s | 47s |
| Boston | 1,006 | 74s | 67s |
| Toronto | 3,290 | 389s | 73s |
| SF Bay Area | 1,143 | 275s | 80s |
| Washington DC | 777 | 126s | 84s |

> **[rev 2026-09-12] Tokyo is absent from the Q4 and Q5 tables above because its
> schedule was never joined, and it is now present in gold — but read it with the same
> asterisk as its on-time share.** `fct_headways` and `int_service_frequency` both took
> their scheduled side from `int_gtfs_scheduled_stop_times`, a GTFS path Tokyo never
> enters (its schedule is `odpt:TrainTimetable`). Tokyo therefore carried 362,473 real
> observed gaps with `sched_headway_sec` NULL on every one, so `gap_ratio`, `bunched_flag`
> and `big_gap_flag` — all of which divide by it — nulled out together, and `is_frequent`
> was unknown so EWT never ran. Fixed via `int_odpt_scheduled_stop_times` →
> `int_scheduled_stop_times`; scheduled-headway coverage went 0.00% → 100.00% with **no
> other city moving a decimal place** on bunching, EWT or OTP, and NYC's composite
> unchanged at 85.8.
>
> **Tokyo's bunching and EWT are then deliberately NULL, and stay absent from these
> tables.** Computed naively it read 0.7% bunched and a 2s median EWT — best in the fleet —
> but Toei publishes a stated delay rather than an arrival time, so an arrival is
> reconstructed as schedule + stated delay, and because 91% of those delays are exactly
> zero, **90.4% of Tokyo's consecutive-train gaps equal the scheduled gap exactly**, against
> 0.2–4.1% in every other city. Gap regularity is only a measurement where the observed side
> is independent of the scheduled side, so `dim_city.gap_regularity_measurable` declares the
> city, `fct_headways` nulls `gap_ratio`/`bunched_flag`/`big_gap_flag`, and EWT is gated on
> the same flag — the mechanism `schedule_matchable` already uses for an id mismatch, and
> the same rule that keeps ADDED trips out of OTP. `assert_gap_regularity_gate_holds` keeps
> it nulled. What Tokyo keeps: scheduled headway and `is_frequent`, which are real facts
> from its published timetable.

Toronto's median EWT collapsed from the provisional 372s to 56s once the
grain-and-evidence rewrite weighted route-days properly and more days closed —
the earlier magnitude was exactly the partial-day artifact the docs warned
about. The Helsinki-vs-everyone gap (20s median) survives and still lines up
with its bunching rate.

## Q6 — Cancellations and disruptions

Two separate aggregates on purpose — joining alerts to route-days fans out and
inflates the cancel rate.

| City | Mean cancel % | Scheduled trips |
|---|---:|---:|
| Boston | 1.883% | 660,563 |
| SF Bay Area | 1.825% | 1,017,701 |
| Zurich | 0.326% | 1,672,401 |
| Helsinki | 0.026% | 952,651 |
| Washington DC | 0.002% | 654,597 |
| Toronto | 0.001% | 1,923,012 |
| Tokyo | 0.000% | 79,656 |
| New York | 0.000% | 362,988 |

| City | Alerts | Alert-hours | Routes alerted |
|---|---:|---:|---:|
| New York | 64,706 | 1,496,369 | 9 |
| Toronto | 6,968 | 39,888 | 220 |
| Boston | 6,605 | 88,481 | 177 |
| SF Bay Area | 6,501 | 125,740 | 188 |
| Washington DC | 2,178 | 10,797 | 123 |
| Zurich | 2,065 | 42,303 | 661 |
| Helsinki | 1,789 | 29,193 | 171 |
| Tokyo | 165 | 3,480 | 14 |

A zero cancel rate for NYC and Tokyo means *their feeds never emit CANCELED*,
not that nothing was cancelled: all 13.4M NYC rows are SCHEDULED, and
`odpt:Train` has no cancellation field at all. **[rev 2026-09-12] Toronto was
listed here as a third never-emitting feed and that was wrong.** TTC does emit
CANCELED — 11 raw rows resolving to 4 cancelled trips, on 2 days out of 19, or
0.00061% of 661,306 scheduled trips. It rounds to 0.00% at two decimals, which
is why it read as a never-emitter; the site now renders any nonzero count below
a hundredth of a percent as `<0.01%` rather than as a bare zero. Three states
therefore exist in this column and must not be collapsed: feeds that cannot say
it, feeds that can and essentially never do, and feeds that say it and mean it.
Cancellation rate is a measure of feed behaviour as much as of service, and must
not be scored across cities without that caveat. NYC's alert profile is the opposite extreme: 1,441 alerts
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
| New York | rain | 103,963 | 66.9% | −0.4pp |
| New York | heavy_rain | 7,692 | 70.2% | +2.9pp |
| Zurich | rain | 735,507 | 96.0% | −0.3pp |
| Helsinki | cloudy | 6,846,097 | 81.6% | +0.3pp |
| SF Bay Area | fog | 1,758,298 | 59.8% | +0.9pp |
| Boston | cloudy | 2,544,835 | 56.3% | +0.4pp |

The counterintuitive early signal — NYC and Zurich running *better* in rain —
did not survive its denominator: on 104k and 736k rain events, both now read
a few tenths of a point *below* their own dry OTP, which is exactly why
`n_scored` rides on every cell. A few thousand events from one rainy evening
is an anecdote with a denominator, not a finding. The question
this table is really waiting for (Helsinki-in-snow vs Boston-in-snow) needs
winter; the machinery for it is now fully wired and tested.

## Q8 — Data completeness

Only closed local days at or after each city's `metrics_from` are judged.

| City | Route-days judged | Mean completeness | Scheduled | Observed | Cancelled |
|---|---:|---:|---:|---:|---:|
| New York | 625 | 98.3% | 175,318 | 173,717 | 0 |
| Toronto | 4,698 | 95.9% | 760,198 | 724,618 | 4 |
| Helsinki | 8,595 | 93.1% | 480,431 | 448,668 | 168 |
| Washington DC | 2,647 | 92.6% | 315,226 | 283,944 | 11 |
| Boston | 3,714 | 91.2% | 335,670 | 324,226 | 7,409 |
| Zurich | 9,548 | 90.5% | 782,571 | 736,455 | 4,642 |
| SF Bay Area | 11,184 | 83.0% | 545,267 | 464,026 | 13,986 |
| Tokyo | 96 | 80.6% | 43,254 | 34,414 | 0 |

**P3's "completeness ≥85%" acceptance criterion: PASSED by six of the eight
cities.** SF Bay Area (83.0%) and Tokyo (80.6%) read below the bar — SF because
roughly a tenth of the operators behind the 511 feed publish a schedule and no
realtime at all (Q8c), Tokyo on 96 route-days, the thinnest denominator on the
page. **[rev 2026-09-20] This cut now skips retired days.** An agency keeps
publishing next-day trips after its poller stops, so the days nobody watched
arrive scheduled and unobserved: counting them read Helsinki at 80.7% against
its true 93.1%, and put three cities under the bar that had never been near it.
`retired_day` is the same flag both completeness tests and the scorecard use.

---

## Where coverage is actually missing, by mode

`never_sched` isolates dead variant rows in the static — Boston publishes 368
bus routes of which 214 never run — so **`sched_not_obs` is the honest gap**:
service that is scheduled and that we never saw in realtime.

| City | Mode (`route_type`) | Static routes | Never scheduled | Scheduled, never seen | Observed |
|---|---|---:|---:|---:|---:|
| Boston | tram 0 (Green Line) | 5 | 0 | 0 | 5 |
| Boston | metro 1 | 3 | 0 | 0 | 3 |
| Boston | rail 2 (commuter) | 14 | 0 | 0 | 14 |
| Boston | bus 3 | 368 | 214 | 2 | 152 |
| Boston | **ferry 4** | 9 | 0 | **9** | **0** |
| DC | metro 1 | 6 | 0 | 0 | 6 |
| DC | bus 3 | 126 | 0 | 0 | 126 |
| Helsinki | tram 0 | 30 | 3 | 1 | 26 |
| Helsinki | metro 1 | 4 | 0 | 0 | 4 |
| Helsinki | **ferry 4** | 3 | 0 | **3** | **0** |
| Helsinki | suburban rail 109 | 13 | 0 | 0 | 13 |
| Helsinki | regional bus 701 | 356 | 4 | 5 | 347 |
| Helsinki | express bus 702 | 14 | 0 | 0 | 14 |
| Helsinki | local bus 704 | 46 | 0 | 2 | 44 |
| Helsinki | light rail 900 | 1 | 0 | 0 | 1 |
| NYC | metro 1 | 28 | 0 | 0 | 28 |
| NYC | rail 2 (SIR) | 1 | 0 | 0 | 1 |
| SF | tram 0 | 16 | 1 | 2 | 13 |
| SF | metro 1 | 12 | 0 | 2 | 10 |
| SF | rail 2 | 8 | 0 | 0 | 8 |
| SF | bus 3 | 615 | 4 | **45** | 566 |
| SF | ferry 4 | 14 | 0 | 7 | 7 |
| SF | cable car 5 | 3 | 0 | 0 | 3 |
| Toronto | tram 0 (streetcar) | 17 | 0 | 0 | 17 |
| Toronto | bus 3 | 212 | 0 | 5 | 207 |
| Toronto | ferry 4 | 1 | 0 | 0 | 1 |
| Zurich | S-Bahn 109 | 62 | 12 | 0 | 50 |
| Zurich | bus 700 | 440 | 38 | 34 | 368 |
| Zurich | tram 900 | 22 | 0 | 1 | 21 |
| Zurich | boat 1000 | 9 | 0 | 9 | 0 |

**DC is complete** — every scheduled route is observed; Toronto is five bus
routes short of it. Zurich's "scheduled, never seen" counts have collapsed as
days accrued — S-Bahn to zero, bus from 294 to 34 — which is what a coverage
read maturing actually looks like.

**Ferries are the systematic hole.** Boston 9 routes, Helsinki 3, Zurich's
boats 9 of 9, SF 7 of 14 — scheduled, none (or few) observed. Helsinki's is
confirmed agency-side (AIS→MQTT, `HSLdevcom/suomenlinna-ferry-hfp`); Boston's
was proven upstream on 2026-08-26 — **zero silver rows ever** for any Boat-%
route — and is now a seeded known coverage gap, alongside its Orange-Line
rail-replacement shuttles (same proof, 0 of 2,791 scheduled shuttle trips ever
published in the feed).

**SF's 45 unobserved bus routes** (down from 75 in the 2026-09-12 run) are not
one failure but many small ones. The
511 feed aggregates ~30 operators under a single `city_key`, and they do not
all publish realtime:

| Operator prefix | Routes scheduled | Observed |
|---|---:|---:|
| AC (AC Transit) | 123 | 123 |
| SM (SamTrans) | 75 | 75 |
| SC (VTA) | 71 | 63 |
| SF (Muni) | 68 | 68 |
| CC (County Connection) | 51 | 51 |
| WH | 30 | 30 |
| SO | 26 | 26 |
| **CM** | 20 | **0** |
| PE | 11 | 11 |
| UC | 6 | 6 |
| **GF** | 5 | **0** |
| **MB** | 4 | **0** |
| **SS** | 3 | **0** |

The large operators run 89–100% coverage. Ten operators publish schedule and no
realtime whatsoever — CM, GF, MB and SS above, plus six one- and two-route
operators (SL, GP, RV, TF, AF, EE); PE and UC, blind in the previous read, now
publish in full. Treating "SF Bay Area" as one city therefore
averages a well-instrumented core with a handful of blind operators — worth
stating explicitly wherever an SF number is quoted.

---

## What modes each source actually gives us

| Source | Feed | Modes in the schedule | Modes we actually observe | Notes |
|---|---|---|---|---|
| **MTA** (NYC) | 8 GTFS-RT line-group feeds, keyless | Subway (28 routes), Staten Island Railway | Subway 28/28, SIR | Buses deliberately out of scope. No vehicle ids and no lat/lon on subway VP. `arrival.delay` absent except on the L feed. |
| **MBTA** (Boston) | GTFS-RT TU/VP/Alerts, keyless | Green Line (tram), subway, commuter rail, bus, **ferry** | All but ferry | Cleanest feed of the set. 214 of 368 static bus routes are non-running variants. |
| **TTC** (Toronto) | `bustime.ttc.ca` GTFS-RT | Streetcar, bus, 1 ferry route | All three modes; 207 of 212 bus routes | **Surface only — the subway publishes alerts, never positions or predictions.** Toronto's score is a surface-mode score, by design. Static must be `SurfaceGTFS.zip`. |
| **HSL** (Helsinki) | GTFS-RT TU + alerts, keyless | Tram, metro, suburban rail, regional/express/local bus, light rail, **ferry** | All but ferry | **No VehiclePositions feed at all.** Ferry realtime is AIS→MQTT, outside GTFS-RT. `trip_id` is empty; trips resolve on (route, direction, date, start time). |
| **WMATA** (DC) | GTFS-RT rail + bus, keyed | Metrorail, Metrobus | Both, 100% | Two static zips (rail + bus) landed under one `gtfs_version_id`. |
| **511.org** (SF Bay) | Regional aggregated GTFS-RT, `agency=RG`, keyed | ~30 operators: bus, tram, metro, regional rail, ferry, **cable car** | Most, with real per-operator gaps | 60 req/hr cap → 200s cadence. Cable car (`route_type` 5) is unique to this feed. |
| **opentransportdata.swiss** (Zurich) | National `/la/gtfs-rt`, keyed | Whole Swiss network — filtered to 615 Zurich routes: S-Bahn, tram, bus, boat, aerial lift, funicular | Bus, tram, S-Bahn observed (368/21/50 routes); boats not yet | Live since 2026-08-25. National rail classes (TGV/ICE/IC/IR/RE/EXT) are excluded from the allow-list on purpose; S-Bahn (`route_type` 109) is kept. No VehiclePositions product on the Swiss LA API — trip updates only, like Helsinki. |
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
