# 06 — What the warehouse currently answers

Every number on this page came out of `analysis/business_questions.sql` run
against `TRANSIT.GOLD` on **2026-09-26 ~05:00Z**, over everything collected
2026-08-22 → 2026-09-25. Re-run that file rather than editing numbers here by hand.

**[rev 2026-09-26] These are the final numbers.** Every poller is retired — seven on
2026-09-15/16, Tokyo on 2026-09-25 — so nothing below will move again unless history is
recomputed. Of Q1–Q8, only Tokyo's figures changed since the 2026-09-20 run — the other
seven cities' history was already closed — though Q3, Q7 and the coverage table now show
every row their queries return, and Q4's query gained a Tokyo guard. Q9–Q12 are new: four cuts the public site shows,
matching its snapshot `site/data/extras.json` (as_of 2026-09-26 04:46 UTC), which the same
queries reproduce (approximate percentiles to within a second).

**The means have been trustworthy since the 2026-08-31 rebuild.** Two defects were fixed
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
  2026-08-26 run and reads +173s now. Earlier editions of this page told you
  to distrust the mean; you no longer have to.

**Read the caveats before quoting anything.** [rev 2026-09-26] Each city has three to
four weeks of data and 21–23 judged days — not a season. Cities onboarded and retired on
different days (`dim_city.metrics_from`, `retired_from`), so the denominators are not equal;
`fct_city_scorecard` refused to rank a city until it had 20 closed judged days, and all eight
now have them. Two families of number live on this page and do not reconcile to the
decimal: Q1–Q8 read every scored event, while the scorecard, the site's standings and
Q9–Q12 read only judged route-days (closed local day, at or after `metrics_from`, not a
`retired_day`, completeness ≥ 0.50). What the numbers establish is that the pipeline
produces plausible, internally consistent, cross-city-comparable measurements. The 7-line
regex casualty is repaired (NYC gained ~350 northbound trips/day) and weather is joined
into gold (Q7, Q10).

---

## The headline question

> Which cities run the most reliable public transit — and what makes them
> reliable?

**[rev 2026-09-25] Answered for all eight cities.** `fct_city_scorecard` holds no row
until a city has 20 closed judged days across its judged window; New York crossed first
on 09-11, Tokyo last on 09-24, and every poller is now retired. From the Headline query
at the end of `analysis/business_questions.sql`:

| City | Score | Judged days | Window | Wait | On-time | Cancel | Bunching |
|---|---:|---:|---|---:|---:|---:|---:|
| Tokyo* | **98.8** | 21 | 09-04 – 09-24 | — | 98.1 | 100.0 | — |
| Zurich | **93.6** | 21 | 08-26 – 09-15 | 87.5 | 96.4 | 99.4 | 94.3 |
| Helsinki | 92.2 | 22 | 08-24 – 09-14 | 94.9 | 81.3 | 100.0 | 97.4 |
| New York | 85.8 | 23 | 08-23 – 09-14 | 92.1 | 67.8 | 100.0 | 87.9 |
| Boston | 81.1 | 22 | 08-24 – 09-14 | 89.3 | 56.6 | 98.2 | 88.3 |
| Washington DC | 80.1 | 21 | 08-25 – 09-14 | 85.4 | 55.0 | 100.0 | 91.6 |
| Toronto | 72.7 | 22 | 08-24 – 09-14 | 70.9 | 50.0 | 100.0 | 86.0 |
| SF Bay Area | 60.5 | 21 | 08-25 – 09-14 | 26.1 | 59.9 | 97.2 | 93.4 |

Two readings before quoting the table. **Tokyo's 98.8 is not comparable** in the way the
others are: its delay is operator-stated and minute-rounded (see "Tokyo's punctuality
number" below), its wait and bunching are NULL by design, so its score rests on on-time
and a cancellation component. **And a cancellation sub-score of 100 is not always
evidence**: New York's and Tokyo's feeds have no CANCELED vocabulary, so their zero cancel
rate is a feed property, yet it still counts. Dropped as unmeasured, New York reads 82.2
and Tokyo 98.1 — the order does not change. Among the seven cities measured against
their own timetable to the second, **Zurich leads at 93.6**. Every input is measured
below, each of the eight sub-questions from `docs/transit-pulse-plan.md` §1 with its
current answer, then four further cuts (Q9–Q12).

[rev 2026-09-26] The public page now shows Tokyo's numbers without a marker, by the
owner's decision. This doc keeps the method note ("Tokyo's punctuality number" below) and
keeps the asterisk on Tokyo in its tables.

---

## Q1 — On-time performance

Share of scored stop arrivals per band. `on_time` is −60s to +299s; `early` is
more than 60s ahead; `very_late` is 15 min or worse.

| City | Scored events | On time | Early | Late | Very late |
|---|---:|---:|---:|---:|---:|
| Tokyo* | 821,235 | **98.1%** | 0.0% | 1.5% | 0.4% |
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
Toei never states a negative delay. First place with an asterisk that travels
with it everywhere in this doc. [rev 2026-09-26] The public page prints Tokyo's
number without a marker, by the owner's decision; the method note stays here.
Tokyo's row now runs to its retirement (707,440 events at 97.9% in the 09-20 run);
no other row moved, because the other seven pollers had already retired.

[rev 2026-09-26] DC's buses are unscored on 2026-09-10..12 — only 7% of those three
judged days' events are scored (Metrorail only), because they were recomputed against a
static whose calendar does not cover them; DC's on-time share is 54.98% with them and
54.89% without (0.09 pt), its score 80.1 either way, and the gap is recorded rather than
repaired (docs/08, 2026-09-26).

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
| Tokyo* | 95.7% | 99.3% |
| Zurich | 96.1% | 92.1% |
| Helsinki | 80.4% | 76.0% |
| New York | 66.1% | 68.3% |
| Boston | 56.3% | 49.9% |
| SF Bay Area | 59.5% | 54.4% |
| Washington DC | 54.1% | 49.6% |
| Toronto | 50.7% | 46.2% |

The near-universal pattern holds: the evening peak is worse than the morning
one everywhere except New York — and now Tokyo, whose stated-delay feed reads
*better* at 5pm than 8am (99.3% vs 95.7% [rev 2026-09-26]); with minute-rounded operator
numbers, treat that as what Toei asserts, not an independent measurement.
Zurich stays above 91% in both peaks.

## Q2 — Average delay

Signed seconds, positive = late.

| City | Mean | Median | p90 | p99 |
|---|---:|---:|---:|---:|
| Tokyo* | 19 | **0** | 0 | 480 |
| Toronto | 67 | **0** | 417 | 1,564 |
| New York | 64 | 4 | 307 | 1,127 |
| Helsinki | 67 | 38 | 242 | 659 |
| Zurich | 81 | 60 | 174 | 420 |
| SF Bay Area | 173 | 60 | 488 | 2,500 |
| Washington DC | 173 | 91 | 562 | 1,827 |
| Boston | 201 | 139 | 644 | 1,823 |

**The means took two rounds to become usable; the second landed on
2026-09-10.** The 2026-08-26 edition reported SF at a mean of −19,027s and told you
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

Still prefer the median when quoting a single figure — and Q9 for how far the
tail reaches. [rev 2026-09-26] Toronto's median of exactly 0s sits between real early
running (35.7% of arrivals more than a minute ahead, Q1) and a long late tail (p99
1,564s); it is not evidence of punctuality. Tokyo's 0/0/0 row is the quantization caveat
made visible: p90 = 0 and Toei never states a negative delay, so at least nine in ten of
its stated delays are exactly zero, and its 19s mean comes almost entirely from the tail
(p99 480s).

## Q3 — Service volume

Peak hour per mode. `vehicle_id_reliable = false` means the feed does not
identify vehicles, so `peak_vehicles` there is a floor.

| City | Mode | Peak vehicles | Peak trips | Peak routes |
|---|---|---:|---:|---:|
| SF Bay Area | bus | 1,667 | 3,004 | 468 |
| Toronto | bus | 1,480 | 3,281 | 194 |
| Tokyo | bus | 1,051 | 2,111 | 140 |
| New York | metro | 1,045 *(id unreliable)* | 1,045 | 28 |
| Washington DC | bus | 1,025 | 1,771 | 126 |
| Boston | bus | 726 | 1,506 | 151 |
| Toronto | tram | 207 | 439 | 16 |
| SF Bay Area | tram | 145 | 221 | 13 |
| Washington DC | metro | 127 | 231 | 6 |
| Boston | tram | 87 | 206 | 5 |
| Boston | rail | 60 | 80 | 14 |
| Boston | metro | 53 | 154 | 3 |
| SF Bay Area | rail | 44 | 47 | 7 |
| SF Bay Area | other (cable car) | 27 | 54 | 3 |
| New York | rail (SIR) | 13 *(id unreliable)* | 13 | 1 |
| SF Bay Area | ferry | 13 | 31 | 7 |
| Toronto | ferry | 2 | 2 | 1 |

[rev 2026-09-26] Tokyo's row is Toei bus, which publishes positions only: it counts as
volume here and is never scored. Tokyo's scored rail comes from `odpt:Train` JSON, not a
GTFS-RT VehiclePositions feed, so it never reaches this table. The query also returns `unknown`
rows — positions whose route id is missing or does not resolve against the static (Toronto
1,458 vehicles, SF 1,014, DC 645, NYC 10 at peak) — left out above because they carry no
mode.

**Helsinki is absent from this table entirely.** HSL publishes no
VehiclePositions feed — trip updates only — so there is nothing to count
vehicles from. Helsinki's OTP and headway numbers are unaffected (both are
derived from stop-time predictions), but any "how many vehicles are moving"
question is structurally unanswerable for Helsinki from GTFS-RT. Zurich is absent for
the same reason: the Swiss LA API has no VehiclePositions product.

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

Helsinki bunches about a fifth as often as Toronto. This is the single
clearest cross-city separation in the data, and it is consistent with Helsinki
holding the best wait and bunching sub-scores in the headline (94.9 and 97.4).

[rev 2026-09-26] The Q4 query now also requires a `gap_ratio`. Tokyo carries a scheduled
headway on every gap, so it had started returning a row reading 0.0% bunched and 0.0% big
gap — its deliberately NULL flags counted as "not bunched" by the `case when`. Gated on
`gap_ratio`, Tokyo drops out as the note below intends, and no other city moves a digit.

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

Toronto's median EWT collapsed from the provisional 372s to double digits (73s
[rev 2026-09-26]) once the grain-and-evidence rewrite weighted route-days properly and
more days closed — the earlier magnitude was exactly the partial-day artifact the docs
warned about. Helsinki's 21s median still leads everyone and still lines up with its
bunching rate.

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
| New York | 0.000% | 362,988 |
| Tokyo | 0.000% | 102,646 |

| City | Alerts | Alert-hours | Routes alerted |
|---|---:|---:|---:|
| New York | 64,706 | 1,496,369 | 9 |
| Toronto | 6,968 | 39,888 | 220 |
| Boston | 6,605 | 88,481 | 177 |
| SF Bay Area | 6,501 | 125,740 | 188 |
| Washington DC | 2,178 | 10,797 | 123 |
| Zurich | 2,065 | 42,303 | 661 |
| Helsinki | 1,789 | 29,193 | 171 |
| Tokyo | 210 | 4,429 | 15 |

A zero cancel rate for NYC and Tokyo means *their feeds never emit CANCELED*,
not that nothing was cancelled: all 13.4M NYC rows are SCHEDULED, and
`odpt:Train` has no cancellation field at all. **[rev 2026-09-12] Toronto was
listed here as a third never-emitting feed and that was wrong.** TTC does emit
CANCELED — 4 cancelled trips on 2 judged days, or 0.0005% of its 760,198 judged
scheduled trips (Q8) [rev 2026-09-26]. It rounds to 0.00% at two decimals, which
is why it read as a never-emitter; the site now renders any nonzero count below
a hundredth of a percent as `<0.01%` rather than as a bare zero. Three states
therefore exist in this column and must not be collapsed: feeds that cannot say
it, feeds that can and essentially never do, and feeds that say it and mean it.
Cancellation rate is a measure of feed behaviour as much as of service, and must
not be scored across cities without that caveat. NYC's alert profile is the opposite
extreme: 64,706 alerts and 1.5M alert-hours across only 9 routes [rev 2026-09-26] — the
MTA raises long-lived, line-level alerts, while MBTA and 511 raise many short route-level
ones. Tokyo's alerts rose with its final days to retirement (165 → 210) and its scheduled
trips with them (79,656 → 102,646); no other city's Q6 figure moved.

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

OTP by condition, every wet bucket plus SF's fog (SF has no rain cell at all; cells under
200 scored events suppressed). "vs its dry OTP" is against the city's Q7b dry share
(< 1 mm/h, all hours):

| City | Condition | Scored | OTP | vs its dry OTP |
|---|---|---:|---:|---:|
| Boston | rain | 219,813 | 60.1% | +4.2pp |
| Helsinki | rain | 1,403,787 | 81.4% | +0.1pp |
| New York | rain | 103,963 | 66.9% | −0.4pp |
| New York | heavy_rain | 7,692 | 70.2% | +2.9pp |
| SF Bay Area | fog | 1,758,298 | 59.8% | +0.9pp |
| Tokyo* | rain | 360,437 | 97.1% | −1.2pp |
| Tokyo* | heavy_rain | 10,654 | 99.1% | +0.8pp |
| Toronto | rain | 526,391 | 48.6% | −0.7pp |
| Toronto | heavy_rain | 46,717 | 34.3% | **−15.0pp** |
| Washington DC | rain | 31,331 | 55.9% | +1.6pp |
| Washington DC | heavy_rain | 2,288 | 57.7% | +3.4pp |
| Zurich | rain | 735,507 | 96.0% | −0.3pp |
| Zurich | extreme | 87,216 | 94.6% | −1.7pp |

[rev 2026-09-26] The table now lists every wet bucket rather than the first reading's six
cells (Tokyo's rows are new to this page); the six cells quoted before have not moved. The
counterintuitive early signal — NYC and Zurich running *better* in rain — did not survive
its denominator: on 104k and 736k rain events, both read a few tenths of a point *below*
their own dry OTP, which is exactly why `n_scored` rides on every cell. A few thousand
events from one rainy evening is an anecdote with a denominator, not a finding. Toronto's
heavy rain (−15.0pp) is the only large effect, and Boston's +4.2pp the largest wrong-way
one; both compare a few rainy hours with a dry average that includes 3am, which is why
Q10 re-asks the question against the dry share at the same hours of day. The
question this table was really waiting for (Helsinki-in-snow vs Boston-in-snow) needs
winter, and every poller retired in September: the machinery is wired and tested, but this
dataset ends before the first snow.

## Q8 — Data completeness

Only closed local days at or after each city's `metrics_from` are judged.

| City | Route-days judged | Mean completeness | Scheduled | Observed | Added | Cancelled |
|---|---:|---:|---:|---:|---:|---:|
| New York | 625 | 98.3% | 175,318 | 173,717 | 0 | 0 |
| Toronto | 4,698 | 95.9% | 760,198 | 724,618 | 11,353 | 4 |
| Helsinki | 8,595 | 93.1% | 480,431 | 448,668 | 0 | 168 |
| Washington DC | 2,647 | 92.6% | 315,226 | 283,944 | 502 | 11 |
| Boston | 3,714 | 91.2% | 335,670 | 324,226 | 23,542 | 7,409 |
| Zurich | 9,548 | 90.5% | 782,571 | 736,455 | 7,519 | 4,642 |
| SF Bay Area | 11,184 | 83.0% | 545,267 | 464,026 | 66 | 13,986 |
| Tokyo | 126 | 73.2% | 56,964 | 40,889 | 0 | 0 |

**P3's "completeness ≥85%" acceptance criterion: PASSED by six of the eight
cities.** SF Bay Area (83.0%) and Tokyo (73.2%) read below the bar — SF because
roughly a tenth of the operators behind the 511 feed publish a schedule and no
realtime at all (Q8c), Tokyo on 126 route-days, the thinnest denominator on the page.
**[rev 2026-09-20] This cut now skips retired days.** An agency keeps
publishing next-day trips after its poller stops, so the days nobody watched
arrive scheduled and unobserved: counting them read Helsinki at 80.7% against
its true 93.1%, and put three cities under the bar that had never been near it.
`retired_day` is the same flag both completeness tests and the scorecard use.

**[rev 2026-09-26] Tokyo fell from 80.6% (96 route-days) because of three national
holidays.** 2026-09-21..23 (Respect for the Aged Day, a bridging citizens' holiday, and the
autumnal equinox) are weekdays on which Toei runs its holiday timetable, but
`int_odpt_scheduled_trips` resolves day type from the weekday alone — the known gap its
own comment says "cannot bite inside this project's window", written when the window
closed on 09-17. Those days expected 2,818 weekday trips and matched ~740, so they read
~28% complete (80.7% across Tokyo's other 18 days; one route sits at 0% every day —
the Nippori-Toneri line, which publishes no realtime). Only 3 of their 18 route-days
clear the 0.50 floor, so the standings barely see them: Tokyo's judged on-time share is
98.05% with them and 97.96% without. Recorded, not repaired. The Added column is new to
this table: ADDED trips count as volume and never toward OTP.

---

**Q9–Q12 [added 2026-09-26]** are four cuts the standings cannot show, measured on the
standings' own judged route-days (closed, at or after `metrics_from`, not a `retired_day`,
completeness ≥ 0.50). Numbers match `site/data/extras.json` (as_of 2026-09-26 04:46 UTC).

## Q9 — How late is late

Percentiles of signed arrival delay, seconds, ordered by p90.

| City | Events | p50 | p75 | p90 | p95 | Mean |
|---|---:|---:|---:|---:|---:|---:|
| Tokyo* | 767,921 | 0 | 0 | 0 | 60 | 19 |
| Zurich | 10,149,137 | 66 | 114 | **178** | 233 | 82 |
| Helsinki | 12,602,244 | 39 | 120 | 242 | 349 | 67 |
| New York | 4,157,578 | 15 | 131 | 328 | 518 | 69 |
| Toronto | 21,190,663 | 0 | 152 | 418 | 701 | 67 |
| SF Bay Area | 15,467,269 | 66 | 237 | 490 | 761 | 167 |
| Washington DC | 9,058,287 | 93 | 283 | 565 | 840 | 175 |
| Boston | 7,908,279 | 141 | 346 | **648** | 922 | 203 |

Q1 says how often a vehicle is on time; this says how late a late one is. Zurich's worst
arrival in twenty is under four minutes behind (p95 233s), Boston's over fifteen (922s).
The top four by p90 are Q1's top four, but below them Toronto — last on on-time share —
has a shorter late tail than SF, DC or Boston, because most of its misses are early rather
than late (35.7% early, Q1). NYC's median reads 15s here against Q2's 4s because of the
judged-day gate, not the approximation — an exact median over the same events is also
15s; Tokyo's p95 of 60s is a single stated minute.

## Q10 — Rain

On-time share in hours with ≥ 0.5 mm of rain, against the same city's dry share
reweighted to the hours of day it rained — a 5pm shower is compared with dry 5pms, not
with a dry average that includes 3am. "Rainy hours" are distinct local hours at ≥ 0.5 mm
holding judged arrivals. Cities with under 5,000 rainy-hour arrivals are omitted, which
drops SF: it recorded no such hour in its window.

| City | Rainy hours | Rainy-hour arrivals | On time in rain | Dry, same hours | Difference |
|---|---:|---:|---:|---:|---:|
| Toronto | 6 | 248,640 | 42.9% | 49.3% | **−6.4 pts** |
| Washington DC | 3 | 33,613 | 56.0% | 58.2% | −2.2 pts |
| Tokyo* | 141 | 238,984 | 97.4% | 98.5% | −1.1 pts |
| Zurich | 12 | 312,600 | 95.9% | 97.0% | −1.1 pts |
| Helsinki | 21 | 638,698 | 80.2% | 81.2% | −1.0 pts |
| New York | 11 | 76,094 | 66.3% | 66.5% | −0.2 pts |
| Boston | 8 | 155,912 | 59.6% | 57.4% | +2.2 pts |

Held to the same hours of day, rain costs Tokyo, Helsinki and Zurich — 12 to 141 rainy
hours each — about a point (1.0–1.1), and New York 0.2. Toronto's −6.4, DC's −2.2 and
Boston's +2.2 rest on 3–8 rainy hours each — too few to call a pattern — and the hour
matching shrinks Q7's raw extremes (Toronto heavy rain −15.0pp, Boston rain +4.2pp) while
keeping their direction. Outside Tokyo, a late-summer window was short on rain.

## Q11 — Delay along the trip

Median arrival delay, seconds, by how far along its trip the vehicle is — first scored stop
= 0%, last = 100%, in tenths (five of the ten shown).

| City | 0–10% | 20–30% | 50–60% | 70–80% | 90–100% |
|---|---:|---:|---:|---:|---:|
| Tokyo* | 0 | 0 | 0 | 0 | 0 |
| Zurich | 54 | 60 | 66 | 72 | 66 |
| Helsinki | 12 | 36 | 46 | 53 | 40 |
| New York | 0 | 6 | 27 | 36 | 30 |
| SF Bay Area | 53 | 60 | 75 | 79 | 82 |
| Boston | 75 | 136 | 178 | 180 | 118 |
| Washington DC | 80 | 84 | 98 | 105 | 117 |
| Toronto | 28 | 11 | **−10** | −24 | **−33** |

Lateness builds along the trip almost everywhere — most in Boston, whose median more than
doubles from the first tenth (75s) to a 182s peak at 60–70%. **Toronto runs the other
way**: trips leave 28s late and finish 33s early, so its timetable allows more running time
than the vehicles use — the early running Q1 counts, accumulating stop by stop. Boston,
Helsinki, NYC and Zurich all give some back in the last tenth, the sign of recovery time
built into the run to the terminal; SF and DC are at their latest in the last tenth.
Tokyo's flat zero is the stated-minute quantization.

## Q12 — Route spread

Every route with at least 2,000 scored arrivals, its on-time share event-weighted across
judged days and directions. p10 and p90 bound the middle 80% of a city's routes (quantile =
sorted[floor(f·n)], the site's rule).

| City | Routes | p10 | Median route | p90 | Routes ≥ 80% on time |
|---|---:|---:|---:|---:|---:|
| Tokyo* | 4 | 93.6% | 99.7% | 99.8% | 100.0% (4) |
| Zurich | 380 | **92.6%** | 97.2% | 99.0% | **99.7% (379)** |
| Helsinki | 370 | 73.7% | 82.5% | 90.5% | 67.6% (250) |
| New York | 29 | 52.4% | 65.9% | 86.2% | 17.2% (5) |
| SF Bay Area | 381 | 47.9% | 63.2% | 77.2% | 6.3% (24) |
| Boston | 167 | 46.4% | 58.2% | 70.8% | 1.2% (2) |
| Washington DC | 132 | 47.0% | 55.0% | 64.6% | **0.0% (0)** |
| Toronto | 222 | 39.2% | 52.9% | 66.6% | 0.5% (1) |

A city's average does not say whether every line is decent or a few are excellent. Zurich
is uniformly good: its 10th-percentile route (92.6%) beats Helsinki's 90th (90.5%), and 379
of its 380 routes clear 80%. DC has no route at 80% (its best is 75.7%), Toronto one of
222 and Boston two of 167 — network-wide problems, not a few bad lines. New York is the
spread outlier, 29 lines from the D at 50.5% to a shuttle at 99.0%; SF's floor is its
Powell–Hyde cable car at 9.7%. Tokyo's four routes are its four Toei subway lines.

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
| Tokyo | tram 0 (Arakawa) | 1 | 0 | 0 | 1 |
| Tokyo | metro 1 (Toei subway) | 4 | 0 | 0 | 4 |
| Tokyo | **rail 2 (Nippori-Toneri)** | 1 | 0 | **1** | **0** |
| Tokyo | bus 3 (Toei bus) | 150 | 150 | 0 | 0 |
| Toronto | tram 0 (streetcar) | 17 | 0 | 0 | 17 |
| Toronto | bus 3 | 212 | 0 | 5 | 207 |
| Toronto | ferry 4 | 1 | 0 | 0 | 1 |
| Zurich | S-Bahn 109 | 62 | 12 | 0 | 50 |
| Zurich | bus 700 | 440 | 38 | 34 | 368 |
| Zurich | express bus 702 | 1 | 0 | 1 | 0 |
| Zurich | night bus 705 | 72 | 0 | 0 | 72 |
| Zurich | demand-responsive bus 715 | 3 | 0 | 1 | 2 |
| Zurich | tram 900 | 22 | 0 | 1 | 21 |
| Zurich | boat 1000 | 9 | 0 | 9 | 0 |
| Zurich | **aerial lift 1300** | 1 | 0 | **1** | **0** |
| Zurich | **funicular 1400** | 3 | 0 | **3** | **0** |

**DC is complete** — every scheduled route is observed; Toronto is five bus
routes short of it. Zurich's "scheduled, never seen" counts have collapsed as
days accrued — S-Bahn to zero, bus from 294 to 34 — which is what a coverage
read maturing actually looks like. [rev 2026-09-26] The table now carries every row the
query returns, so Tokyo and Zurich's smaller route types appear: Zurich's night buses
(705) are fully observed, while its aerial lift and three funiculars — the modes only its
feed carries — were scheduled and never once seen in realtime. Tokyo's 150 bus routes read
"never scheduled" because Tokyo's delivery denominator is built from `odpt:TrainTimetable`,
which covers Toei's subway, tram and Nippori-Toneri only; Toei bus is positions-only and
shows up as volume (Q3, 140 routes at peak), never in delivery. Its one unobserved rail route is the Nippori-Toneri line, which
publishes no realtime.

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
| **opentransportdata.swiss** (Zurich) | National `/la/gtfs-rt`, keyed | Whole Swiss network — filtered to 615 Zurich routes: S-Bahn, tram, bus, boat, aerial lift, funicular | Bus, tram, S-Bahn observed (442/21/50 routes, bus incl. 72 night lines [rev 2026-09-26]); boats, funiculars and the aerial lift never | Live 2026-08-25, retired from 2026-09-16. National rail classes (TGV/ICE/IC/IR/RE/EXT) are excluded from the allow-list on purpose; S-Bahn (`route_type` 109) is kept. No VehiclePositions product on the Swiss LA API — trip updates only, like Helsinki. |
| **CTA** (Chicago) | GTFS-RT, keyed | 'L' rail + bus | **Nothing** | Key issued, beta activation still returns `errCd 101`. |
| **ODPT** (Tokyo) | `odpt:Train` JSON + ToeiBus GTFS-RT (VP only) | Toei subway (4 lines, stated delay) + Arakawa tram (position only) + Toei bus (positions only) | Live 2026-09-03, retired from 2026-09-25: 4 subway lines scored, Arakawa tram volume-only, Toei bus positions. NipporiToneri publishes no realtime at all. | Key active. Center serves **no Metro odpt:Train** — Metro is line-status text only, so Tokyo OTP rests on Toei rail. Rail GTFS-RT is alerts-only, so the JSON API is the path. **`odpt:delay` is minute-quantized** (see caveat below). |
| **Open-Meteo** | Hourly weather JSON | n/a | Joined into gold | [rev 2026-09-26] `fct_weather_hourly`, joined to stop events on (city, local date, local hour); drives Q7 and Q10. The old "collected, unused" entry predated 2026-08-26. |

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
has to say so. Two rules followed: the tram (`Toei.Arakawa`) publishes position but
never a delay, so its 2,710 events count as service volume and are excluded from
OTP — the denominators use `count(delay_arr_sec)` — and that rule stands; and any
chart or sentence that ranked Tokyo alongside the others carried the
operator-stated, minute-rounded caveat inline, not in a footnote. **[rev 2026-09-26]
The second rule now binds this doc only.** By the owner's decision the public page
shows Tokyo's numbers without a marker; `delay_operator_stated` still rides in the
site export, and every Tokyo row on this page keeps its asterisk and this note.

Two feeds carry a mode nobody else does: 511's **cable car** and Zurich's
**funicular/aerial lift** (Zurich's in the schedule only — never observed, Q8b). Two modes are systematically missing everywhere they
exist: **ferries** (agencies publish them outside GTFS-RT) and **Toronto's
subway** (TTC publishes no realtime for it).
