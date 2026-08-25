# 07 — Transit Pulse, explained in plain English

This is the long version. `docs/05-pipeline-walkthrough.md` gets a new engineer
productive in an afternoon; this document is for actually understanding the thing —
why each piece exists, what every table means, which numbers you can trust, and where
the bodies are buried.

No jargon goes unexplained. If you know what a "watermark" or a "grain" is, skim past
those bits.

Written 2026-08-25. Where a number appears, it came from the live warehouse that day.

---

## 1. What we are trying to find out

**Which cities run the most reliable public transit — and what makes them reliable?**

That sounds simple and isn't, because "reliable" means different things depending on
how often the service runs.

If a train comes every 20 minutes, you look up the timetable and you care whether it
arrives when it said it would. That is **punctuality** — on-time performance.

If a bus comes every 4 minutes, nobody reads a timetable. You turn up and wait. What
you care about is **how long you actually waited**, and whether the gaps between buses
are even. Three buses arriving together after a 15-minute gap is a disaster for riders
even though all three might be "on time" against some schedule.

So a fair comparison needs both, plus a few things people forget:

- A bus that leaves **early** is a failure, not a success. If it departs a timed stop
  60 seconds ahead of schedule, the rider who arrived on time watched it drive away.
- A trip that **never ran at all** is the worst delay possible, and it shows up in no
  punctuality statistic — because there is no arrival to be late.
- If your data feed is missing half a city's buses, your "on-time percentage" is a
  measurement of your feed, not of the city. So **how complete our own data is** has to
  be a first-class metric, not a footnote.

Everything in this system exists to answer those, city by city, in a way that lets you
put Helsinki and Toronto on the same chart without lying.

### The rule that makes cross-city comparison honest

Everything is stored in UTC and **analysed in local time**. When we say "the 8am peak",
we mean 8am where the rider is. 3am in Tokyo compares to 3am in New York, not to
whatever o'clock that is in Greenwich. Every city carries its own timezone
(`dim_city.iana_tz`) and every analysis converts through it.

This sounds obvious and is the single easiest thing to get wrong. It has already
caused one real bug in this project — see §14.

---

## 2. The whole system in one paragraph

Small programs called **pollers** ask each transit agency's live API "where is
everything right now?" every 30–200 seconds. The raw answer is saved to cloud storage
untouched, then published onto a queue. A **Spark** job drains that queue every couple
of hours, cleans the records up, throws away duplicates, and writes them into
structured tables on S3. Those tables are exposed to **Snowflake**, a data warehouse,
where **dbt** transforms them: it matches each live vehicle report to the trip it was
supposed to be, compares actual times against the printed schedule, and rolls
everything up into a handful of tables you can chart. **Dagster** runs all of this on a
timer and shouts if a feed dies.

---

## 3. Words you need

**GTFS** — the standard format transit agencies publish their *schedule* in. A zip of
CSV files: routes, trips, stops, stop_times, calendar. Nearly every agency on earth
uses it.

**GTFS-RT (Realtime)** — the live companion. A binary "protobuf" blob, refreshed every
few seconds, containing three kinds of message:
- **TripUpdate** — "trip X will reach stop Y at 14:32". This is where delay comes from.
- **VehiclePosition** — "vehicle 4471 is at this lat/lon". Useful for counting what is
  moving.
- **Alert** — "the Red Line is suspended". Free text plus affected routes.

**Protobuf** — a compact binary encoding. You cannot read it with your eyes; you need
the schema to decode it. That is why we save the raw bytes *and* the decoded version.

**Trip** — one scheduled run of a vehicle from one end of a route to the other. The
09:14 from Alewife is a different trip from the 09:26.

**Stop time** — one scheduled call at one stop on one trip. A 40-stop bus route
generates 40 stop times per trip. This is the finest grain in the schedule, and it is
why the schedule tables are the biggest in the warehouse (125 million rows).

**Grain** — what one row means. Getting this wrong is how you accidentally count things
twice. Our core fact table's grain is "one vehicle arriving at one stop on one trip on
one day".

**Bronze / silver / gold** — a common way to organise a warehouse:
- **bronze** = exactly what arrived, decoded but otherwise untouched. An archive.
- **silver** = cleaned, typed, deduplicated, one consistent shape across all cities.
- **gold** = the business answers. Joined, aggregated, ready to chart.

**Idempotent** — safe to run twice. Everything scheduled here is idempotent, because
retries are normal and double-counting is not.

**Watermark** — in stream processing, how far back you keep remembering. We hold two
hours, so if the same prediction arrives twice within two hours we recognise the
duplicate. Past two hours we forget, so memory stays bounded.

---

## 4. Where the data comes from

Nine cities were planned. Six are live and scored, one just came online, two are
blocked on other people.

### New York — MTA
Eight separate feeds, one per group of subway lines (ACE, BDFM, G, JZ, NQRW, L, 1–7,
Staten Island). No API key needed. Three things make NYC awkward:

1. **The feed does not tell us the delay.** Seven of the eight feeds omit
   `arrival.delay` entirely. Only the L train sets it. So for NYC we compute delay
   ourselves by comparing the predicted arrival to the printed schedule.
2. **Trip IDs encode the origin time**, like `070950_A..S58R` — that is the 07:09:50
   departure of an A train. They do not match the schedule's trip IDs directly, so NYC
   gets its own matching logic.
3. **Vehicle positions have no coordinates.** The subway feed reports position relative
   to a stop, not a lat/lon. You cannot map NYC vehicles.

Buses are deliberately out of scope. Subway only.

### Boston — MBTA
The cleanest feed of the lot. Keyless, all three message types, and its realtime trip
IDs match its schedule trip IDs exactly. Covers the Green Line, subway, commuter rail,
buses and ferries.

A quirk worth knowing: MBTA's schedule lists 368 bus routes but **215 of them never
run** — they are variants kept in the file. Counting static routes makes Boston look
like it is missing 40% of its buses. It isn't; real coverage is 151 of 153.

### Toronto — TTC
Keyless, attribution required. Two things:

1. **The subway publishes alerts only** — no positions, no predictions. Toronto is
   therefore scored on surface modes (streetcars and buses) and that is stated wherever
   a Toronto number appears. It is a scoping decision, not a gap we can close.
2. **TTC publishes two different schedule files** and we spent a day reading the wrong
   one. The one that pairs with the realtime feed is `SurfaceGTFS.zip`. Against the
   other file, only 0.8% of stops matched and Toronto looked unfixable. Against the
   right one, trip IDs match 100% and stops 99.6%. See §14 — this is the most
   instructive mistake in the project's history.

About 3% of Toronto trips are **ADDED** — extra runs not in the schedule, with negative
IDs. They count as service delivered but are excluded from punctuality, because there
is no scheduled time to be punctual against.

### Helsinki — HSL
Keyless. Three quirks, all real:

1. **The trip ID field is empty.** Every realtime record. So trips are matched on
   (route, direction, service date, start time) instead.
2. **There is no VehiclePositions feed at all.** Only trip updates and alerts. Helsinki
   cannot answer "how many vehicles are moving" — it is absent from that table entirely.
3. **`arrival.delay` is not set either** (this was mis-recorded once and corrected —
   0 of 8,021 arrivals carried it). Delay is computed against the schedule, same as NYC.

Helsinki is currently the most reliable city in the data by every measure.

### Washington DC — WMATA
Needs a free key. Six endpoints: rail and bus, each with trip updates, positions and
alerts. Ships **two separate schedule zips** (rail and bus) which we deliberately land
under a single version ID so both survive the "latest version only" filter.

DC and Toronto are the only two cities where every scheduled route is observed.

### San Francisco Bay Area — 511.org
One regional feed aggregating roughly 30 operators — AC Transit, Muni, Caltrain, BART,
SamTrans, VTA and many small ones. Needs a key, and is rate-limited to 60 requests an
hour, so it polls every 200 seconds instead of 30.

**This is one "city" that is really thirty agencies**, and they do not all publish
realtime. Six small operators file a schedule and no live data whatsoever. The big ones
run 84–100% coverage. Any SF number is therefore an average of a well-instrumented core
and a handful of blind operators, and should be quoted that way.

SF is also the only feed carrying **cable cars**.

### Zurich — opentransportdata.swiss
Went live 2026-08-25. The hardest one, for a reason that is not technical:

**The feed is national.** `/la/gtfs-rt` returns every operator in Switzerland — TGVs to
Paris, ICEs to Germany, postbuses in the Alps. If you ingest it as-is you are measuring
Swiss punctuality and calling it Zurich.

So there is an **allow-list**: 615 routes that actually serve canton Zürich, generated
from the national schedule by two rules — the majority of a route's stops fall inside a
box around the canton, *and* the route is not a national rail class. That second rule
had to be added after the first went live: Swiss route IDs are segment-granular, so an
ICE route covering only Zürich HB → Zürich Flughafen scores 100% "inside the box". 51
long-distance routes sneaked through that way. The S-Bahn (`route_type` 109) is
deliberately kept — it is the backbone of Zurich rail.

Anything not on the allow-list is dropped in silver. If the allow-list file goes
missing, Zurich rows are dropped entirely rather than let through unfiltered — **fail
closed**, because silently ingesting all of Switzerland is worse than ingesting nothing.

Zurich also has **no VehiclePositions product**, and it needs two different API tokens,
one per endpoint.

### Chicago — CTA (blocked)
Key issued, beta activation never completed; the endpoint still returns `errCd 101`.
Nothing we can do from here.

### Tokyo — ODPT (not started)
Needs a developer key we have not applied for. Its GTFS-RT is alerts-only, so the path
is their `odpt:Train` JSON API, where the operator states the delay directly rather than
us computing it.

### Weather — Open-Meteo
Collected hourly for every city since P5. **Nothing reads it yet.** It is banking
against the question "does Zurich handle snow better than New York", which needs about
two years of history before it means anything.

### What each source can never give us

| Missing | Where | Why |
|---|---|---|
| Ferries | Boston (9 routes), Helsinki (3), SF (8 of 14) | Agencies publish ferry realtime outside GTFS-RT. Helsinki's is confirmed: it comes from ship AIS transponders piped into a separate MQTT stream. |
| Subway | Toronto | TTC publishes alerts only for it. |
| Vehicle positions | Helsinki, Zurich | No such product in their feed. |
| Vehicle identity | NYC subway | Feed does not identify vehicles, so "how many trains are running" is a floor, not a count. |
| Buses | New York | Scope decision, not a gap. |

---

## 5. Following one bus arrival all the way through

Concrete beats abstract. Here is one record's life.

**1. A bus exists.** Route 501 streetcar in Toronto is running its 14:22 trip. TTC's
system knows it will reach Spadina & King at about 14:31.

**2. We ask.** The Toronto poller wakes up (every 30 seconds), sends one HTTPS request,
gets back ~1 MB of protobuf describing every TTC surface vehicle.

**3. We keep the evidence.** Those exact bytes are written to
`s3://…-raw/toronto/trip_updates/2026-08-25/14/20260825T142930Z.pb`. Untouched. If we
later discover our decoder was wrong, we can replay from here.

**4. We decode and publish.** The blob is parsed into individual records. Our streetcar's
arrival becomes one message on a Kafka topic, wrapped in a common envelope: which city,
which agency, which endpoint, when we fetched it, plus the payload.

**5. Spark drains the queue.** Every two hours a Spark job reads everything new. It
writes the envelopes into the bronze archive, and separately normalises them into
silver.

**6. Normalising is where the work happens.** Our record gets:
   - a **service date** — which "transit day" it belongs to (§8)
   - a **trip_uid** — a stable fingerprint identifying this trip on this day (§8)
   - **deduplication** — that same 14:31 prediction arrived in ~20 consecutive polls.
     Spark keeps one.
   - **city filtering** — for Zurich only, non-allow-listed routes are dropped here.

   It lands in `silver.stop_time_predictions` as one row: city, service date, trip_uid,
   stop, sequence number, predicted arrival time, and what the schedule said.

**7. Snowflake is told to look again.** Silver lives on S3 in Iceberg format. Snowflake
reads it as an external table, but is pinned to a specific metadata file, so after each
drain we explicitly refresh it.

**8. dbt cleans it up.** `stg_gtfsrt__trip_updates` types the columns and attaches the
city's timezone.

**9. dbt matches the trip to the schedule.** Toronto's realtime trip ID joins the
`SurfaceGTFS` schedule exactly. Now we know our streetcar was *supposed* to reach
Spadina & King at 14:28:30.

**10. dbt picks a winner.** We saw ~20 predictions for this arrival. Which is the truth?
The last one before the bus actually got there. That becomes the observed arrival.

**11. Delay is computed.** 14:31:10 actual against 14:28:30 scheduled = **+160 seconds**.
That falls in the "on time" band (anything from 60 seconds early to 299 seconds late).

**12. It becomes a row in `fct_stop_events`** — the atomic fact. One row per arrival, with
delay, band, local hour, whether it was peak, and how confident the trip match was.

**13. It gets counted.** That row feeds the route's daily punctuality, the gap between it
and the previous streetcar (for bunching), and the count of trips actually delivered
versus scheduled.

---

## 6. Each stage, in more detail

### 6.1 Polling
One small Python program per city, all running the same image, configured entirely by a
YAML file in `ingestion/config/cities/`. Adding a city is meant to be a YAML file plus
at most one quirk, not new code.

Each poller loops: fetch every endpoint, save the raw bytes, decode, publish to Kafka,
sleep until the next tick. Endpoints are fetched concurrently, and **one failing feed
never stops the others** — a 503 on alerts must not take down trip updates.

There is a hard floor of 30 seconds between polls of the same endpoint. We are guests on
these APIs.

**Per-endpoint cadence.** Since 2026-08-25 an individual feed can poll slower than its
city. This exists because Zurich's alerts endpoint returns a 10 MB payload that is
*byte-identical* on every poll — the same file, 2,880 times a day, 29 GB of storage for
zero information. Zurich now polls trip updates every 60s and alerts every 600s. The
30-second floor applies per endpoint, so an override can only ever slow a feed down.

There is a ceiling too: the health check (below) fails if a feed's newest file is more
than 40 minutes old, so no cadence may approach that.

### 6.2 The raw archive
Every response is stored exactly as received, partitioned by city, endpoint, date and
hour. This is the replay layer: every downstream mistake is recoverable as long as the
raw bytes survive. It currently holds about 27 GB.

It has **no expiry policy**, so it grows forever. That is a decision waiting to be made,
not an oversight — see §13.

### 6.3 Kafka
A buffer between "the agency answered" and "we processed it". It means the pollers never
block on the warehouse, and a Spark failure loses nothing — the messages wait.

One single-broker instance on a small EC2 box. Not highly available, deliberately: if it
dies we lose buffered messages but not the raw archive, and the cost of a proper cluster
is not justified at this scale.

### 6.4 Spark: bronze and silver
One job on EMR Serverless starts **four concurrent streaming queries**: bronze envelopes,
silver stop-time predictions, silver vehicle positions, silver alerts.

It runs with `Trigger.AvailableNow`, which means "process everything currently in the
queue, then exit". It is not a permanently running stream. This matters:

- We pay only for the minutes it runs.
- It always leaves one cycle's tail behind, because it snapshots the queue position at
  start. Being slightly behind is normal and not a bug.
- **Only one drain may run at a time.** All drains share one checkpoint, and Structured
  Streaming assumes a single writer. The scheduler skips if one is already in flight.

**Deduplication** uses a two-hour watermark. The same prediction arriving twice within
two hours is recognised and dropped. This state is the memory-hungry part of the job.

**Sizing is tight and has bitten us.** The EMR application has a 32 GB ceiling, and each
worker carries about 10% overhead. A 6 GB driver plus two 12 GB executors comes to
roughly 33 GB — over the limit — so the second executor is silently refused and the lone
survivor gets killed by the OOM killer. That is exactly what happened on 2026-08-24 and
it cost five hours of stalled ingestion.

### 6.5 The schedule side
Once a week, each city's GTFS zip is downloaded and parsed into `silver.gtfs_static_*`.
Each load gets a version ID; downstream models filter to the newest per city.

Schedules matter more than people expect. Half the delay numbers in this project are
computed against them, and the Toronto episode was entirely a schedule problem.

### 6.6 Snowflake registration
Silver tables live on S3 in Iceberg format. Snowflake reads them as external tables
without copying data — but each table is pinned to a specific metadata file, so after
every drain we run an explicit refresh. Skip that and the warehouse silently serves
stale data while everything looks green.

### 6.7 dbt: staging → intermediate → marts

**Staging** is thin: type the columns, attach the timezone, rename to house style. One
staging model per silver table. No business logic.

**Intermediate** is where the difficulty lives:

- `int_service_dates` — resolves which schedule patterns run on which dates, including
  holiday exceptions.
- `int_gtfs_scheduled_stop_times` — expands the schedule into real timestamps for each
  service date. 10 million rows.
- `int_trip_matching_nyc` — NYC's origin-time-encoded IDs.
- `int_trip_matching_generic` — everyone else. Boston, DC, SF, Zurich and Toronto join
  on trip ID exactly; Helsinki resolves on (route, direction, start time) because its
  trip ID is empty. There is also a "fuzzy" origin-time branch, currently used by no
  city — Toronto was the last one and moved to exact matching when its schedule was
  fixed. It is kept wired for the next city with that shape.
- `int_stop_events_finalized` — picks the winning prediction per arrival and computes
  delay.
- `int_service_frequency` — median scheduled gap per route/direction/time-of-day, which
  decides whether a route counts as "frequent" and therefore gets an excess-wait number.

**Marts** are the answers — see §7.

### 6.8 Orchestration
Dagster runs four schedules:
- **Every 2 hours**: drain → refresh Snowflake → dbt build → sanity checks.
- **Every 15 minutes**: the killed-feed tripwire. It lists S3 and fails if any polled
  city's endpoint has produced nothing in 40 minutes. It never touches the warehouse, so
  it is nearly free.
- **Weekly**: schedule refresh.
- **Hourly**: weather.

---

## 7. Every table, in plain English

### The schedule (silver)
| Table | Rows | What it is |
|---|---:|---|
| `gtfs_static_stop_times` | 125.4M | Every scheduled call at every stop. The biggest table we have. |
| `gtfs_static_calendar_dates` | 21.3M | Per-date exceptions — holidays, added and cancelled service. |
| `gtfs_static_shapes` | 7.1M | Route geometry for maps. **Nothing reads it yet.** |
| `gtfs_static_trips` | 6.0M | Every scheduled trip. |
| `gtfs_static_stops` | 328.9K | Stop names and coordinates. Zurich's allow-list is built from these. |
| `gtfs_static_calendar` | 175.0K | Which days of the week each service pattern runs. |
| `gtfs_static_routes` | 14.1K | Routes, and critically `route_type` — the mode. |

### The live data (silver)
| Table | Rows | What it is |
|---|---:|---|
| `stop_time_predictions` | 99.4M | Every arrival/departure prediction we have ever seen, deduplicated. The workhorse. |
| `vehicle_positions` | 8.2M | Where vehicles were. Empty for Helsinki and Zurich. |
| `alerts` | 17.5K | Disruption notices. |
| `weather_hourly` | 1.8K | Hourly weather per city. **Nothing reads it yet.** |

### The answers (gold)
| Table | Rows | What it answers |
|---|---:|---|
| `fct_stop_events` | 3.45M | **The atomic fact.** One row per vehicle arrival at a stop: delay, on-time band, local hour, peak flag, match confidence. Everything else is an aggregate of this. |
| `fct_headways` | 3.23M | The gap between consecutive vehicles at the same stop, with bunching and big-gap flags. |
| `fct_route_reliability_daily` | 3.9K | Per route per day: punctuality, delay percentiles, excess wait, bunching, cancellations. |
| `fct_service_delivery_daily` | 4.4K | Scheduled vs actually observed trips per route-day — the completeness metric. |
| `fct_vehicle_activity_hourly` | 431 | How many vehicles, trips and routes were moving, per city per mode per hour. |
| `fct_alerts_daily` | 687 | Alert count, total active minutes and worst effect per route-day. |
| `dim_city` | 7 | The small table that governs everything: timezone, peak windows, when scoring starts, whether the schedule can be matched, attribution text. |

`dim_city` is worth dwelling on. It is seven rows and it decides whether a city's numbers
exist at all. `metrics_from` blank means "do not score this city yet".
`schedule_matchable` false means "we cannot trust this city's schedule join, so null out
every schedule-derived column". Both have caused silent, invisible failures.

---

## 8. The rules that must be obeyed

These are written once and used everywhere. Reimplementing one is how the numbers start
disagreeing with each other.

### Service date — the "transit day"
A transit day is not a calendar day. The 00:40 night bus belongs to the previous day's
service, and schedules genuinely contain times like `25:30:00` meaning 1:30am tomorrow.

The rule: use the feed's own `start_date` when it provides one. When it does not, take
the local timestamp, subtract 12 hours, and use the date part. That puts everything
before noon into the previous service day, which is the GTFS convention.

**Toronto is an exception.** Its feed sets `start_date` on exactly zero trips, and −12h
misdates the entire midnight-to-noon half of its day. Toronto uses a −4h cutover instead.

### trip_uid
A stable fingerprint for "this trip, on this day":

```
hash(city, service_date, COALESCE(trip_id, route_id || '-' || direction_id || '-' || start_time))
```

The `COALESCE` is there for Helsinki, whose trip ID is empty.

### Delay
```
delay = COALESCE(feed's own arrival.delay, predicted arrival − scheduled arrival)
```
Most cities have no feed-supplied delay, so the second half does the work. Tokyo will be
different when it lands: the operator states the delay and that is authoritative.

Positive means late. Negative means early, and early is tracked separately rather than
being folded into "not late".

### The uniqueness rule
One row per `(city_key, service_date, trip_uid, stop_sequence)`. This is tested, and it
is the guard against double-counting.

### Added trips
Count toward service volume, never toward punctuality. There is no scheduled time for an
unscheduled trip to be late against.

---

## 9. What we filter out, and why

Every filter is a decision about what the number means. These are the ones that matter:

1. **Non-Zurich Swiss routes** — dropped in silver against a 615-route allow-list.
   Without it we would be measuring Switzerland.
2. **National rail classes within Zurich** — TGV, ICE, EC, IC, IR, night trains and
   charters, even when they pass the geographic test.
3. **Duplicate predictions** — the same arrival seen in 20 consecutive polls collapses
   to one.
4. **Stale predictions** — if the winning prediction was last seen more than 60 minutes
   before the event it describes, it is a schedule echo rather than an observation. It
   still counts as service volume but scores no punctuality band.
5. **Added trips** — volume yes, punctuality no.
6. **Cancelled and skipped stops** — no arrival happened, so no punctuality band.
7. **Events with no stop ID** — Helsinki emits a few hundred; they break the
   headway table's contract.
8. **Old schedule versions** — only the newest load per city is used.
9. **Unmatched trips** — flow through with a null schedule, so they count as volume but
   score nothing. Except Helsinki, where a missing match also means a missing stop
   sequence, so those drop out entirely.
10. **In-progress service days** — a day is only judged once it has actually finished in
    that city's own local time.
11. **Route-days with fewer than 10 scheduled trips** — excluded from the completeness
    test, because on a 4-trip route one missing bus is a 25% "failure" and that is noise,
    not signal.

---

## 10. How we know the numbers are right

**Tests.** About 120 dbt assertions run on every build: uniqueness on the atomic fact's
grain, not-null on every key, ranges on delay and headway, and the completeness floors.
Two are custom checks that a rule actually held — for example, that a skipped stop never
carries a punctuality band.

**Sampling at every hop.** The standing rule on this project: whenever data moves between
stages, check actual field values on both sides. A green job is not evidence. Every city
onboarded here was verified by decoding raw protobuf, then checking the same records in
silver, then in gold.

That rule exists because of the Toronto episode. Every job was green. Every test passed.
The data was wrong, and only a field-level comparison against the agency's own file found
it.

**What the tests currently say:** roughly 118 pass, 4 warn, 1 error. The error is the
completeness floor on 15 Helsinki route-days — 11 of which are routes with genuinely zero
realtime coverage (ferries, and one tram line). It is a real finding, deliberately left
red rather than silenced.

---

## 11. What the numbers currently say

From 2–3 days of data. **This is not enough to rank cities** and should not be presented
as one. It is enough to show the pipeline produces sane, comparable measurements.

| City | On time | Early | Median delay | Bunched | Median excess wait |
|---|---:|---:|---:|---:|---:|
| Helsinki | 81.3% | 13.3% | +31s | 2.6% | 22s |
| New York | 69.9% | 19.3% | +14s | 7.7% | 29s |
| Boston | 59.0% | 17.5% | +112s | 10.7% | 58s |
| SF Bay | 58.9% | 23.3% | +52s | 7.7% | 55s |
| Washington DC | 53.8% | 24.0% | +81s | 9.4% | 76s |
| Toronto | 49.7% | 41.0% | −23s | 11.7% | 372s |

Three things to understand before quoting any of it:

**Toronto's story is early, not late.** It runs *ahead of schedule* four times in ten.
For buses that is a genuine service failure, and it is why the early band is tracked
separately. It is also high enough that the schedule's timing points deserve checking
before this goes in a scorecard.

**Use medians, not means.** New York's mean delay is −98 seconds against a median of +14.
That is not a fast subway; it is a long negative tail from trips matched to a schedule
that ran at a different time. Boston's mean and median agree, which is what a
well-behaved city looks like.

**Two of these numbers measure the feed, not the service.** Toronto and NYC both report a
0.000% cancellation rate because their feeds never emit a cancellation. And NYC logs
1,441 alerts across just 10 routes because the MTA raises long-lived line-level alerts,
where Boston and SF raise many short route-level ones. Neither is comparable across
cities without saying so.

---

## 12. What is missing, plainly

**Ferries, nearly everywhere.** Boston has 9 scheduled ferry routes and we observe none.
Helsinki 3, none. SF 8 of 14 unobserved. Agencies publish ferry realtime outside GTFS-RT.
Helsinki's is confirmed — it comes from ship transponders via a separate stream. The
others have the same shape and have not been separately confirmed.

**One Helsinki tram line (100H)** — 187 trips a day, 69 stops, running 04:00 to 02:00, and
it has never produced a single realtime row while its sibling variants all report
normally. This needs an email to HSL, not a code change.

**Six small SF operators** publish schedules and no realtime at all.

**Toronto's subway** and **New York's buses** — scope decisions, not defects.

**Weather** is collected and unused. **Route geometry** is loaded and unused. **Stop
details** are staged and unused.

**Four of six cities have never had their completeness judged**, because judging requires
a finished local service day at or after that city's start date, and Boston, Toronto, DC
and SF have not reached one yet.

**The scorecard itself does not exist.** The composite 0–100 city score is the next phase.
Every input to it is built; the ranking is not.

---

## 13. What it costs

Real numbers from AWS Cost Explorer for 1–25 August: **$22.38 of usage, fully covered by
credits, $0.00 actually billed.**

The shape is more interesting than the total. S3 is 64% of it — and almost none of that
is storage. It is *requests*: 2.36 million write-class and 6.4 million read-class calls
against roughly $0.008 of actual bytes. Spark's checkpointing generates it, rewriting
state files across 200 partitions on every micro-batch of four concurrent queries.

The alerts stream is the clearest example: 3,267 checkpoint objects holding 1.9 MB. More
objects than the prediction stream, for a thousandth of the data.

Two things are unresolved:

1. **The raw archive has no expiry.** It grows forever, for every city. It is also the
   replay layer, which is the whole point of keeping it — so an expiry policy is a real
   tradeoff, not an obvious win.
2. **EMR debug logs have no expiry either.** 3.4 GB in three days, and nobody reads them
   after the week they were produced. This one is close to free money.

The next efficiency lever, if cost moves, is reducing Spark's shuffle partitions from the
default 200.

---

## 14. The traps that have already cost hours

Read this section before debugging anything.

**Treat a coverage gap as our bug until the agency's docs prove otherwise.** Toronto sat
at 0% for a day because we read the wrong one of two schedule files TTC publishes. Every
job was green. The tell was that realtime stop IDs matched the other file's *stop codes*
at exactly 58.7% — a suspiciously specific number that meant "you are joining the wrong
column", not "the agency's data is bad".

**A reboot does not re-land configuration.** Cloud-init runs once per instance. Rebooting
the box — or even stopping and starting it — leaves the old configuration on disk. It has
to be `cloud-init clean --logs && reboot`. The services box silently ran a configuration
7 hours older than Terraform believed, and nothing surfaced it.

**dbt seeds need `--full-refresh` when you add a column.** A new column in a seed CSV is
not picked up otherwise. A per-city gate was therefore never actually live in production,
while looking correct in the code.

**Rebuilding only the marts leaves the intermediates stale.** Two cities showed 0% scored
until a full rebuild. After a new city lands, rebuild the whole project, not a selection.

**`current_date` is UTC.** Judging "finished" service days against it meant New York's day
was being marked complete while it still had seven hours to run — 157 false failures every
night. Any "is this day over" test must go through the city's own timezone.

**EMR has a hard memory ceiling.** Driver plus executors plus ~10% overhead each must fit
under 32 GB, or the extra executor is refused with a message that does not mention memory,
and the survivor is OOM-killed. The fastest diagnostic is billed memory-GB-hours divided
by runtime: it tells you how many executors actually ran.

**Checkpoint offsets versus commits** is the definitive answer to "is the drain stuck". If
`offsets/N` exists and `commits/N` does not, a batch started and never finished.

**The Terraform Snowflake provider fails to configure** even though it is unused. Plans
and applies error on it; scoping with `-target` is the workaround. A plan file saved from
an errored run is incomplete and apply will refuse it.

---

## 15. Running it day to day

**Is it healthy?** Four things: the tripwire is green, the two-hourly chain succeeded, the
pollers are up, and gold row counts grew during service hours.

**A feed died.** The tripwire tells you within about 55 minutes. Check the poller's logs
first — usually it is the agency, not us.

**The drain is stuck.** Compare checkpoint offsets against commits. If a batch started and
never finished, look for an out-of-memory kill.

**The numbers look wrong.** Investigate the data before touching a threshold. Loosening a
test requires amending the doc that justifies it — that rule exists because the alternative
is a warehouse where every test passes and nothing is true.

**Adding a city.** A YAML file, a `dim_city` row, a timezone entry, fixtures recorded, and
a matcher branch if its trip IDs are unusual. Then verify every hop by hand before
believing any number it produces.

---

## 16. What comes next

Nearest first:

1. Let the four unjudged cities accumulate a finished service day and check their
   completeness against the ≥85% target.
2. Watch Zurich's first full day, and confirm the allow-list holds as the network wakes up.
3. Decide the raw-archive retention question.
4. Chase Chicago's key and apply for Tokyo's.
5. Build the scorecard: slowly-changing dimensions, the weighted composite score, and the
   dashboard that lets a stranger answer "which city is most reliable at 8am?" in two
   clicks.

The honest status: the measurement machinery is built and verified. The ranking it exists
to produce is not built yet, and there is not enough history to publish one even if it
were.
