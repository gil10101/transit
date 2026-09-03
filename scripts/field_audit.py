"""Per-source, per-hop field audit: does every hop still carry what the business
question needs?

Hops: SOURCE (recorded fixtures) -> RAW (latest archived bytes on S3) ->
SILVER (Snowflake external Iceberg) -> STATIC (schedule tables) -> GOLD (marts).
Each check maps to a metric: delay fields -> OTP; arrival predictions + static
join -> headways/EWT/bunching; scheduled-vs-observed -> completeness; occupancy
-> crowding; alert effect/severity -> disruption context; route_type -> mode
split; local-time fields -> cross-city comparability.

Usage:
    uv run python scripts/field_audit.py                 # all hops, all cities
    uv run python scripts/field_audit.py source raw      # subset of hops
Read-only everywhere; one small query per city per check (XS warehouse, pruned
partitions). Never prints secrets.
"""

from __future__ import annotations

import os
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))  # scripts/ invocation: make `ingestion` importable
FIXTURES = REPO / "tests" / "fixtures"
RAW_BUCKET = "transit-pulse-622221238588-raw"
REGION = "us-east-2"
# GTFS-RT cities only: this audit decodes protobuf fixtures field by field.
# Tokyo is deliberately absent — its rail source is odpt:Train JSON, so it needs
# a separate decode path rather than a name in this tuple (docs/01 §C).
CITIES = ("nyc", "boston", "toronto", "helsinki", "dc", "sf", "zurich")


def pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "—"


# ---------------------------------------------------------------------------
# SOURCE hop: decode recorded fixtures, count field presence
# ---------------------------------------------------------------------------


def decode(blob: bytes):
    # the adapter's parser, not raw ParseFromString: it carries the Swiss OTD
    # non-spec TripDescriptor.7 remap the SA feed needs
    from ingestion.adapters.gtfs_rt import parse_feed

    return parse_feed(blob)


def audit_feed_message(msg) -> dict:
    tu = stu = delay = atime = seq = tid = sdate = 0
    vp = latlon = occ = 0
    al = effect = sev = period = 0
    rel: Counter = Counter()
    for ent in msg.entity:
        if ent.HasField("trip_update"):
            tu += 1
            t = ent.trip_update.trip
            tid += bool(t.trip_id)
            sdate += bool(t.start_date)
            rel[t.schedule_relationship] += 1
            for s in ent.trip_update.stop_time_update:
                stu += 1
                if s.HasField("arrival"):
                    delay += s.arrival.HasField("delay")
                    atime += s.arrival.HasField("time")
                seq += s.HasField("stop_sequence")
        if ent.HasField("vehicle"):
            vp += 1
            latlon += ent.vehicle.HasField("position")
            occ += ent.vehicle.HasField("occupancy_status")
        if ent.HasField("alert"):
            al += 1
            a = ent.alert
            effect += a.HasField("effect")
            sev += a.HasField("severity_level")
            period += len(a.active_period) > 0
    return {
        "TU": tu,
        "trip_id": pct(tid, tu),
        "start_date": pct(sdate, tu),
        "sched_rel": dict(rel) or "—",
        "STU": stu,
        "arr.delay": pct(delay, stu),
        "arr.time": pct(atime, stu),
        "stop_seq": pct(seq, stu),
        "VP": vp,
        "pos": pct(latlon, vp),
        "occupancy": pct(occ, vp),
        "alerts": al,
        "effect": pct(effect, al),
        "severity": pct(sev, al),
        "active_period": pct(period, al),
    }


def hop_source() -> None:
    print("\n== SOURCE (fixtures) — feeds the delay/OTP, EWT, crowding, alert metrics ==")
    for pb in sorted(FIXTURES.glob("*.pb")):
        try:
            stats = audit_feed_message(decode(pb.read_bytes()))
        except Exception as e:  # noqa: BLE001 — report and keep auditing
            print(f"{pb.name}: DECODE FAIL {e!r}")
            continue
        print(f"{pb.name}: {stats}")
    missing = [c for c in CITIES if not list(FIXTURES.glob(f"{c}_*.pb")) and c != "nyc"]
    if missing:
        print(f"no fixtures yet: {', '.join(missing)}")


# ---------------------------------------------------------------------------
# RAW hop: newest archived object per city, decoded the same way
# ---------------------------------------------------------------------------


def hop_raw() -> None:
    import boto3

    print("\n== RAW (latest S3 archive per city) — replay layer must match source ==")
    s3 = boto3.client("s3", region_name=REGION)
    for city in CITIES:
        feeds = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f"{city}/", Delimiter="/")
        prefixes = [p["Prefix"] for p in feeds.get("CommonPrefixes", [])]
        if not prefixes:
            print(f"{city}: no raw prefix (poller not deployed)")
            continue
        # sample a trip-update feed when the city has one: it carries the delay,
        # stop and schedule fields the metrics are built on. Alphabetical order
        # would pick "alerts", whose stats say nothing about OTP or headways.
        chosen = [p for p in prefixes if "trip_update" in p or "trips" in p] or prefixes
        for prefix in chosen[:1]:
            newest = None
            pages = s3.get_paginator("list_objects_v2").paginate(Bucket=RAW_BUCKET, Prefix=prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    if newest is None or obj["LastModified"] > newest["LastModified"]:
                        newest = obj
            if newest is None:
                print(f"{city}: prefix {prefix} empty")
                continue
            blob = s3.get_object(Bucket=RAW_BUCKET, Key=newest["Key"])["Body"].read()
            feed_name = newest["Key"].rsplit("/", 2)[-2]
            try:
                stats = audit_feed_message(decode(blob))
                print(f"{city} {feed_name} @ {newest['LastModified']:%H:%MZ}: {stats}")
            except Exception as e:  # noqa: BLE001
                print(f"{city} {newest['Key']}: DECODE FAIL {e!r}")


# ---------------------------------------------------------------------------
# Warehouse hops
# ---------------------------------------------------------------------------


def sf_connect():
    import snowflake.connector

    return snowflake.connector.connect(
        account="wqteqyy-ib47757",
        user=os.environ.get("SNOWFLAKE_USER", "TERRAFORM_SVC"),
        private_key_file=os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            str(Path.home() / ".snowflake" / "keys" / "transit_terraform_key.p8"),
        ),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse="TRANSFORM_XS",
        database="TRANSIT",
        schema="SILVER",
        session_parameters={"TIMEZONE": "UTC"},
    )


def q(cur, sql: str):
    return cur.execute(sql).fetchall()


def hop_silver(cur) -> None:
    print("\n== SILVER (predictions) — every metric's raw material ==")
    print(
        "city | rows(2d) | fresh_utc | null trip_uid/svc_date/route/stop/seq | arr_pred | dup_grain"
    )
    for row in q(
        cur,
        """
        select city, count(*),
               max(fetched_at),
               sum(iff(trip_uid is null,1,0)), sum(iff(service_date is null,1,0)),
               sum(iff(route_id is null,1,0)), sum(iff(stop_id is null,1,0)),
               sum(iff(stop_sequence is null,1,0)),
               sum(iff(arr_pred_ts_utc is null,1,0)),
               count(*) - count(distinct city||'|'||service_date||'|'||trip_uid
                    ||'|'||coalesce(stop_id,'')||'|'||coalesce(stop_sequence,-1)
                    ||'|'||fetched_at)
        from silver.stop_time_predictions
        where service_date >= dateadd(day,-2,current_date)
        group by city order by city
    """,
    ):
        c, n, fresh, *nulls, dup = row
        key_nulls = "/".join(pct(x, n) for x in nulls[:5])
        print(f"{c} | {n:,} | {fresh:%m-%d %H:%M} | {key_nulls} | null {pct(nulls[5], n)} | {dup}")


def hop_static(cur) -> None:
    print("\n== STATIC (schedule) — matcher + completeness + mode split need this ==")
    has_tp = (
        q(
            cur,
            """
        select count(*) from transit.information_schema.columns
        where table_schema='SILVER' and table_name='GTFS_STATIC_STOP_TIMES'
          and column_name='TIMEPOINT'
    """,
        )[0][0]
        > 0
    )
    tp_expr = (
        "(select 100*avg(iff(s.timepoint is not null,1,0))"
        " from silver.gtfs_static_stop_times s"
        " where s.city=v.city and s.gtfs_version_id=v.vid)"
        if has_tp
        else "-1"
    )
    if not has_tp:
        print("timepoint column ABSENT — pre-canonical schema; reparse not landed yet")
    print("city | version | trips | stop_times | timepoint% | routes | svc_ids_active_today")
    for row in q(
        cur,
        f"""
        with v as (
            select city, max(gtfs_version_id) vid
            from silver.gtfs_static_trips group by 1
        )
        select v.city, v.vid,
               (select count(*) from silver.gtfs_static_trips t
                 where t.city=v.city and t.gtfs_version_id=v.vid),
               (select count(*) from silver.gtfs_static_stop_times s
                 where s.city=v.city and s.gtfs_version_id=v.vid),
               {tp_expr},
               (select count(*) from silver.gtfs_static_routes r
                 where r.city=v.city and r.gtfs_version_id=v.vid),
               (select count(distinct c.service_id) from silver.gtfs_static_calendar c
                 where c.city=v.city and c.gtfs_version_id=v.vid
                   and to_date(c.start_date,'YYYYMMDD') <= current_date
                   and to_date(c.end_date,'YYYYMMDD') >= current_date)
        from v order by 1
    """,
    ):
        c, vid, trips, st, tp, routes, svc = row
        tp_s = "n/a" if tp == -1 else f"{tp:.0f}%"
        print(f"{c} | {vid} | {trips:,} | {st:,} | {tp_s} | {routes} | {svc}")


def hop_gold(cur) -> None:
    print("\n== GOLD (marts) — the business question itself ==")
    print(
        "fct_stop_events by city (2d): rows | sched-matched% | conf dist"
        " | delay p50/p95 s | on_time% | early_dep"
    )
    for row in q(
        cur,
        """
        select city_key, count(*),
               100*avg(iff(sched_arr_ts_utc is not null,1,0)),
               listagg(distinct match_confidence, ','),
               median(delay_arr_sec), percentile_cont(0.95) within group (order by delay_arr_sec),
               -- [rev 2026-08-25] count(otp_band), NOT count(*). This read
               -- 100*avg(iff(otp_band='on_time',1,0)), and IFF(NULL='on_time',1,0)
               -- returns 0 — so every unscored row (skipped stop, stale prediction,
               -- unmatched trip) stayed in the DENOMINATOR. That contradicts docs/05 §7
               -- ("NULL removes the row from both the numerator AND the denominator")
               -- and it under-reported OTP against the canonical
               -- fct_route_reliability_daily by up to 4.3 points: nyc 64.6 vs 68.9,
               -- boston 55.6 vs 59.0, sf 56.7 vs 58.9, toronto 48.1 vs 49.7.
               -- This is the tool used to sign off a new city, so it was making the
               -- pipeline look worse than it is.
               100.0*count_if(otp_band = 'on_time')/nullif(count(otp_band),0),
               sum(iff(early_departure_flag,1,0))
        from gold.fct_stop_events
        where service_date >= dateadd(day,-2,current_date)
        group by 1 order by 1
    """,
    ):
        c, n, m, conf, p50, p95, otp, early = row
        print(f"{c} | {n:,} | {m:.0f}% | [{conf}] | {p50}/{p95} | {otp:.1f}% | {early}")
    print("reliability daily (yesterday): city | routes | avg_otp% | avg_ewt_s | bunching%")
    for row in q(
        cur,
        """
        select city_key, count(distinct route_id), round(100*avg(otp_pct),1),
               round(avg(ewt_sec)), round(100*avg(bunching_pct),1)
        from gold.fct_route_reliability_daily
        where service_date = dateadd(day,-1,current_date)
        group by 1 order by 1
    """,
    ):
        print(" | ".join(str(x) for x in row))
    print("service delivery (yesterday): city | scheduled | observed | completeness%")
    for row in q(
        cur,
        """
        select city_key, sum(trips_scheduled), sum(trips_observed),
               round(100*avg(completeness_pct),1)
        from gold.fct_service_delivery_daily
        where service_date = dateadd(day,-1,current_date)
        group by 1 order by 1
    """,
    ):
        print(" | ".join(str(x) for x in row))


def main() -> None:
    hops = sys.argv[1:] or ["source", "raw", "silver", "static", "gold"]
    if "source" in hops:
        hop_source()
    if "raw" in hops:
        hop_raw()
    warehouse = [h for h in hops if h in ("silver", "static", "gold")]
    if warehouse:
        conn = sf_connect()
        cur = conn.cursor()
        try:
            if "silver" in hops:
                hop_silver(cur)
            if "static" in hops:
                hop_static(cur)
            if "gold" in hops:
                hop_gold(cur)
        finally:
            conn.close()


if __name__ == "__main__":
    main()
