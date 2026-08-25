"""Generate the Zurich route allow-list from the parsed Swiss national static.

Zurich's realtime feed (`/la/gtfs-rt`) covers all of Switzerland. To answer
"how reliable is Zurich?" we keep only routes that actually serve the Zurich
area, and drop the rest before they reach silver (docs/01 §B; the filter lives
in spark_jobs/silver_normalize.national_filter).

Rule: a route is Zurich's when the MAJORITY of its stops lie inside the canton
box below. Majority rather than "any stop" on purpose — Geneva–St. Gallen
intercity trains call at Zürich HB but are national services, and counting them
would score Swiss long-distance punctuality as Zurich's. VBZ trams/buses and the
ZVV S-Bahn network sit far above the threshold; through-running IC/EC services
sit far below, so the exact cutoff is not delicate.

Writes both consumers of the list, which must never diverge:
  * dbt/transit/seeds/zurich_route_allowlist.csv  (checked in; docs/03 §5)
  * s3://<artifacts>/config/zurich_route_allowlist.csv  (read by the Spark job)

Usage: uv run python scripts/generate_zurich_allowlist.py [--dry-run]
Requires the Zurich static parsed into TRANSIT.SILVER (weekly gtfs_static run).
"""

from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SEED = REPO / "dbt" / "transit" / "seeds" / "zurich_route_allowlist.csv"
S3_KEY = "config/zurich_route_allowlist.csv"
CITY = "zurich"

# Canton Zürich bounding box (WGS84). Covers the ZVV service area — city trams
# and buses, the S-Bahn ring out to Winterthur/Uster/Wetzikon and the lake
# shores — without reaching Basel, Bern, Lucerne or St. Gallen.
BBOX = {"lat_min": 47.16, "lat_max": 47.71, "lon_min": 8.35, "lon_max": 8.99}
MIN_SHARE = 0.5

# National rail classes, dropped regardless of how the box rule scores them.
# [rev 2026-08-25] The box rule alone was NOT sufficient, contrary to this
# script's original assumption that through-running services "sit far below" the
# threshold. Measured against the live national static: 51 SBB long-distance
# routes (9,066 trips, 2.2% of the allow-list's trips) passed at zone_share
# 0.50-1.00, because Swiss route_ids are segment-granular — an ICE route_id
# covering only Zürich HB -> Zürich Flughafen scores 1.00. Scoring TGV, ICE,
# EC, RJX, IC, IR, NightJet and Extrazug punctuality as Zurich's would answer
# the wrong question (all are agency_id 11 = SBB long distance).
#   101 high-speed · 102 long-distance · 103 inter-regional · 105 sleeper
#   106 regional express (RE) · 117 additional/extra trains (EXT)
# 109 (suburban railway) is DELIBERATELY ABSENT: that is the ZVV S-Bahn, the
# backbone of Zurich rail, and it must stay. So must every non-rail type the
# rule admits — tram 900, bus 700/702/705/715, boat 1000, aerial 1300,
# funicular 1400.
EXCLUDED_ROUTE_TYPES = (101, 102, 103, 105, 106, 117)

QUERY = f"""
with latest as (
    select max(gtfs_version_id) as vid
    from silver.gtfs_static_trips where city = '{CITY}'
),
-- every CTE joins `latest` explicitly: mixing a comma-join with an ANSI JOIN
-- binds the JOIN to `latest` instead of the comma'd table, so `st.trip_id`
-- falls out of scope ("invalid identifier ST.TRIP_ID").
stops as (
    select s.stop_id,
           max(case when s.stop_lat between {BBOX["lat_min"]} and {BBOX["lat_max"]}
                     and s.stop_lon between {BBOX["lon_min"]} and {BBOX["lon_max"]}
                    then 1 else 0 end) as in_zone
    from silver.gtfs_static_stops s
    join latest l on s.gtfs_version_id = l.vid
    where s.city = '{CITY}'
    group by 1
),
trips as (
    select t.trip_id, t.route_id
    from silver.gtfs_static_trips t
    join latest l on t.gtfs_version_id = l.vid
    where t.city = '{CITY}'
),
route_stops as (
    select distinct t.route_id, st.stop_id
    from silver.gtfs_static_stop_times st
    join latest l on st.gtfs_version_id = l.vid
    join trips t on t.trip_id = st.trip_id
    where st.city = '{CITY}'
),
scored_stops as (
    select rs.route_id, rs.stop_id, s.in_zone
    from route_stops rs
    join stops s on s.stop_id = rs.stop_id
),
scored as (
    select route_id,
           count(*) as stops_total,
           sum(in_zone) as stops_in_zone,
           cast(sum(in_zone) as double) / nullif(count(*), 0) as zone_share
    from scored_stops group by 1
)
select s.route_id,
       coalesce(r.route_short_name, '') as route_short_name,
       coalesce(r.agency_id, '') as agency_id,
       r.route_type,
       s.stops_in_zone,
       s.stops_total,
       round(s.zone_share, 3) as zone_share
from scored s
left join silver.gtfs_static_routes r
       on r.city = '{CITY}' and r.route_id = s.route_id
      and r.gtfs_version_id = (select vid from latest)
where s.zone_share >= {MIN_SHARE}
  and coalesce(r.route_type, -1) not in ({",".join(str(t) for t in EXCLUDED_ROUTE_TYPES)})
order by s.route_id
"""

HEADER = [
    "route_id",
    "route_short_name",
    "agency_id",
    "route_type",
    "stops_in_zone",
    "stops_total",
    "zone_share",
]


def fetch_rows() -> list[tuple]:
    import snowflake.connector

    conn = snowflake.connector.connect(
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
    try:
        return conn.cursor().execute(QUERY).fetchall()
    finally:
        conn.close()


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    rows = fetch_rows()
    if not rows:
        raise SystemExit(
            "no routes matched — is the Zurich static parsed into "
            "TRANSIT.SILVER.gtfs_static_* yet? (weekly gtfs_static run)"
        )
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(HEADER)
    writer.writerows(rows)
    body = buf.getvalue()

    modes = {}
    for r in rows:
        modes[r[3]] = modes.get(r[3], 0) + 1
    print(f"{len(rows)} Zurich routes (route_type -> count): {dict(sorted(modes.items()))}")
    print("sample:", ", ".join(f"{r[1] or r[0]}" for r in rows[:12]))
    if dry_run:
        print("--dry-run: nothing written")
        return

    SEED.write_text(body)
    print(f"wrote {SEED.relative_to(REPO)}")
    bucket = os.environ.get("ARTIFACTS_BUCKET", "transit-pulse-622221238588-artifacts")
    import boto3

    boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2")).put_object(
        Bucket=bucket, Key=S3_KEY, Body=body.encode()
    )
    print(f"wrote s3://{bucket}/{S3_KEY} — silver picks it up on the next drain")


if __name__ == "__main__":
    main()
