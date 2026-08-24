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

QUERY = f"""
with latest as (
    select max(gtfs_version_id) as vid
    from silver.gtfs_static_trips where city = '{CITY}'
),
stops as (
    select stop_id,
           case when stop_lat between {BBOX["lat_min"]} and {BBOX["lat_max"]}
                 and stop_lon between {BBOX["lon_min"]} and {BBOX["lon_max"]}
                then 1 else 0 end as in_zone
    from silver.gtfs_static_stops, latest
    where city = '{CITY}' and gtfs_version_id = latest.vid
),
route_stops as (
    select t.route_id,
           s.stop_id,
           max(s.in_zone) as in_zone
    from silver.gtfs_static_stop_times st, latest
    join silver.gtfs_static_trips t
      on t.city = '{CITY}' and t.gtfs_version_id = latest.vid and t.trip_id = st.trip_id
    join stops s on s.stop_id = st.stop_id
    where st.city = '{CITY}' and st.gtfs_version_id = latest.vid
    group by 1, 2
),
scored as (
    select route_id,
           count(*) as stops_total,
           sum(in_zone) as stops_in_zone,
           cast(sum(in_zone) as double) / nullif(count(*), 0) as zone_share
    from route_stops group by 1
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
