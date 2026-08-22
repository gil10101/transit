"""Phase 1 proof: OTP% and median delay by route by local hour for NYC, plus
10 sample finalized stop events for manual spot-checking against a live tracker.

Run `make dbt-build` first; reads the duckdb warehouse built by dbt.
Usage: uv run python scripts/proof_nyc.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

DB = Path(__file__).resolve().parent.parent / "dbt" / "transit" / "transit.duckdb"


def main() -> None:
    con = duckdb.connect(str(DB), read_only=True)
    # stg_gtfs__* are views over iceberg_scan -> this connection needs the same
    # settings the dbt profile uses (secret, tz, version-hint race fallback)
    con.execute("LOAD httpfs; LOAD iceberg; LOAD icu;")
    con.execute("SET TimeZone='UTC'; SET unsafe_enable_version_guessing=true;")
    con.execute("""
        CREATE SECRET IF NOT EXISTS minio (TYPE s3, KEY_ID 'minioadmin',
        SECRET 'minioadmin', ENDPOINT 'localhost:9000', USE_SSL false, URL_STYLE 'path');
    """)

    span = con.execute(
        """
        select count(*), min(actual_arr_ts_utc), max(actual_arr_ts_utc),
               count(distinct trip_uid), count(distinct route_id)
        from fct_stop_events where city_key = 'nyc'
        """
    ).fetchone()
    print(f"finalized events: {span[0]}  trips: {span[3]}  routes: {span[4]}")
    print(f"event span (UTC): {span[1]} .. {span[2]}\n")

    print("=== OTP% and median delay by route x local hour (nyc) ===")
    rows = con.execute(
        """
        select route_id,
               local_hour,
               count(*)                                                as events,
               round(100.0 * count(*) filter (otp_band = 'on_time') /
                     count(*) filter (otp_band is not null), 1)        as otp_pct,
               round(100.0 * count(*) filter (otp_band = 'early') /
                     count(*) filter (otp_band is not null), 1)        as early_pct,
               cast(median(delay_arr_sec) as int)                      as median_delay_s,
               cast(quantile_cont(delay_arr_sec, 0.90) as int)         as p90_delay_s
        from fct_stop_events
        where city_key = 'nyc' and otp_band is not null
        group by 1, 2
        having count(*) >= 20
        order by 1, 2
        """
    ).fetchall()
    fmt = "{:>5} {:>3} {:>7} {:>6} {:>7} {:>6} {:>6}"
    print(fmt.format("route", "hr", "events", "otp%", "early%", "med_s", "p90_s"))
    for r in rows:
        print(fmt.format(*r))

    print("\n=== route rollup ===")
    for r in con.execute(
        """
        select route_id, count(*) events,
               round(100.0 * count(*) filter (otp_band = 'on_time') / count(*), 1) otp_pct,
               cast(median(delay_arr_sec) as int) median_delay_s,
               round(avg(prediction_count), 1) avg_preds_per_event,
               round(avg(match_confidence), 2) avg_match_conf
        from fct_stop_events
        where city_key = 'nyc' and otp_band is not null
        group by 1 order by 1
        """
    ).fetchall():
        print(r)

    print("\n=== 10 sample finalized stop events (spot-check vs live tracker) ===")
    samples = con.execute(
        """
        select f.trip_id_raw, f.route_id,
               case f.direction_id when 0 then 'N' when 1 then 'S' end dir,
               s.stop_name, f.stop_id, f.stop_sequence,
               strftime(timezone('America/New_York', cast(f.sched_arr_ts_utc as timestamptz)),
                        '%H:%M:%S') sched_local,
               strftime(timezone('America/New_York', cast(f.actual_arr_ts_utc as timestamptz)),
                        '%H:%M:%S') actual_local,
               f.delay_arr_sec, f.otp_band, f.match_confidence, f.prediction_count
        from fct_stop_events f
        left join stg_gtfs__stops s on s.city_key = f.city_key and s.stop_id = f.stop_id
        where f.city_key = 'nyc' and f.delay_arr_sec is not null
          and f.prediction_count >= 3
        using sample 10
        """
    ).fetchall()
    if not samples:  # thin data: relax the revised-prediction filter
        samples = con.execute(
            """
            select f.trip_id_raw, f.route_id,
                   case f.direction_id when 0 then 'N' when 1 then 'S' end dir,
                   s.stop_name, f.stop_id, f.stop_sequence,
                   strftime(timezone('America/New_York',
                            cast(f.sched_arr_ts_utc as timestamptz)), '%H:%M:%S') sched_local,
                   strftime(timezone('America/New_York',
                            cast(f.actual_arr_ts_utc as timestamptz)), '%H:%M:%S') actual_local,
                   f.delay_arr_sec, f.otp_band, f.match_confidence, f.prediction_count
            from fct_stop_events f
            left join stg_gtfs__stops s on s.city_key = f.city_key and s.stop_id = f.stop_id
            where f.city_key = 'nyc' and f.delay_arr_sec is not null
            using sample 10
            """
        ).fetchall()
    for r in samples:
        print(
            f"trip {r[0]:<22} rt {r[1]:>2}{r[2]} stop {r[3]} [{r[4]} seq {r[5]}] "
            f"sched {r[6]} actual {r[7]} delay {r[8]:+d}s {r[9]} "
            f"(conf {r[10]}, {r[11]} preds)"
        )


if __name__ == "__main__":
    main()
