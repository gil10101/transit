"""Feed poller: fetch every endpoint each cycle, archive raw bytes, emit envelopes.

Usage: python -m ingestion.poller <city>
Poll floor is 30s regardless of config — never hammer agency endpoints.
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

from ingestion.adapters import gtfs_rt
from ingestion.adapters.base import (
    CityConfig,
    KafkaEmitter,
    RawArchiver,
    build_session,
    fetch_feed,
    load_city_config,
    utcnow,
)


def poll_endpoint(session, cfg: CityConfig, archiver: RawArchiver, name: str, url: str):
    fetched_at = utcnow()
    raw = fetch_feed(session, cfg, url)
    archiver.archive(cfg.city, name, fetched_at, raw)
    envelopes = gtfs_rt.envelopes_for_feed(
        city=cfg.city, agency=cfg.agency, endpoint=name, raw=raw, fetched_at=fetched_at
    )
    return name, len(raw), envelopes


def run_cycle(session, cfg, emitter, archiver, pool) -> None:
    futures = {
        pool.submit(poll_endpoint, session, cfg, archiver, name, url): name
        for name, url in cfg.endpoints.items()
    }
    counts = {"trip_updates": 0, "vehicle_positions": 0, "alerts": 0}
    total_bytes, failures = 0, []
    for fut, name in futures.items():
        try:
            _, nbytes, envelopes = fut.result()
            total_bytes += nbytes
            for env in envelopes:
                counts[env["feed"]] += len(env["payload"])
                emitter.emit(env)
        except Exception as exc:  # keep polling other endpoints on a single-feed failure
            failures.append(f"{name}: {exc}")
    emitter.flush()
    status = (
        f"tu={counts['trip_updates']} vp={counts['vehicle_positions']} "
        f"al={counts['alerts']} bytes={total_bytes}"
    )
    if failures:
        status += f" FAILURES={failures}"
    print(f"{utcnow():%H:%M:%S} {cfg.city} {status}", flush=True)


def main() -> None:
    load_dotenv()
    city = sys.argv[1] if len(sys.argv) > 1 else "nyc"
    cfg = load_city_config(city)
    if cfg.adapter != "gtfs_rt":
        raise SystemExit(f"adapter {cfg.adapter} not implemented yet")
    session = build_session(cfg)
    emitter = KafkaEmitter()
    emitter.ensure_topics()
    archiver = RawArchiver()
    interval = cfg.effective_poll_seconds
    print(f"polling {city}: {len(cfg.endpoints)} endpoints every {interval}s", flush=True)
    with ThreadPoolExecutor(max_workers=len(cfg.endpoints)) as pool:
        while True:
            started = time.monotonic()
            run_cycle(session, cfg, emitter, archiver, pool)
            time.sleep(max(0.0, interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
