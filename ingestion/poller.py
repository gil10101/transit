"""Feed poller: fetch every endpoint each cycle, archive raw bytes, emit envelopes.

Usage: python -m ingestion.poller <city>
Poll floor is 30s regardless of config — never hammer agency endpoints.
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

from ingestion.adapters import ADAPTERS
from ingestion.adapters.base import (
    TOPICS,
    CityConfig,
    KafkaEmitter,
    RawArchiver,
    build_session,
    fetch_feed,
    load_city_config,
    utcnow,
)


def poll_endpoint(session, cfg: CityConfig, archiver: RawArchiver, name: str, url: str):
    adapter = ADAPTERS[cfg.adapter_for(name)]
    fetched_at = utcnow()
    raw = fetch_feed(session, cfg, url, endpoint=name)
    archiver.archive(cfg.city, name, fetched_at, raw, ext=adapter.EXTENSION)
    envelopes = adapter.envelopes_for_feed(
        city=cfg.city, agency=cfg.agency, endpoint=name, raw=raw, fetched_at=fetched_at
    )
    return name, len(raw), envelopes


def run_cycle(session, cfg, emitter, archiver, pool, due=None) -> None:
    """Poll the endpoints in `due` (default: all of them) once, concurrently."""
    names = list(cfg.endpoints) if due is None else list(due)
    futures = {
        pool.submit(poll_endpoint, session, cfg, archiver, name, cfg.endpoints[name]): name
        for name in names
    }
    counts = dict.fromkeys(TOPICS, 0)
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
        f"al={counts['alerts']} od={counts['odpt_trains']} bytes={total_bytes}"
    )
    if len(names) < len(cfg.endpoints):
        status += f" polled={','.join(sorted(names))}"
    if failures:
        status += f" FAILURES={failures}"
    print(f"{utcnow():%H:%M:%S} {cfg.city} {status}", flush=True)


def main() -> None:
    load_dotenv()
    city = sys.argv[1] if len(sys.argv) > 1 else "nyc"
    cfg = load_city_config(city)
    unknown = {a for a in (cfg.adapter_for(name) for name in cfg.endpoints) if a not in ADAPTERS}
    if unknown:
        raise SystemExit(f"unregistered adapter(s) for {city}: {sorted(unknown)}")
    session = build_session(cfg)
    emitter = KafkaEmitter()
    emitter.ensure_topics()
    archiver = RawArchiver()
    # Each endpoint keeps its own cadence (feed_groups.<name>.poll_seconds), so a
    # feed whose payload barely changes is not re-fetched at the city's rate. The
    # loop ticks at the fastest of them and polls only what is due; when every
    # endpoint shares one interval this is exactly the old behaviour.
    intervals = {name: cfg.poll_seconds_for(name) for name in cfg.endpoints}
    tick = min(intervals.values())
    plan = ", ".join(f"{n} every {s}s" for n, s in sorted(intervals.items()))
    print(f"polling {city}: {plan}", flush=True)
    next_due = dict.fromkeys(cfg.endpoints, 0.0)
    with ThreadPoolExecutor(max_workers=len(cfg.endpoints)) as pool:
        while True:
            started = time.monotonic()
            due = [name for name, at in next_due.items() if started >= at]
            if due:
                run_cycle(session, cfg, emitter, archiver, pool, due)
                for name in due:
                    next_due[name] = started + intervals[name]
            time.sleep(max(0.0, tick - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
