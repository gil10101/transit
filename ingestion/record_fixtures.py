"""Record one live snapshot per feed endpoint into tests/fixtures/<city>_<endpoint>.<ext>.

Usage: python -m ingestion.record_fixtures [city ...]   (no args = every configured city)

The only sanctioned live fetch outside `make poll-<city>`. One snapshot per feed, no
loops. Unit tests decode these fixtures exclusively. Extension follows the endpoint's
adapter wire format (gtfs_rt -> .pb, odpt_json -> .json).
"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

from ingestion.adapters import ADAPTERS
from ingestion.adapters.base import CONFIG_DIR, build_session, fetch_feed, load_city_config, utcnow

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"


def record_city(city: str) -> None:
    cfg = load_city_config(city)
    session = build_session(cfg)
    for name, url in cfg.endpoints.items():
        adapter = ADAPTERS[cfg.adapter_for(name)]
        raw = fetch_feed(session, cfg, url, endpoint=name)
        path = FIXTURE_DIR / f"{city}_{name}.{adapter.EXTENSION}"
        path.write_bytes(raw)
        envelopes = adapter.envelopes_for_feed(
            city=cfg.city, agency=cfg.agency, endpoint=name, raw=raw, fetched_at=utcnow()
        )
        summary = " ".join(f"{env['feed']}={len(env['payload'])}" for env in envelopes) or "empty"
        print(f"{path.name}: {len(raw)} bytes  {summary}")


def main() -> None:
    load_dotenv()
    cities = sys.argv[1:] or sorted(p.stem for p in CONFIG_DIR.glob("*.yaml"))
    for city in cities:
        record_city(city)


if __name__ == "__main__":
    main()
