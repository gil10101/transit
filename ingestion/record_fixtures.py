"""Record one live snapshot per feed endpoint into tests/fixtures/<city>_<endpoint>.pb.

The only sanctioned live fetch outside `make poll-<city>`. Unit tests decode these
fixtures exclusively.
"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

from ingestion.adapters import gtfs_rt
from ingestion.adapters.base import build_session, fetch_feed, load_city_config

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"


def main() -> None:
    load_dotenv()
    city = sys.argv[1] if len(sys.argv) > 1 else "nyc"
    cfg = load_city_config(city)
    session = build_session(cfg)
    for name, url in cfg.endpoints.items():
        raw = fetch_feed(session, cfg, url)
        path = FIXTURE_DIR / f"{city}_{name}.pb"
        path.write_bytes(raw)
        split = gtfs_rt.split_feed(gtfs_rt.parse_feed(raw))
        print(
            f"{path.name}: {len(raw)} bytes  tu={len(split['trip_updates'])} "
            f"vp={len(split['vehicle_positions'])} al={len(split['alerts'])}"
        )


if __name__ == "__main__":
    main()
