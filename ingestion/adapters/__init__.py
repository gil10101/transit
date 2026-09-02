"""Adapter registry: config `adapter:` values -> modules.

Every module here exposes the same surface: SOURCE_FORMAT, EXTENSION, and
envelopes_for_feed(city=, agency=, endpoint=, raw=, fetched_at=) -> list[dict].
"""

from ingestion.adapters import gtfs_rt, odpt

ADAPTERS = {
    "gtfs_rt": gtfs_rt,
    "odpt_json": odpt,
}
