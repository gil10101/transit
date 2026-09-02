"""Tokyo ODPT JSON adapter: odpt:Train + odpt:TrainInformation -> canonical envelopes.

The retrieval API returns a JSON-LD array (docs/01 §C). odpt:Train objects are
TRAIN-grain position/delay snapshots (no stop_time_updates); they ride the dedicated
`odpt_trains` feed into silver.odpt_trains, and dbt explodes them to stop grain
against odpt:TrainTimetable. odpt:TrainInformation objects are per-line status text
and are flattened into the generic alert record shape so they ride the existing
alerts topic/stream unchanged.

Field semantics verified against developer.odpt.org/documents (2026-09-01):
- owl:sameAs is `odpt.Train:Operator.Line.TrainNumber` — a stable train identity.
  Its tail becomes `trip_id`, so the canonical trip_uid COALESCE in silver applies
  unchanged and equijoins TrainTimetable's `Operator.Line.TrainNumber` prefix in dbt.
- `odpt:toStation` null nominally means "stopped at fromStation", but the spec warns
  it may be non-null when the moving/stopped state is unknown — arrival detection is
  transition-based downstream, never a toStation-null test (docs/01 §C.1).
- Multilingual fields ({ja, en} objects) flatten here (en, else ja) because the
  alert record schema carries plain strings; full fidelity stays in the raw archive.
- `odpt:trainInformationStatus` is OMITTED during normal operation, so only objects
  carrying a status become alert records ("service normal" is not an alert).
"""

from __future__ import annotations

import json
from datetime import datetime

from ingestion.adapters.base import build_envelope

SOURCE_FORMAT = "odpt_json"
EXTENSION = "json"

_TRAIN_TYPE = "odpt:Train"
_INFO_TYPE = "odpt:TrainInformation"


def _tail(urn: str | None) -> str | None:
    """`odpt.Train:TokyoMetro.Ginza.B1045S` -> `TokyoMetro.Ginza.B1045S`."""
    if not urn:
        return None
    return urn.split(":", 1)[-1] or None


def _epoch(iso_ts: str | None) -> int | None:
    """ISO8601 with offset (`2026-09-01T11:02:15+09:00`) -> epoch seconds."""
    if not iso_ts:
        return None
    try:
        return int(datetime.fromisoformat(iso_ts).timestamp())
    except ValueError:
        return None


def _lang(obj) -> str | None:
    """Flatten a multilingual {ja, en} object to one string (en preferred)."""
    if not obj:
        return None
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get("en") or obj.get("ja") or next(iter(obj.values()), None)
    return None


def train_record(obj: dict) -> dict:
    return {
        "entity_id": obj.get("@id"),
        "trip_id": _tail(obj.get("owl:sameAs")),
        "train_number": obj.get("odpt:trainNumber"),
        "operator": _tail(obj.get("odpt:operator")),
        "railway": obj.get("odpt:railway"),
        "rail_direction": obj.get("odpt:railDirection"),
        "train_type": obj.get("odpt:trainType"),
        "delay_sec": obj.get("odpt:delay"),
        "from_station": obj.get("odpt:fromStation"),
        "to_station": obj.get("odpt:toStation"),
        "origin_stations": obj.get("odpt:originStation") or [],
        "destination_stations": obj.get("odpt:destinationStation") or [],
        "car_composition": obj.get("odpt:carComposition"),
        "train_index": obj.get("odpt:index"),
        "timestamp": _epoch(obj.get("dc:date")),
        "valid_until": _epoch(obj.get("dct:valid")),
        "frequency_sec": obj.get("odpt:frequency"),
    }


def train_information_record(obj: dict) -> dict:
    """odpt:TrainInformation -> the generic alert record shape (gtfs_rt.alert_record
    keys exactly, so silver's ALERT_RECORD parses it like any other alert)."""
    railway = obj.get("odpt:railway")
    return {
        "alert_id": obj.get("owl:sameAs"),
        "active_periods": [
            {
                "start": _epoch(obj.get("odpt:timeOfOrigin")),
                "end": _epoch(obj.get("odpt:resumeEstimate")),
            }
        ],
        "informed_entities": [
            {
                "agency_id": _tail(obj.get("odpt:operator")),
                "route_id": railway,
                "route_type": None,
                "stop_id": None,
                "trip_id": None,
            }
        ],
        "cause": _lang(obj.get("odpt:trainInformationCause")),
        "effect": None,
        "severity_level": None,
        "header_text": _lang(obj.get("odpt:trainInformationStatus")),
        "description_text": _lang(obj.get("odpt:trainInformationText")),
    }


def split_feed(objects: list[dict]) -> dict[str, list[dict]]:
    """Split a JSON-LD array into canonical records per feed type. TrainInformation
    without a status (= normal operation) is dropped — not an alert."""
    out: dict[str, list[dict]] = {"odpt_trains": [], "alerts": []}
    for obj in objects:
        rdf_type = obj.get("@type")
        if rdf_type == _TRAIN_TYPE:
            out["odpt_trains"].append(train_record(obj))
        elif rdf_type == _INFO_TYPE and obj.get("odpt:trainInformationStatus"):
            out["alerts"].append(train_information_record(obj))
    return out


def envelopes_for_feed(
    *,
    city: str,
    agency: str,
    endpoint: str,
    raw: bytes,
    fetched_at: datetime,
) -> list[dict]:
    """Decode one fetched JSON-LD array and build one envelope per non-empty feed type.

    An empty array is the API's documented no-match response -> no envelopes.
    feed_ts is the newest dc:date in the response (the API has no header timestamp).
    """
    objects = json.loads(raw)
    if not isinstance(objects, list):
        raise ValueError(f"expected JSON-LD array from {endpoint}, got {type(objects).__name__}")
    split = split_feed(objects)
    feed_ts = max((ts for ts in (_epoch(o.get("dc:date")) for o in objects) if ts), default=None)
    return [
        build_envelope(
            city=city,
            agency=agency,
            feed=feed_type,
            fetched_at=fetched_at,
            source_format=SOURCE_FORMAT,
            payload=records,
            endpoint=endpoint,
            feed_ts=feed_ts,
        )
        for feed_type, records in split.items()
        if records
    ]
