"""Generic GTFS-RT adapter: protobuf FeedMessage -> canonical envelopes.

Handles combined feeds (NYC: one endpoint carries TU+VP+Alert entities) and per-type
feeds alike — entities are split by type and emitted to the matching topic.
Record shapes follow docs/01-data-dictionary.md §A.2–A.4.
"""

from __future__ import annotations

from datetime import datetime

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from ingestion.adapters.base import build_envelope

SOURCE_FORMAT = "gtfs_rt"
EXTENSION = "pb"  # raw-archive object extension for this wire format


def parse_feed(raw: bytes) -> gtfs_realtime_pb2.FeedMessage:
    msg = gtfs_realtime_pb2.FeedMessage()
    try:
        msg.ParseFromString(raw)
    except DecodeError as err:
        # Swiss OTD gtfs-sa quirk (fixture 2026-08-23): TripDescriptor field 7 carries a
        # raw STRING journey id ("ch:1:sjyid:...") where the current spec defines the
        # ModifiedTripSelector MESSAGE -> strict decode fails for ~19% of alert entities.
        # Remap those tags to an unknown field (byte-length preserving) and retry; the
        # entities' real fields (effect/severity/active_period/informed routes) survive.
        try:
            remapped = _remap_nonspec_trip_field7(raw)
        except Exception:
            raise err from None  # remap walk failed -> the original error is the truth
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.ParseFromString(remapped)
    return msg


# --- non-spec TripDescriptor.7 remap (Swiss OTD service alerts) ----------------------
# Wire-format constants: tag = (field_number << 3) | wire_type; wire type 2 = bytes.
_TRIP_FIELD7_TAG = (7 << 3) | 2  # 0x3A — spec: ModifiedTripSelector; Swiss: raw string
_UNKNOWN_FIELD15_TAG = (15 << 3) | 2  # 0x7A — last single-byte tag, unallocated in spec


def _read_varint(buf, i: int) -> tuple[int, int]:
    shift = value = 0
    while True:
        byte = buf[i]
        i += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, i
        shift += 7


def _iter_fields(buf, start: int, end: int):
    """Yield (field_number, wire_type, tag_offset, payload_start, payload_end) over one
    embedded message's span; payload offsets are None for non-length-delimited fields."""
    i = start
    while i < end:
        tag_offset = i
        tag, i = _read_varint(buf, i)
        field_number, wire_type = tag >> 3, tag & 7
        if wire_type == 2:
            length, i = _read_varint(buf, i)
            yield field_number, wire_type, tag_offset, i, i + length
            i += length
        elif wire_type == 0:
            _, i = _read_varint(buf, i)
            yield field_number, wire_type, tag_offset, None, None
        elif wire_type == 5:
            yield field_number, wire_type, tag_offset, None, None
            i += 4
        elif wire_type == 1:
            yield field_number, wire_type, tag_offset, None, None
            i += 8
        else:
            raise ValueError(f"unsupported wire type {wire_type} at offset {tag_offset}")


def _remap_trip_descriptor(buf: bytearray, start: int, end: int) -> int:
    remapped = 0
    for field_number, wire_type, tag_offset, _, _ in _iter_fields(buf, start, end):
        if field_number == 7 and wire_type == 2 and buf[tag_offset] == _TRIP_FIELD7_TAG:
            buf[tag_offset] = _UNKNOWN_FIELD15_TAG
            remapped += 1
    return remapped


def _remap_trips_in(buf: bytearray, start: int, end: int, trip_field: int) -> int:
    remapped = 0
    for field_number, wire_type, _, ps, pe in _iter_fields(buf, start, end):
        if field_number == trip_field and wire_type == 2:
            remapped += _remap_trip_descriptor(buf, ps, pe)
    return remapped


def _remap_nonspec_trip_field7(raw: bytes) -> bytes:
    """Rewrite TripDescriptor field-7 tags to field 15 everywhere a TripDescriptor sits:
    entity.trip_update.trip / entity.vehicle.trip / entity.alert.informed_entity.trip.
    Tag bytes are swapped in place (same byte length) so no offsets shift."""
    buf = bytearray(raw)
    for field_number, wire_type, _, ps, pe in _iter_fields(buf, 0, len(buf)):
        if field_number != 2 or wire_type != 2:  # FeedMessage.entity
            continue
        for efn, ewt, _, es, ee in _iter_fields(buf, ps, pe):
            if ewt != 2:
                continue
            if efn in (3, 4):  # trip_update / vehicle -> .trip = 1
                _remap_trips_in(buf, es, ee, trip_field=1)
            elif efn == 5:  # alert -> informed_entity = 5 -> .trip = 4
                for afn, awt, _, as_, ae in _iter_fields(buf, es, ee):
                    if afn == 5 and awt == 2:
                        _remap_trips_in(buf, as_, ae, trip_field=4)
    return bytes(buf)


def _trip_fields(trip) -> dict:
    return {
        "trip_id": trip.trip_id or None,
        "route_id": trip.route_id or None,
        "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
        "start_date": trip.start_date or None,
        "start_time": trip.start_time or None,
        "schedule_relationship": trip.ScheduleRelationship.Name(trip.schedule_relationship),
    }


def _stop_time_event(event) -> dict:
    return {
        "time": event.time if event.HasField("time") else None,
        "delay": event.delay if event.HasField("delay") else None,
        "uncertainty": event.uncertainty if event.HasField("uncertainty") else None,
    }


def trip_update_record(entity) -> dict:
    tu = entity.trip_update
    rec = _trip_fields(tu.trip)
    rec.update(
        {
            "entity_id": entity.id,
            "vehicle_id": tu.vehicle.id or None,
            "vehicle_label": tu.vehicle.label or None,
            "timestamp": tu.timestamp if tu.HasField("timestamp") else None,
            "stop_time_updates": [
                {
                    "stop_id": stu.stop_id or None,
                    "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                    "arrival": _stop_time_event(stu.arrival) if stu.HasField("arrival") else None,
                    "departure": (
                        _stop_time_event(stu.departure) if stu.HasField("departure") else None
                    ),
                    "schedule_relationship": stu.ScheduleRelationship.Name(
                        stu.schedule_relationship
                    ),
                }
                for stu in tu.stop_time_update
            ],
        }
    )
    return rec


def vehicle_position_record(entity) -> dict:
    vp = entity.vehicle
    rec = _trip_fields(vp.trip)
    rec.update(
        {
            "entity_id": entity.id,
            "vehicle_id": vp.vehicle.id or None,
            "vehicle_label": vp.vehicle.label or None,
            "lat": vp.position.latitude if vp.HasField("position") else None,
            "lon": vp.position.longitude if vp.HasField("position") else None,
            "bearing": vp.position.bearing if vp.HasField("position") else None,
            "speed": vp.position.speed if vp.HasField("position") else None,
            "current_stop_sequence": (
                vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None
            ),
            "stop_id": vp.stop_id or None,
            "current_status": vp.VehicleStopStatus.Name(vp.current_status),
            "timestamp": vp.timestamp if vp.HasField("timestamp") else None,
            "occupancy_status": (
                vp.OccupancyStatus.Name(vp.occupancy_status)
                if vp.HasField("occupancy_status")
                else None
            ),
        }
    )
    return rec


def alert_record(entity) -> dict:
    alert = entity.alert
    return {
        "alert_id": entity.id,
        "active_periods": [
            {
                "start": p.start if p.HasField("start") else None,
                "end": p.end if p.HasField("end") else None,
            }
            for p in alert.active_period
        ],
        "informed_entities": [
            {
                "agency_id": ie.agency_id or None,
                "route_id": ie.route_id or None,
                "route_type": ie.route_type if ie.HasField("route_type") else None,
                "stop_id": ie.stop_id or None,
                "trip_id": ie.trip.trip_id or None,
            }
            for ie in alert.informed_entity
        ],
        "cause": alert.Cause.Name(alert.cause),
        "effect": alert.Effect.Name(alert.effect),
        "severity_level": alert.SeverityLevel.Name(alert.severity_level),
        "header_text": _first_translation(alert.header_text),
        "description_text": _first_translation(alert.description_text),
    }


def _first_translation(translated) -> str | None:
    return translated.translation[0].text if translated.translation else None


def split_feed(msg: gtfs_realtime_pb2.FeedMessage) -> dict[str, list[dict]]:
    """Split a FeedMessage into canonical records per feed type."""
    out: dict[str, list[dict]] = {"trip_updates": [], "vehicle_positions": [], "alerts": []}
    for entity in msg.entity:
        if entity.HasField("trip_update"):
            out["trip_updates"].append(trip_update_record(entity))
        if entity.HasField("vehicle"):
            out["vehicle_positions"].append(vehicle_position_record(entity))
        if entity.HasField("alert"):
            out["alerts"].append(alert_record(entity))
    return out


def envelopes_for_feed(
    *,
    city: str,
    agency: str,
    endpoint: str,
    raw: bytes,
    fetched_at: datetime,
) -> list[dict]:
    """Decode one fetched feed and build one envelope per non-empty feed type."""
    msg = parse_feed(raw)
    feed_ts = msg.header.timestamp or None
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
        for feed_type, records in split_feed(msg).items()
        if records
    ]
