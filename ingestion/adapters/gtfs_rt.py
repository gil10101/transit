"""Generic GTFS-RT adapter: protobuf FeedMessage -> canonical envelopes.

Handles combined feeds (NYC: one endpoint carries TU+VP+Alert entities) and per-type
feeds alike — entities are split by type and emitted to the matching topic.
Record shapes follow docs/01-data-dictionary.md §A.2–A.4.
"""

from __future__ import annotations

from datetime import datetime

from google.transit import gtfs_realtime_pb2

from ingestion.adapters.base import build_envelope

SOURCE_FORMAT = "gtfs_rt"


def parse_feed(raw: bytes) -> gtfs_realtime_pb2.FeedMessage:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    return msg


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
