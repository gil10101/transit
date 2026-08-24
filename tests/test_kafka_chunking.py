"""Oversized envelopes split into broker-sized messages (511 regional feed)."""

from __future__ import annotations

import json

from ingestion.adapters.base import MAX_MESSAGE_BYTES, KafkaEmitter


class _Emitter(KafkaEmitter):
    def __init__(self):  # no broker connection in tests
        pass


def envelope(n_records: int) -> dict:
    return {
        "city": "sf",
        "agency": "511-RG",
        "feed": "trip_updates",
        "fetched_at": "2026-08-24T16:30:41Z",
        "source_format": "gtfs-rt",
        "schema_version": 2,
        "endpoint": "trip_updates",
        "feed_ts": 1787588000,
        "payload": [
            {"trip_id": f"SF:{i}", "route_id": "14", "stop_time_updates": [{"stop_id": "x" * 40}]}
            for i in range(n_records)
        ],
    }


def test_small_envelope_emitted_whole():
    chunks = _Emitter()._size_bounded(envelope(5))
    assert len(chunks) == 1
    assert json.loads(chunks[0])["payload"][0]["trip_id"] == "SF:0"


def test_large_envelope_splits_under_limit_preserving_every_record():
    # ~115 bytes/record here, so 12k records clear the 900 KB bound comfortably;
    # the real 511 trip-update envelope is ~2.4 MB
    original = envelope(12_000)
    chunks = _Emitter()._size_bounded(original)
    assert len(chunks) > 1
    assert all(len(c) <= MAX_MESSAGE_BYTES for c in chunks)
    decoded = [json.loads(c) for c in chunks]
    # metadata identical on every chunk; payload partitioned, order preserved
    for d in decoded:
        assert {k: v for k, v in d.items() if k != "payload"} == {
            k: v for k, v in original.items() if k != "payload"
        }
    rebuilt = [rec for d in decoded for rec in d["payload"]]
    assert rebuilt == original["payload"]


def test_single_record_over_limit_is_not_dropped():
    # a lone huge record cannot be split further — emit it and let the broker
    # decide, rather than silently losing the trip
    huge = envelope(1)
    huge["payload"][0]["stop_time_updates"] = [{"stop_id": "y" * (MAX_MESSAGE_BYTES + 100)}]
    chunks = _Emitter()._size_bounded(huge)
    assert len(chunks) == 1
    assert len(json.loads(chunks[0])["payload"]) == 1
