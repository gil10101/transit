"""Decode tests over checked-in ODPT fixtures (tests/fixtures/tokyo_*). No live calls.

Encodes the verified Tokyo quirks (fixtures 2026-09-01, key active):
- odpt:Train on the permanent center is TOEI ONLY: 4 subway lines (Asakusa, Mita,
  Oedo, Shinjuku — odpt:delay set on 100% of their trains, almost always 0) plus the
  Arakawa tram (position only, NO delay field). Tokyo Metro publishes NO odpt:Train
  here (catalog: statics + TrainInformation + alerts-only GTFS-RT). The poller still
  asks for both operators; Metro flows the moment they publish.
- toStation is null on ~3/4 of snapshots and the spec says non-null does NOT prove
  "moving" — arrival detection downstream is transition-based on fromStation.
- dc:date/dct:valid are +09:00 ISO stamps; dct:valid ≈ dc:date + 5 min. odpt:frequency
  was absent from every live object despite appearing in the spec's sample.
- odpt:TrainInformation covers all 16 Metro+Toei lines; during normal operation
  trainInformationStatus is OMITTED and the text says "平常どおり" — those objects are
  dropped (not alerts). Fixture recorded on a quiet day: 16/16 normal, so the alerts
  envelope is empty; a disruption fixture upgrade is welcome when one is captured.
- multilingual objects can be ja-ONLY (no en key) — flatten falls back to ja.
- ToeiBus GTFS-RT is VehiclePosition ONLY (532 VP, 0 TU, 0 alerts) with trip_id and
  route_id set per VP -> service volume / live map; no bus OTP without TUs.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

from ingestion.adapters import gtfs_rt, odpt

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FETCHED_AT = datetime(2026, 9, 1, 2, 0, 0, tzinfo=UTC)


def load_json(name: str) -> list[dict]:
    return json.loads((FIXTURE_DIR / f"tokyo_{name}.json").read_bytes())


def envelopes(name: str) -> dict[str, dict]:
    raw = (FIXTURE_DIR / f"tokyo_{name}.json").read_bytes()
    envs = odpt.envelopes_for_feed(
        city="tokyo", agency="ODPT", endpoint=name, raw=raw, fetched_at=FETCHED_AT
    )
    return {e["feed"]: e for e in envs}


def test_trains_fixture_is_toei_only_with_subway_delays():
    objs = load_json("trains")
    assert all(o["@type"] == "odpt:Train" for o in objs)
    operators = {o["odpt:operator"] for o in objs}
    assert operators == {"odpt.Operator:Toei"}  # Metro absent — see module docstring
    subway = [o for o in objs if "Arakawa" not in o["odpt:railway"]]
    tram = [o for o in objs if "Arakawa" in o["odpt:railway"]]
    assert subway and tram
    assert all(o.get("odpt:delay") is not None for o in subway)
    assert all(o.get("odpt:delay") is None for o in tram)


def test_train_record_shape():
    env = envelopes("trains")
    assert set(env) == {"odpt_trains"}
    e = env["odpt_trains"]
    assert e["source_format"] == "odpt_json"
    assert e["schema_version"] == 2
    assert e["feed_ts"] and e["feed_ts"] > 1_780_000_000
    for rec in e["payload"]:
        # trip_id is the owl:sameAs tail: Operator.Line.TrainNumber — the canonical
        # trip_uid COALESCE picks it up unchanged and it equijoins TrainTimetable
        parts = rec["trip_id"].split(".")
        assert parts[0] == "Toei" and parts[-1] == rec["train_number"]
        assert rec["railway"].startswith("odpt.Railway:Toei.")
        assert rec["operator"] == "Toei"
        assert rec["timestamp"] and rec["valid_until"] > rec["timestamp"]
        assert rec["from_station"] is None or rec["from_station"].startswith("odpt.Station:")
    delays = [r["delay_sec"] for r in e["payload"] if r["delay_sec"] is not None]
    assert delays and all(isinstance(d, int) for d in delays)


def test_train_information_normal_ops_produces_no_alerts():
    objs = load_json("train_information")
    assert {o["@type"] for o in objs} == {"odpt:TrainInformation"}
    # quiet-day fixture: every line normal -> status omitted -> nothing is an alert
    assert all("odpt:trainInformationStatus" not in o for o in objs)
    assert envelopes("train_information") == {}
    # both operators' lines present — Metro's TrainInformation IS on this center
    assert {o["odpt:operator"] for o in objs} == {
        "odpt.Operator:TokyoMetro",
        "odpt.Operator:Toei",
    }


def test_train_information_disruption_flattens_to_alert_record():
    # synthetic disruption (shape from the live spec: ja-only multilingual objects)
    obj = {
        "@type": "odpt:TrainInformation",
        "owl:sameAs": "odpt.TrainInformation:TokyoMetro.Ginza",
        "dc:date": "2026-09-01T11:00:00+09:00",
        "odpt:timeOfOrigin": "2026-09-01T10:55:00+09:00",
        "odpt:operator": "odpt.Operator:TokyoMetro",
        "odpt:railway": "odpt.Railway:TokyoMetro.Ginza",
        "odpt:trainInformationStatus": {"ja": "遅延"},
        "odpt:trainInformationText": {"ja": "上野駅での混雑の影響で、遅れが出ています。"},
    }
    split = odpt.split_feed([obj])
    assert split["odpt_trains"] == []
    (rec,) = split["alerts"]
    assert rec["alert_id"] == "odpt.TrainInformation:TokyoMetro.Ginza"
    assert rec["header_text"] == "遅延"  # ja fallback: no en key on the live feed
    assert rec["informed_entities"][0]["route_id"] == "odpt.Railway:TokyoMetro.Ginza"
    assert rec["informed_entities"][0]["agency_id"] == "TokyoMetro"
    expected = int(datetime.fromisoformat("2026-09-01T10:55:00+09:00").timestamp())
    assert rec["active_periods"][0]["start"] == expected  # 10:55 JST -> epoch
    # gtfs_rt.alert_record key parity — silver's ALERT_RECORD parses both alike
    synth = gtfs_rt.split_feed(_synthetic_alert_feed())["alerts"][0]
    assert set(rec) == set(synth)


def _synthetic_alert_feed():
    from google.transit import gtfs_realtime_pb2 as rt

    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    ent = msg.entity.add()
    ent.id = "x"
    ent.alert.header_text.translation.add().text = "hdr"
    return msg


def test_toeibus_fixture_is_vehicle_positions_only():
    msg = gtfs_rt.parse_feed((FIXTURE_DIR / "tokyo_toeibus_vehicle_positions.pb").read_bytes())
    assert msg.header.timestamp > 0
    kinds = {
        "tu": sum(1 for e in msg.entity if e.HasField("trip_update")),
        "vp": sum(1 for e in msg.entity if e.HasField("vehicle")),
        "al": sum(1 for e in msg.entity if e.HasField("alert")),
    }
    assert kinds["vp"] > 0 and kinds["tu"] == 0 and kinds["al"] == 0
    vp = next(e for e in msg.entity if e.HasField("vehicle")).vehicle
    assert vp.trip.trip_id and vp.trip.route_id  # joinable to ToeiBus GTFS static


def test_empty_array_yields_no_envelopes():
    assert (
        odpt.envelopes_for_feed(
            city="tokyo", agency="ODPT", endpoint="trains", raw=b"[]", fetched_at=FETCHED_AT
        )
        == []
    )
