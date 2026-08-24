"""Auth plumbing for the P3 batch-2 keyed cities — no fixtures, NO network, and only
DUMMY env values (never real keys; monkeypatch scrubs them). Verifies exactly what
fetch_feed will send before a single live request is spent (511's budget is 60/hr).
"""

import pytest
import requests

from ingestion.adapters.base import build_session, fetch_feed, load_city_config


class FakeResponse:
    content = b"pb-bytes"

    def raise_for_status(self):
        return None


class FakeSession:
    """Records the kwargs fetch_feed passes; returns canned bytes."""

    def __init__(self):
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return FakeResponse()


@pytest.fixture
def scrubbed_env(monkeypatch):
    """Real keys must never reach a test; every name resolves to a dummy."""
    for name in ("WMATA_API_KEY", "BAY511_API_TOKEN", "SWISS_OTD_TOKEN", "SWISS_OTD_SA_TOKEN"):
        monkeypatch.setenv(name, f"dummy-{name.lower()}")
    return monkeypatch


def test_dc_header_auth_on_every_endpoint(scrubbed_env):
    cfg = load_city_config("dc")
    assert len(cfg.endpoints) == 6  # {rail,bus} x {tu,vp,alerts}
    session = FakeSession()
    for name, url in cfg.endpoints.items():
        fetch_feed(session, cfg, url, endpoint=name)
    for call in session.calls:
        assert call["headers"] == {"api_key": "dummy-wmata_api_key"}
        assert call["params"] is None


def test_sf_query_auth_merges_with_agency_param(scrubbed_env):
    cfg = load_city_config("sf")
    assert len(cfg.endpoints) == 3
    assert cfg.effective_poll_seconds == 200  # 54 req/hr under the 60/hr hard limit
    session = FakeSession()
    for name, url in cfg.endpoints.items():
        fetch_feed(session, cfg, url, endpoint=name)
        # agency=RG lives in the URL; the key is a separate param. requests merges
        # both — prove it on a PreparedRequest (offline).
        prepared = requests.Request("GET", url, params=session.calls[-1]["params"]).prepare()
        assert "agency=RG" in prepared.url
        assert "api_key=dummy-bay511_api_token" in prepared.url
    assert all(c["headers"] is None for c in session.calls)


def test_zurich_per_endpoint_tokens_raw_header(scrubbed_env):
    # One token per Swiss OTD API product: gtfs-rt and gtfs-sa must NOT share a token,
    # and the header is the RAW token (no "Bearer " prefix).
    cfg = load_city_config("zurich")
    assert set(cfg.endpoints) == {"trip_updates", "alerts"}
    session = FakeSession()
    for name, url in cfg.endpoints.items():
        fetch_feed(session, cfg, url, endpoint=name)
    by_url = {c["url"]: c["headers"] for c in session.calls}
    assert by_url["https://api.opentransportdata.swiss/la/gtfs-rt"] == {
        "Authorization": "dummy-swiss_otd_token"
    }
    assert by_url["https://api.opentransportdata.swiss/la/gtfs-sa"] == {
        "Authorization": "dummy-swiss_otd_sa_token"
    }


def test_missing_env_var_names_the_var_not_the_value(scrubbed_env, monkeypatch):
    monkeypatch.delenv("WMATA_API_KEY")
    cfg = load_city_config("dc")
    with pytest.raises(RuntimeError, match="WMATA_API_KEY"):
        fetch_feed(FakeSession(), cfg, next(iter(cfg.endpoints.values())), endpoint="rail_alerts")


def test_session_carries_no_auth(scrubbed_env):
    # Auth must be per-request: a shared session header would leak the wrong token
    # across Zurich's two endpoints (and into redirects requests wouldn't strip).
    for city in ("dc", "sf", "zurich"):
        session = build_session(load_city_config(city))
        assert "Authorization" not in session.headers
        assert "api_key" not in session.headers


def test_keyless_batch1_city_unaffected(scrubbed_env):
    cfg = load_city_config("boston")
    session = FakeSession()
    fetch_feed(session, cfg, cfg.endpoints["trip_updates"], endpoint="trip_updates")
    assert session.calls[0]["headers"] is None
    assert session.calls[0]["params"] is None
