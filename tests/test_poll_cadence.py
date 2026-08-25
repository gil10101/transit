"""Per-endpoint poll cadence (feed_groups.<name>.poll_seconds).

Added for Zurich: its national service-alerts product returns a 10 MB payload that
is byte-identical between polls, so polling it at the city's 30s wrote ~29 GB/day of
duplicate raw objects. No fixtures, no network — config + loop scheduling only.
"""

from ingestion.adapters.base import CityConfig, load_city_config
from ingestion.poller import run_cycle


def _cfg(**kw) -> CityConfig:
    base = dict(
        city="testville",
        agency="TEST",
        timezone="UTC",
        adapter="gtfs_rt",
        poll_seconds=30,
        endpoints={"trip_updates": "https://x/tu", "alerts": "https://x/al"},
    )
    base.update(kw)
    return CityConfig(**base)


def test_endpoint_override_wins_and_city_value_is_the_fallback():
    cfg = _cfg(endpoint_poll_seconds={"alerts": 600})
    assert cfg.poll_seconds_for("alerts") == 600
    assert cfg.poll_seconds_for("trip_updates") == 30  # falls back to the city value
    assert cfg.poll_seconds_for("nonexistent") == 30


def test_floor_applies_per_endpoint_so_an_override_can_only_slow_a_feed():
    # the 30s floor exists so we never hammer an agency; a per-feed override must
    # not become a way around it
    cfg = _cfg(endpoint_poll_seconds={"alerts": 1})
    assert cfg.poll_seconds_for("alerts") == 30


def test_zurich_yaml_carries_the_measured_cadences():
    cfg = load_city_config("zurich")
    assert cfg.poll_seconds_for("trip_updates") == 60
    assert cfg.poll_seconds_for("alerts") == 600


def test_every_other_city_keeps_one_cadence_for_all_its_endpoints():
    # the override is opt-in: no city but zurich should have gained per-feed rates
    for city in ("nyc", "boston", "toronto", "helsinki", "dc", "sf"):
        cfg = load_city_config(city)
        rates = {cfg.poll_seconds_for(n) for n in cfg.endpoints}
        assert rates == {cfg.effective_poll_seconds}, city


class _Pool:
    """Runs submitted work inline so run_cycle needs no threads."""

    def submit(self, fn, *args):
        class _F:
            def __init__(self, value=None, exc=None):
                self._v, self._e = value, exc

            def result(self):
                if self._e:
                    raise self._e
                return self._v

        try:
            return _F(value=fn(*args))
        except Exception as exc:  # noqa: BLE001 — mirrors the executor's behaviour
            return _F(exc=exc)


class _Emitter:
    def __init__(self):
        self.emitted = []

    def emit(self, env):
        self.emitted.append(env)

    def flush(self):
        return None


def test_run_cycle_polls_only_the_due_endpoints(monkeypatch, capsys):
    polled = []

    def fake_poll(session, cfg, archiver, name, url):
        polled.append(name)
        return name, 10, []

    monkeypatch.setattr("ingestion.poller.poll_endpoint", fake_poll)
    cfg = _cfg(endpoint_poll_seconds={"alerts": 600})

    run_cycle(None, cfg, _Emitter(), None, _Pool(), due=["trip_updates"])
    assert polled == ["trip_updates"]
    # a partial cycle says so, otherwise "al=0" would read as an empty alerts feed
    assert "polled=trip_updates" in capsys.readouterr().out

    polled.clear()
    run_cycle(None, cfg, _Emitter(), None, _Pool())  # due=None -> everything
    assert sorted(polled) == ["alerts", "trip_updates"]
    assert "polled=" not in capsys.readouterr().out


def test_a_failing_endpoint_does_not_stop_the_others(monkeypatch, capsys):
    def fake_poll(session, cfg, archiver, name, url):
        if name == "alerts":
            raise RuntimeError("503")
        return name, 10, []

    monkeypatch.setattr("ingestion.poller.poll_endpoint", fake_poll)
    run_cycle(None, _cfg(), _Emitter(), None, _Pool())
    out = capsys.readouterr().out
    assert "FAILURES=" in out and "503" in out
