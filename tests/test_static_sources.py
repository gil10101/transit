"""Static GTFS source normalization and the staged-zip lister."""

from __future__ import annotations

import pytest

from ingestion.city_static import resolve_auth, static_sources
from spark_jobs import gtfs_static_parse as gsp


def test_bare_url_is_one_default_source():
    got = static_sources("https://cdn.mbta.com/MBTA_GTFS.zip")
    assert [(s.name, s.url, s.auth) for s in got] == [
        ("default", "https://cdn.mbta.com/MBTA_GTFS.zip", {})
    ]


def test_missing_static_is_empty_not_an_error():
    # a live RT-only city is legitimate; the weekly asset skips it
    assert static_sources(None) == []


def test_multi_source_keeps_per_source_auth():
    got = static_sources(
        {
            "rail": {
                "url": "https://api.wmata.com/gtfs/rail-gtfs-static.zip",
                "auth": {"type": "header", "name": "api_key", "env": "WMATA_API_KEY"},
            },
            "bus": {
                "url": "https://api.wmata.com/gtfs/bus-gtfs-static.zip",
                "auth": {"type": "header", "name": "api_key", "env": "WMATA_API_KEY"},
            },
        }
    )
    assert [s.name for s in got] == ["rail", "bus"]
    assert all(s.auth["env"] == "WMATA_API_KEY" for s in got)


def test_resolve_auth_reads_env_and_never_returns_the_name_as_value(monkeypatch):
    monkeypatch.setenv("WMATA_API_KEY", "s3cret-value")
    headers, params = resolve_auth({"type": "header", "name": "api_key", "env": "WMATA_API_KEY"})
    assert headers == {"api_key": "s3cret-value"} and params == {}
    headers, params = resolve_auth({"type": "query", "name": "api_key", "env": "WMATA_API_KEY"})
    assert params == {"api_key": "s3cret-value"} and headers == {}


def test_resolve_auth_names_the_variable_not_the_value_when_unset(monkeypatch):
    monkeypatch.delenv("BAY511_API_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="BAY511_API_TOKEN"):
        resolve_auth({"type": "query", "name": "api_key", "env": "BAY511_API_TOKEN"})


class _FakeS3:
    def __init__(self, keys):
        self._keys = keys

    def get_paginator(self, _op):
        keys = self._keys

        class P:
            def paginate(self, Bucket, Prefix):  # noqa: N803 — boto3 kwarg names
                yield {"Contents": [{"Key": k} for k in keys if k.startswith(Prefix)]}

        return P()


def test_archived_sources_ignores_a_legacy_top_level_zip(monkeypatch):
    # the pre-multi-source layout put the zip at <version>/gtfs.zip; when an
    # unchanged upstream file is re-staged after UTC midnight it lands under the
    # SAME version id, and reading it as a source produced <version>/gtfs.zip/gtfs.zip
    version = "toronto-20260824-6a2597e0"
    monkeypatch.setattr(
        gsp,
        "_s3",
        lambda: _FakeS3(
            [
                f"static/toronto/{version}/gtfs.zip",  # legacy, must be ignored
                f"static/toronto/{version}/default/gtfs.zip",
                f"static/toronto/{version}/default/txt/stops.txt",
            ]
        ),
    )
    assert gsp.archived_sources("toronto", version) == ["default"]


def test_archived_sources_lists_every_source(monkeypatch):
    version = "dc-20260824-abcd1234"
    monkeypatch.setattr(
        gsp,
        "_s3",
        lambda: _FakeS3(
            [f"static/dc/{version}/rail/gtfs.zip", f"static/dc/{version}/bus/gtfs.zip"]
        ),
    )
    assert gsp.archived_sources("dc", version) == ["bus", "rail"]


def test_archived_sources_raises_when_nothing_is_staged(monkeypatch):
    monkeypatch.setattr(gsp, "_s3", lambda: _FakeS3([]))
    with pytest.raises(SystemExit, match="no staged zips"):
        gsp.archived_sources("sf", "sf-20260824-deadbeef")
