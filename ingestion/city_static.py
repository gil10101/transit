"""Per-city static GTFS sources and request auth — the one implementation shared
by the poller, the Dagster staging asset, and the local parse CLI.

Deliberately stdlib-only (no requests / kafka / boto3): the Dagster image copies
just this module and the city configs, and EMR Serverless has neither.

`static_gtfs` in ingestion/config/cities/<city>.yaml is either a bare URL:

    static_gtfs: https://cdn.mbta.com/MBTA_GTFS.zip

or a mapping when an agency splits its schedule across several zips or the
download needs auth (WMATA ships rail and bus separately, both key-gated):

    static_gtfs:
      rail: {url: ..., auth: {type: header, name: api_key, env: WMATA_API_KEY}}
      bus:  {url: ..., auth: {type: header, name: api_key, env: WMATA_API_KEY}}

Every source of a city lands under ONE gtfs_version_id: the staging models keep
`max(gtfs_version_id)` per city, so versioning the zips separately would hide
one mode's schedule behind the other's (and silently empty that mode's matcher,
scheduled stop times and completeness).

`auth` has the same shape everywhere — {type: header|query, name: ..., env: ...}.
The yaml carries the env var NAME only; the value is resolved per request and
never logged: errors name the variable, never its content.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StaticSource:
    name: str
    url: str
    auth: dict = field(default_factory=dict)


def static_sources(raw: Any) -> list[StaticSource]:
    """Normalize a city config's `static_gtfs` value to a source list.

    Returns [] when the city has no static feed — a live RT-only city is a
    legitimate state (an unverified upstream URL is never guessed), so callers
    skip rather than fail.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        return [StaticSource("default", raw)]
    sources = []
    for name, value in dict(raw).items():
        if isinstance(value, str):
            sources.append(StaticSource(name, value))
        else:
            sources.append(StaticSource(name, value["url"], dict(value.get("auth") or {})))
    return sources


def auth_secret(auth: dict) -> str:
    """Resolve the secret at request time from the env var the config names.
    Error messages carry the VAR NAME only — never the value."""
    env_name = auth["env"]
    value = os.environ.get(env_name, "")
    if not value:
        raise RuntimeError(f"auth env var {env_name} is not set (see .env.example)")
    return value


def resolve_auth(auth: dict) -> tuple[dict, dict]:
    """(headers, params) for one request. Empty dicts when the feed is keyless."""
    if not auth:
        return {}, {}
    kind = auth.get("type")
    if kind == "header":
        return {auth["name"]: auth_secret(auth)}, {}
    if kind == "query":
        return {}, {auth["name"]: auth_secret(auth)}
    raise ValueError(f"unknown auth type {kind!r} (expected 'header' or 'query')")
