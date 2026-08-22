"""Dagster definitions. Assets arrive in later phases (dagster-dbt, GTFS refresh, sensors)."""

from dagster import Definitions

defs = Definitions(assets=[])
