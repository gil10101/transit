"""Bronze -> silver backfill: window parsing and the EMR request shape.

The replay itself runs on EMR against the lake; what can break silently on this side
is a window that parses wrong or a request that the drain guard mistakes for a drain.
"""

from datetime import datetime

import pytest

from orchestration.transit_dagster.lib import backfill_job_request
from spark_jobs import silver_normalize as sn
from spark_jobs.silver_backfill import parse_windows

ENV = {
    "APP_ID": "app",
    "EXEC_ROLE_ARN": "role",
    "ENTRY_POINT": "s3://artifacts/code/entry.py",
    "SPARK_PARAMS": "--py-files s3://artifacts/code/spark_jobs.zip --conf x=1",
    "LOG_URI": "s3://artifacts/emr-logs/",
}


def test_windows_parse_as_utc_iso():
    assert parse_windows(["helsinki,2026-09-10T14:00,2026-09-10T22:30"]) == [
        ("helsinki", datetime(2026, 9, 10, 14, 0), datetime(2026, 9, 10, 22, 30))
    ]


def test_inverted_window_is_refused_not_silently_empty():
    with pytest.raises(ValueError):
        parse_windows(["nyc,2026-09-10T08:00,2026-09-10T06:00"])


def test_no_windows_is_a_usage_error():
    with pytest.raises(SystemExit):
        parse_windows([])


def test_request_reuses_drain_params_and_stages_beside_entry():
    req = backfill_job_request(ENV, ["nyc,2026-09-10T05:00,2026-09-10T08:00"])
    submit = req["jobDriver"]["sparkSubmit"]
    assert submit["entryPoint"] == "s3://artifacts/code/silver_backfill.py"
    assert submit["entryPointArguments"] == ["nyc,2026-09-10T05:00,2026-09-10T08:00"]
    assert submit["sparkSubmitParameters"] == ENV["SPARK_PARAMS"]


def test_request_is_never_adopted_as_a_drain():
    # EmrResource adopts any active run whose name starts with "transit-drain"
    assert not backfill_job_request(ENV, ["nyc,2026-09-10T05:00,2026-09-10T08:00"])[
        "name"
    ].startswith("transit-drain")


def test_every_silver_table_has_a_dedup_identity():
    names = {table.split(".")[-1] for table in sn.TABLES}
    assert names == set(sn.DEDUP_KEYS)
