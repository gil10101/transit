"""Run-failure alert message shape.

Only orchestration.transit_dagster.lib is imported — like test_dagster_cities.py, these
run in the repo venv without dagster installed. The sensor wiring itself (default status
RUNNING, registration in Definitions) cannot be asserted here for that reason; it is
pinned by an explicit `default_status=` in alerting.py and reviewed on deploy.
"""

from orchestration.transit_dagster.lib import ALERT_MAX_ERROR_CHARS, alert_body


def test_body_leads_with_job_and_run_so_it_is_readable_on_a_phone():
    body = alert_body("abc123", "warehouse_chain", "RuntimeError: EMR run FAILED")
    assert body.splitlines()[:2] == ["job: warehouse_chain", "run: abc123"]
    assert "RuntimeError: EMR run FAILED" in body


def test_long_errors_are_truncated_rather_than_rejected_by_sns():
    # SNS caps a message at 256 KB; a truncated alert that arrives beats a rejected one
    body = alert_body("r", "j", "x" * (ALERT_MAX_ERROR_CHARS * 3))
    assert len(body) < ALERT_MAX_ERROR_CHARS + 200
    assert body.endswith("... (truncated)")


def test_missing_error_detail_still_produces_a_usable_message():
    # a failure with no message must still page someone, not render as an empty alert
    assert "no error detail recorded" in alert_body("r", "j", None)
    assert "no error detail recorded" in alert_body("r", "j", "   ")
