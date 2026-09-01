"""Zombie-run reaper: the backstop for a run whose worker died with the box.

[rev 2026-09-01] Why this exists, and why run_monitoring is not enough.

When the services box wedges (2026-08-28, 2026-09-01) the in-flight
warehouse_chain is left STARTED with its run worker gone. run_monitoring's
max_runtime_seconds cannot help: terminating a run goes through the run
launcher, and there is no longer a worker to terminate — the MonitoringDaemon
logs "Checking run <id>" every 120s forever. Observed live on 2026-09-01: the
16:05Z chain sat STARTED for six hours across a reboot and was only cleared by
hand.

That would be survivable if it merely wasted a slot. It does worse. The
QueuedRunCoordinator serializes warehouse_chain at limit 1, so ONE dead run
blocks every later chain: runs pile up QUEUED, nothing executes, and nothing
alerts, because a blocked queue is not a failure. The pipeline stops silently,
which is the one failure mode the whole alerting design is meant to make
impossible.

It also became MORE likely, not less, the moment the box gained an auto-reboot
on a failed instance status check: every future wedge now self-heals into
exactly this state.

So: fail runs that outlived the runtime cap by a margin. The margin matters —
this is a backstop for the case run_monitoring cannot reach, not a competing
policy, so it only acts well after run_monitoring has had its chance. Failing
the run (rather than deleting it) keeps the record and fires the run-failure
sensor, so a reaped zombie arrives as an email like any other failure instead of
vanishing.
"""

from __future__ import annotations

import time

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from .lib import REAP_AFTER_SECONDS, zombie_run_ids


@sensor(
    name="zombie_run_reaper",
    minimum_interval_seconds=120,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Fails runs still STARTED long past run_monitoring's max_runtime, whose "
        "worker died with the box. Without this one dead run blocks the "
        "serialized chain queue forever and the pipeline stops silently."
    ),
)
def zombie_run_reaper(context: SensorEvaluationContext) -> SensorResult:
    now = time.time()
    records = context.instance.get_run_records(
        filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]),
        limit=50,
    )
    by_id = {r.dagster_run.run_id: r for r in records}
    doomed = zombie_run_ids([(r.dagster_run.run_id, r.start_time) for r in records], now)

    reaped: list[str] = []
    for run_id in doomed:
        record = by_id[run_id]
        age_h = (now - record.start_time) / 3600
        context.instance.report_run_failed(
            record.dagster_run,
            f"Reaped by zombie_run_reaper: STARTED for {age_h:.1f}h with no live "
            f"run worker (cap {REAP_AFTER_SECONDS / 3600:.1f}h). The worker almost "
            "certainly died with its host; run_monitoring cannot terminate what no "
            "longer exists, and leaving it STARTED blocks the serialized run queue.",
        )
        reaped.append(run_id)
        context.log.warning(f"reaped zombie run {run_id} ({age_h:.1f}h)")

    return SensorResult(
        skip_reason=None if reaped else "no zombie runs",
    )
