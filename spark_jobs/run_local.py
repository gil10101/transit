"""Run bronze + silver streaming jobs in one local Spark session (make spark-local).

Supervised: a transient stream failure (e.g. a one-off MinIO 403 on a checkpoint
read) tears down the session and rebuilds all queries. Kafka offsets + Iceberg
commits live in the checkpoints, so restarts are lossless.
"""

from __future__ import annotations

import time
import traceback

from spark_jobs import bronze_writer, silver_normalize
from spark_jobs.session import build_spark

MAX_RESTARTS = 20
BACKOFF_SECONDS = 30


def run_once() -> None:
    spark = build_spark("transit-pulse-local")
    spark.sparkContext.setLogLevel("WARN")
    try:
        queries = [bronze_writer.start(spark), *silver_normalize.start(spark)]
        print(f"streaming queries running: {[q.name for q in queries]}", flush=True)
        spark.streams.awaitAnyTermination()
        # a query died; surface its error to the supervisor
        for q in queries:
            if q.exception() is not None:
                raise RuntimeError(f"{q.name}: {q.exception()}")
    finally:
        spark.stop()


def main() -> None:
    restarts = 0
    while True:
        try:
            run_once()
        except KeyboardInterrupt:
            return
        except Exception:
            restarts += 1
            if restarts > MAX_RESTARTS:
                raise
            traceback.print_exc()
            print(
                f"stream failure; restart {restarts}/{MAX_RESTARTS} in {BACKOFF_SECONDS}s",
                flush=True,
            )
            time.sleep(BACKOFF_SECONDS)


if __name__ == "__main__":
    main()
