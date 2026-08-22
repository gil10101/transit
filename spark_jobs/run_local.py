"""Run bronze + silver streaming jobs in one local Spark session (make spark-local)."""

from __future__ import annotations

from spark_jobs import bronze_writer, silver_normalize
from spark_jobs.session import build_spark


def main() -> None:
    spark = build_spark("transit-pulse-local")
    spark.sparkContext.setLogLevel("WARN")
    queries = [bronze_writer.start(spark), *silver_normalize.start(spark)]
    print(f"streaming queries running: {[q.name for q in queries]}", flush=True)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()


if __name__ == "__main__":
    main()
