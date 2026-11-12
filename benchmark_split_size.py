"""Controlled check of the input split size (spark.sql.files.maxPartitionBytes).

Why: in the first full run the scan/regex stage of B1 had 25 tasks on 24 cores,
but only 12 tasks contained 2020-2026 papers (the source files are ordered by
update_date), so those 12 tasks did all the regex work while the other cores
were idle (see reports/baseline_run/stage_B1_scan_tasks_128MB_splits.png).

This script runs the SAME action (cleaned papers -> monthly AI counts) on the
SAME full data in ONE Spark application with different split sizes, three
times each, checks that the results are identical, and records the medians.
The raw files were already in the OS file cache from earlier runs, so all
settings start equally warm.

Usage:
    python benchmark_split_size.py      # writes reports/split_size_benchmark.json
"""

import json
import statistics
import time

from pyspark.sql import functions as F

import pipeline as P
from spark_session import ROOT, build_spark, safe_path

SETTINGS = ["128m", "64m", "32m"]
REPEATS = 3


def main():
    spark = build_spark("split-size-benchmark", shuffle_partitions=P.SHUFFLE_PARTITIONS,
                        event_log_dir=P.EVENT_LOG_DIR)
    rules = P.load_rules()
    files = [safe_path(f) for f in sorted(P.RAW_DIR.glob("*.parquet"))]
    results, reference = {}, None
    for size in SETTINGS:
        spark.conf.set("spark.sql.files.maxPartitionBytes", size)
        raw = spark.read.parquet(*files)
        papers = P.clean_papers(raw, rules)
        times = []
        for i in range(REPEATS):
            spark.sparkContext.setJobDescription(f"Split-size benchmark {size} run {i + 1}")
            t = time.perf_counter()
            rows = P.cache_query(papers)
            times.append(round(time.perf_counter() - t, 3))
        out = [r.asDict() for r in rows]
        reference = reference or out
        results[size] = {
            "input_partitions": raw.rdd.getNumPartitions(),
            "seconds": times,
            "median_seconds": statistics.median(times),
            "same_result_as_128m": out == reference,
        }
        print(size, results[size])
    report = {
        "application_id": spark.sparkContext.applicationId,
        "query": "clean_papers(raw) -> groupBy(month_start).agg(count, sum(ai_related), sum(mentions_genai_llm))",
        "local_cores": spark.sparkContext.defaultParallelism,
        "results": results,
    }
    spark.stop()
    path = ROOT / "reports" / "split_size_benchmark.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"written {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
