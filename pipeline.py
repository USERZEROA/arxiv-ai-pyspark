"""AI Research Growth and Cross-Disciplinary Diffusion: a PySpark analysis of arXiv, 2020-2026.

Batch pipeline over the pinned arXiv metadata snapshot (11 Parquet files, ~2.7 GB):

  A. load + data-quality checks           (raw Parquet, column pruning, one-pass QA aggregation)
  B. clean + transform + join             (filters, withColumn, explode, broadcast join, groupBy)
  C. two Spark SQL queries                (monthly AI trends; 2025 vs 2026 field comparison)
  D. write Parquet/CSV, read back, verify (partitioned by submitted_year)
  P. performance evidence                 (explain plans, partition pruning, lazy evaluation demo,
                                           optional cache experiment, Spark REST metrics)

Usage:
    python pipeline.py                 # full run on all 11 shards (the graded run)
    python pipeline.py --dev           # quick development run on shard 0 only (outputs/dev, reports/dev)
    python pipeline.py --skip-cache    # skip the optional cache experiment
    python pipeline.py --capture-ui    # also screenshot the live Spark UI (needs playwright + Chrome)

The analysis is descriptive. Counts of keyword hits or category labels say
nothing about whether AI *caused* anything.
"""

import argparse
import contextlib
import inspect
import io
import json
import logging
import os
import platform
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_session import ROOT, build_spark, safe_path

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
RAW_DIR = ROOT / "data" / "raw" / "arxiv-metadata-snapshot" / "data"
CATEGORIES_CSV = ROOT / "metadata" / "arxiv_categories.csv"
RULES_JSON = ROOT / "metadata" / "classification_rules.json"
MANIFEST = ROOT / "metadata" / "data_manifest.json"
SQL_DIR = ROOT / "sql"
EVENT_LOG_DIR = ROOT / "spark-events"

EXPECTED_ROWS = 3_224_635        # source-declared row count of the pinned revision
STUDY_START = "2020-01-01"       # inclusive, v1 submission date (UTC)
STUDY_END = "2026-10-01"         # exclusive; the snapshot's last v1 date is 2026-10-22
COMPARE_LAST_MONTH = 9           # 2026 vs 2025 compared on January..September only
YTD_YEAR = 2026

# Only these raw columns are read (authors, DOI, comments, ... are never loaded).
NEEDED_COLUMNS = ["id", "title", "abstract", "categories", "versions", "update_date"]

# Columns kept in the cleaned paper-level table (the abstract is dropped after matching).
PAPER_COLUMNS = [
    "id", "title", "update_date", "submitted_date", "submitted_year", "submitted_month",
    "month_start", "category_list", "is_ai_category", "mentions_ai_terms",
    "mentions_genai_llm", "ai_related", "text_available", "abstract_available",
]

# v1 `created` looks like "Mon, 2 Apr 2007 19:18:42 GMT". Spark >= 3 cannot *parse*
# the day-of-week field (E), so the "Mon, " prefix is stripped first.
V1_WEEKDAY_PREFIX = r"^\s*[A-Za-z]{3},\s*"
V1_DATE_FORMAT = "d MMM yyyy HH:mm:ss z"

# 2x the 24 local cores instead of the default 200; AQE coalesces small partitions further.
SHUFFLE_PARTITIONS = 48

log = logging.getLogger("pipeline")


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
class Paths:
    def __init__(self, dev: bool):
        sub = "dev" if dev else ""
        self.outputs = ROOT / "outputs" / sub
        self.reports = ROOT / "reports" / sub
        self.plans = self.reports / "plans"
        self.rest = self.reports / "spark_rest"
        self.screenshots = ROOT / "screenshots" / sub
        for p in (self.outputs, self.reports, self.plans, self.rest):
            p.mkdir(parents=True, exist_ok=True)

    def out(self, name):
        return safe_path(self.outputs / name)


def setup_logging(log_file: Path):
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(message)s", "%H:%M:%S")
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, "w", encoding="utf-8")):
        h.setFormatter(fmt)
        log.addHandler(h)


TIMINGS = {}


@contextlib.contextmanager
def stage(spark, key, description):
    """Time a pipeline step (wall clock, including the actions it triggers) and label
    its Spark jobs so they are easy to find in the Spark UI / event log."""
    spark.sparkContext.setJobDescription(description)
    log.info(f"[start] {description}")
    t0 = time.perf_counter()
    yield
    secs = time.perf_counter() - t0
    TIMINGS[key] = {"description": description, "seconds": round(secs, 3)}
    log.info(f"[done ] {description}  ({secs:.1f} s)")
    spark.sparkContext.setJobDescription(None)


def save_plan(df: DataFrame, path: Path, mode="formatted") -> str:
    """Save df.explain(mode) output to a text file and return it."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    text = buf.getvalue()
    path.write_text(text, encoding="utf-8")
    return text


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def load_rules(path=RULES_JSON) -> dict:
    rules = json.loads(Path(path).read_text(encoding="utf-8"))
    rules["genai_llm_regex"] = f"(?:{rules['genai_llm_phrase_regex']})|(?:{rules['llm_acronym_regex']})"
    return rules


# --------------------------------------------------------------------------------------
# Transformations (pure DataFrame -> DataFrame functions; also used by the tests)
# --------------------------------------------------------------------------------------
def with_submission_date(df: DataFrame) -> DataFrame:
    """Add the first-submission timestamp taken from the versions entry labelled 'v1'.

    The array element is selected by its label, not by position. A missing v1
    entry or an unparseable string gives NULL (try_to_timestamp never raises),
    and update_date is *not* used as a fallback.
    """
    v1_entries = F.filter("versions", lambda v: v["version"] == "v1")
    return (
        df.withColumn("v1_count", F.coalesce(F.size(v1_entries), F.lit(0)))
        .withColumn("v1_created_raw", F.get(v1_entries, 0)["created"])
        .withColumn(
            "submitted_ts",
            F.try_to_timestamp(F.regexp_replace("v1_created_raw", V1_WEEKDAY_PREFIX, ""),
                               F.lit(V1_DATE_FORMAT)),
        )
    )


def add_time_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("submitted_date", F.to_date("submitted_ts"))   # session time zone = UTC
        .withColumn("submitted_year", F.year("submitted_date"))
        .withColumn("submitted_month", F.month("submitted_date"))
        .withColumn("month_start", F.trunc("submitted_date", "month"))
    )


def add_ai_flags(df: DataFrame, rules: dict) -> DataFrame:
    """Rule-based AI flags (see metadata/classification_rules.json)."""
    title = F.coalesce(F.col("title"), F.lit(""))
    abstract = F.coalesce(F.col("abstract"), F.lit(""))
    # whitespace, hyphens/dashes and underscores -> single space
    norm_text = F.regexp_replace(F.concat_ws(" ", title, abstract), r"[\s\-‐-―_]+", " ")
    split_cats = F.split(F.trim(F.coalesce(F.col("categories"), F.lit(""))), r"\s+")
    ai_cats = F.array(*[F.lit(c) for c in rules["ai_categories"]])
    return (
        df.withColumn("category_list", F.array_distinct(F.filter(split_cats, lambda c: c != "")))
        .withColumn("norm_text", norm_text)
        .withColumn("text_available", (F.length(F.trim(title)) > 0) | (F.length(F.trim(abstract)) > 0))
        .withColumn("abstract_available", F.length(F.trim(abstract)) > 0)
        .withColumn("is_ai_category", F.coalesce(F.arrays_overlap("category_list", ai_cats), F.lit(False)))
        .withColumn("mentions_genai_llm",
                    F.coalesce(F.col("norm_text").rlike(rules["genai_llm_regex"]), F.lit(False)))
        .withColumn("mentions_ai_terms",
                    F.coalesce(F.col("norm_text").rlike(rules["ai_term_regex"]), F.lit(False))
                    | F.col("mentions_genai_llm"))
        .withColumn("ai_related", F.col("is_ai_category") | F.col("mentions_ai_terms"))
        .drop("norm_text")
    )


def clean_papers(raw: DataFrame, rules: dict, start=STUDY_START, end=STUDY_END,
                 use_update_prefilter=True) -> DataFrame:
    """Raw metadata -> one row per paper in the study window, with AI flags.

    Filters are written as early as their inputs allow:
      1. valid id                    (top-level column -> pushed into the Parquet scan)
      2. update_date >= start        (top-level column -> pushed; a lossless pre-filter
                                      because a paper's last update can never precede its
                                      v1 submission - verified on this snapshot by the QA step)
      3. parseable v1 timestamp      (derived from the nested versions array; NOT pushable)
      4. study window on v1 date     (derived; NOT pushable)
    The expensive regex matching only runs after filters 1-4.
    """
    df = raw.select(*NEEDED_COLUMNS)                                   # column pruning
    df = df.filter(F.col("id").isNotNull() & (F.trim("id") != ""))   # filter 1
    if use_update_prefilter:
        df = df.filter(F.expr(f"update_date >= TIMESTAMP_NTZ'{start} 00:00:00'"))  # filter 2
    df = with_submission_date(df)
    df = df.filter(F.col("submitted_ts").isNotNull())                # filter 3
    df = add_time_columns(df)
    df = df.filter((F.col("submitted_date") >= F.lit(start).cast("date"))
                   & (F.col("submitted_date") < F.lit(end).cast("date")))  # filter 4
    df = add_ai_flags(df, rules)
    return df.select(*PAPER_COLUMNS)


def dedupe_papers(df: DataFrame) -> DataFrame:
    """Deterministic rule for conflicting duplicate ids: keep the row with the latest
    update_date, then the lexicographically largest title. Only applied when the QA
    step finds duplicate ids inside the study window."""
    w = Window.partitionBy("id").orderBy(F.desc("update_date"), F.desc("title"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


CATEGORY_SCHEMA = T.StructType([
    T.StructField("category_id", T.StringType()),
    T.StructField("category_name", T.StringType()),
    T.StructField("subject_group", T.StringType()),
    T.StructField("archive_id", T.StringType()),
    T.StructField("archive_name", T.StringType()),
    T.StructField("canonical_category_id", T.StringType()),
    T.StructField("is_alias", T.BooleanType()),
    T.StructField("alias_note", T.StringType()),
])


def load_categories(spark: SparkSession, path=CATEGORIES_CSV) -> DataFrame:
    return spark.read.csv(safe_path(path), header=True, schema=CATEGORY_SCHEMA)


def explode_categories(papers: DataFrame) -> DataFrame:
    """One row per (paper, listed category). explode_outer keeps papers whose
    category list is empty (they become 'Unknown' after the join)."""
    return papers.select(
        "id", "submitted_year", "submitted_month", "month_start",
        "ai_related", "mentions_genai_llm",
        F.explode_outer("category_list").alias("category_id"),
    )


def join_subjects(paper_cats: DataFrame, categories: DataFrame) -> DataFrame:
    """LEFT join with the small taxonomy table (broadcast). Unmatched codes are kept
    and labelled 'Unknown' instead of being dropped by an inner join."""
    dim = categories.select("category_id", "canonical_category_id", "subject_group")
    return (
        paper_cats.join(F.broadcast(dim), on="category_id", how="left")
        .withColumn("subject_group", F.coalesce("subject_group", F.lit("Unknown")))
        .withColumn("canonical_category_id", F.coalesce("canonical_category_id", "category_id"))
    )


def build_paper_subjects(papers: DataFrame, categories: DataFrame) -> DataFrame:
    """Unique (paper id, subject_group) rows. Several categories of the same group
    (or an alias and its canonical code) count once for that group."""
    joined = join_subjects(explode_categories(papers), categories)
    return joined.select(
        "id", "subject_group", "submitted_year", "submitted_month", "month_start",
        "ai_related", "mentions_genai_llm",
    ).dropDuplicates(["id", "subject_group"])


def annual_summary(papers: DataFrame, ytd_last_month=COMPARE_LAST_MONTH) -> DataFrame:
    """DataFrame-API groupBy: per-year totals for the full year and for Jan..ytd_last_month."""
    ai = F.col("ai_related").cast("int")
    genai = F.col("mentions_genai_llm").cast("int")
    in_ytd = F.col("submitted_month") <= ytd_last_month
    return (
        papers.groupBy("submitted_year")
        .agg(
            F.count("*").alias("total_papers"),
            F.sum(ai).alias("ai_papers"),
            F.sum(genai).alias("genai_llm_papers"),
            F.sum(F.col("is_ai_category").cast("int")).alias("ai_category_papers"),
            F.sum(in_ytd.cast("int")).alias("ytd_total_papers"),
            F.sum((in_ytd & F.col("ai_related")).cast("int")).alias("ytd_ai_papers"),
            F.sum((in_ytd & F.col("mentions_genai_llm")).cast("int")).alias("ytd_genai_llm_papers"),
            F.countDistinct("submitted_month").alias("months_covered"),
        )
        .orderBy("submitted_year")
    )


def multi_group_summary(paper_subjects: DataFrame) -> DataFrame:
    """Share of AI-related papers whose categories span >= 2 distinct subject groups.
    Descriptive only: it is about category labels, not co-authorship or diffusion."""
    per_paper = (
        paper_subjects.filter(F.col("subject_group") != "Unknown")
        .groupBy("id", "submitted_year", "submitted_month", "ai_related")
        .agg(F.count("subject_group").alias("n_groups"))   # rows are unique per (id, group)
    )
    multi = F.col("n_groups") >= 2
    in_ytd = F.col("submitted_month") <= COMPARE_LAST_MONTH
    return (
        per_paper.groupBy("submitted_year")
        .agg(
            F.count("*").alias("papers_with_known_group"),
            F.sum(multi.cast("int")).alias("multi_group_papers"),
            F.sum(F.col("ai_related").cast("int")).alias("ai_papers"),
            F.sum((F.col("ai_related") & multi).cast("int")).alias("ai_multi_group_papers"),
            F.sum((in_ytd & F.col("ai_related")).cast("int")).alias("ytd_ai_papers"),
            F.sum((in_ytd & F.col("ai_related") & multi).cast("int")).alias("ytd_ai_multi_group_papers"),
        )
        .withColumn("ai_multi_group_share", F.col("ai_multi_group_papers") / F.col("ai_papers"))
        .withColumn("ytd_ai_multi_group_share",
                    F.col("ytd_ai_multi_group_papers") / F.nullif(F.col("ytd_ai_papers"), F.lit(0)))
        .orderBy("submitted_year")
    )


def crosslist_check(papers: DataFrame) -> DataFrame:
    """How often cs.LG papers are also listed in stat.ML, by half-year.
    Used to interpret the multi-group share (stat.ML belongs to Statistics)."""
    has = lambda c: F.array_contains("category_list", c)  # noqa: E731
    half = F.when(F.col("submitted_month") <= 6, "H1").otherwise("H2")
    return (papers.filter(has("cs.LG"))
            .groupBy("submitted_year", half.alias("half_year"))
            .agg(F.count("*").alias("cs_lg_papers"),
                 F.sum(has("stat.ML").cast("int")).alias("also_stat_ml"))
            .withColumn("share_also_stat_ml", F.col("also_stat_ml") / F.col("cs_lg_papers"))
            .orderBy("submitted_year", "half_year"))


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------------------
# Spark REST API (live UI) - used to record real execution metrics
# --------------------------------------------------------------------------------------
def rest_get(spark: SparkSession, path: str):
    sc = spark.sparkContext
    port = urlparse(sc.uiWebUrl).port
    url = f"http://localhost:{port}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.load(resp)


def sql_executions(spark, description=None):
    execs = rest_get(spark, "sql?details=true&planDescription=true&offset=0&length=100000")
    if description is not None:
        execs = [e for e in execs if e.get("description") == description]
    return execs


def scan_metrics(execution: dict) -> list:
    """Metrics of the file-scan / exchange / join nodes of one SQL execution."""
    wanted = ("Scan", "Exchange", "BroadcastHashJoin", "SortMergeJoin", "InMemoryTableScan")
    out = []
    for node in execution.get("nodes", []):
        if node["nodeName"].startswith(wanted):
            out.append({"nodeName": node["nodeName"],
                        "metrics": {m["name"]: m["value"] for m in node.get("metrics", [])}})
    return out


# --------------------------------------------------------------------------------------
# Stage A: load and data quality
# --------------------------------------------------------------------------------------
def run_quality_checks(raw: DataFrame, start=STUDY_START, end=STUDY_END) -> dict:
    """Most quality indicators in ONE aggregation over the pruned raw columns."""
    df = with_submission_date(raw.select(*NEEDED_COLUMNS))
    sub_date = F.to_date("submitted_ts")
    valid_id = F.col("id").isNotNull() & (F.trim("id") != "")
    in_window = valid_id & (sub_date >= F.lit(start).cast("date")) & (sub_date < F.lit(end).cast("date"))
    empty = lambda c: (F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("int")  # noqa: E731
    row = df.agg(
        F.count("*").alias("rows"),
        F.sum((~valid_id).cast("int")).alias("missing_or_empty_id"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum((F.col("v1_count") == 0).cast("int")).alias("missing_v1_entry"),
        F.sum((F.col("v1_count") > 1).cast("int")).alias("multiple_v1_entries"),
        F.sum((F.get(F.col("versions"), 0)["version"] != "v1").cast("int")).alias("first_versions_entry_not_v1"),
        F.sum((F.col("v1_created_raw").isNotNull() & F.col("submitted_ts").isNull()).cast("int"))
        .alias("v1_parse_failures"),
        # formatted inside Spark (UTC session time zone); collect() would otherwise
        # convert timestamps to the local time zone of the Python process
        F.date_format(F.min("submitted_ts"), "yyyy-MM-dd HH:mm:ss 'UTC'").alias("min_v1_ts"),
        F.date_format(F.max("submitted_ts"), "yyyy-MM-dd HH:mm:ss 'UTC'").alias("max_v1_ts"),
        F.date_format(F.min("update_date"), "yyyy-MM-dd").alias("min_update_date"),
        F.date_format(F.max("update_date"), "yyyy-MM-dd").alias("max_update_date"),
        F.sum((F.to_date("update_date") < sub_date).cast("int")).alias("update_date_before_v1_date"),
        F.sum(empty("title")).alias("missing_title"),
        F.sum(empty("abstract")).alias("missing_abstract"),
        F.sum(empty("categories")).alias("missing_categories"),
        F.sum(in_window.cast("int")).alias("rows_in_window"),
        F.countDistinct(F.when(in_window, F.col("id"))).alias("distinct_ids_in_window"),
        F.sum((in_window & (F.col("update_date") < F.lit(start).cast("timestamp_ntz"))).cast("int"))
        .alias("in_window_but_update_before_start"),
    ).collect()[0].asDict()
    row["duplicate_rows_total"] = row["rows"] - row["distinct_ids"]
    row["duplicate_rows_in_window"] = row["rows_in_window"] - row["distinct_ids_in_window"]
    return row


def monthly_coverage(raw: DataFrame) -> DataFrame:
    df = with_submission_date(raw.select("id", "versions"))
    return (df.filter(F.col("submitted_ts") >= F.lit("2019-01-01").cast("timestamp"))
            .groupBy(F.date_format("submitted_ts", "yyyy-MM").alias("v1_month"))
            .agg(F.count("*").alias("records"))
            .orderBy("v1_month"))


def duplicate_id_report(raw: DataFrame) -> DataFrame:
    """Details of duplicated ids.

    Optimised after the first full run: grouping the full rows (with abstracts) by
    id shuffled 2.3 GiB. Now only the id column is shuffled to find duplicated ids,
    and the few matching rows are fetched with a broadcast LEFT SEMI join.
    """
    dup_ids = raw.select("id").groupBy("id").count().filter("count > 1").select("id")
    df = raw.select(*NEEDED_COLUMNS).join(F.broadcast(dup_ids), on="id", how="left_semi")
    df = add_time_columns(with_submission_date(df))
    return (df.groupBy("id")
            .agg(F.count("*").alias("n_rows"),
                 F.countDistinct("title", "abstract", "categories", "update_date").alias("n_distinct_versions"),
                 F.min("submitted_date").alias("v1_date"),
                 F.collect_set("categories").alias("category_variants"),
                 F.max("update_date").alias("latest_update_date"))
            .filter("n_rows > 1")
            .withColumn("category_variants", F.concat_ws(" | ", "category_variants"))
            .orderBy("id"))


# --------------------------------------------------------------------------------------
# Performance evidence
# --------------------------------------------------------------------------------------
def lazy_demo_transformations(papers: DataFrame) -> DataFrame:
    # Transformations only: nothing is computed when these lines run.
    return (
        papers
        .filter(F.col("submitted_year") == 2025)                    # transformation
        .filter(F.col("ai_related"))                                # transformation
        .withColumn("title_words",                                  # transformation
                    F.size(F.split(F.trim("title"), r"\s+")))
    )


def lazy_demo_action(demo: DataFrame):
    # Action: collect() a small aggregate that USES the new column, so Spark
    # really has to compute title_words (a plain count() could prune it away).
    return (demo.groupBy("mentions_genai_llm")
            .agg(F.count("*").alias("papers"),
                 F.round(F.avg("title_words"), 2).alias("avg_title_words"))
            .orderBy("mentions_genai_llm")
            .collect())


def run_lazy_demo(spark, papers_path, paths: Paths) -> dict:
    sc = spark.sparkContext
    tracker = sc.statusTracker()
    group = "lazy-demo"
    papers = spark.read.parquet(papers_path)  # schema/file listing (metadata) happens here
    sc.setJobGroup(group, "Lazy evaluation demo")

    jobs_0 = len(tracker.getJobIdsForGroup(group))
    t0 = time.perf_counter()
    demo = lazy_demo_transformations(papers)
    define_secs = time.perf_counter() - t0
    jobs_1 = len(tracker.getJobIdsForGroup(group))

    plan = save_plan(demo, paths.plans / "lazy_demo_plan.txt")

    t1 = time.perf_counter()
    rows = lazy_demo_action(demo)
    action_secs = time.perf_counter() - t1
    jobs_2 = len(tracker.getJobIdsForGroup(group))
    sc.setLocalProperty("spark.jobGroup.id", None)
    sc.setLocalProperty("spark.job.description", None)

    result = {
        "jobs_before": jobs_0,
        "jobs_after_transformations": jobs_1,
        "jobs_after_action": jobs_2,
        "define_seconds": round(define_secs, 4),
        "action_seconds": round(action_secs, 3),
        "action_result": [r.asDict() for r in rows],
    }
    code = inspect.getsource(lazy_demo_transformations) + "\n" + inspect.getsource(lazy_demo_action)
    md = [
        "# Actions vs transformations - recorded output",
        "",
        "Code (from `pipeline.py`):",
        "", "```python", code.rstrip(), "```", "",
        f"* Spark jobs in group `{group}` before: **{jobs_0}**",
        f"* after defining 2 filters + withColumn (transformations): **{jobs_1}** "
        f"({define_secs * 1000:.1f} ms to build the plan)",
        f"* after `collect()` on the aggregate (action): **{jobs_2}** ({action_secs:.2f} s)",
        "",
        "Action result:", "",
        "| mentions_genai_llm | papers | avg_title_words |", "|---|---:|---:|",
    ]
    md += [f"| {r['mentions_genai_llm']} | {r['papers']:,} | {r['avg_title_words']} |" for r in result["action_result"]]
    md += ["", "Plan printed by `demo.explain('formatted')` before the action (no job is run):", "",
           "```text", plan.rstrip(), "```"]
    (paths.reports / "lazy_demo.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    return result


def cache_query(df: DataFrame):
    # Repeated analysis action: monthly AI and GenAI/LLM counts (consumes the regex-derived flags).
    return (df.groupBy("month_start")
            .agg(F.count("*").alias("papers"),
                 F.sum(F.col("ai_related").cast("int")).alias("ai_papers"),
                 F.sum(F.col("mentions_genai_llm").cast("int")).alias("genai_llm_papers"))
            .orderBy("month_start")
            .collect())


def run_cache_experiment(spark, raw, rules, paths: Paths, repeats=3, capture=None) -> dict:
    """Same action on the cleaned-papers DataFrame built from the raw Parquet
    (scan + filters + regex flags), first without cache, then with .cache()."""
    papers = clean_papers(raw, rules)

    def timed_runs(label):
        times, result = [], None
        for i in range(repeats):
            spark.sparkContext.setJobDescription(f"Cache experiment: {label} run {i + 1}")
            t = time.perf_counter()
            result = cache_query(papers)
            times.append(round(time.perf_counter() - t, 3))
        return times, result

    uncached_times, uncached_result = timed_runs("uncached")
    save_plan(papers.groupBy("month_start").count(), paths.plans / "cache_query_uncached_plan.txt")

    papers.cache()
    spark.sparkContext.setJobDescription("Cache experiment: fill cache (count)")
    t = time.perf_counter()
    cached_rows = papers.count()              # action that materialises every cached partition
    fill_secs = round(time.perf_counter() - t, 3)
    storage = rest_get(spark, "storage/rdd")
    if capture:
        capture()
    save_plan(papers.groupBy("month_start").count(), paths.plans / "cache_query_cached_plan.txt")

    cached_times, cached_result = timed_runs("cached")
    same = [r.asDict() for r in uncached_result] == [r.asDict() for r in cached_result]
    papers.unpersist(blocking=True)
    spark.sparkContext.setJobDescription(None)

    result = {
        "cached_dataframe": "papers = clean_papers(raw): Parquet scan + 4 filters + withColumn + regex AI flags",
        "query": "groupBy(month_start).agg(count, sum(ai_related), sum(mentions_genai_llm)).collect()",
        "repeats": repeats,
        "uncached_seconds": uncached_times,
        "uncached_median": statistics.median(uncached_times),
        "cache_fill_seconds": fill_secs,
        "cached_rows": cached_rows,
        "cached_seconds": cached_times,
        "cached_median": statistics.median(cached_times),
        "speedup_median": round(statistics.median(uncached_times) / statistics.median(cached_times), 2),
        "results_identical": same,
        "storage_after_fill": [{"rdd_id": s.get("id"), "name": s.get("name", "")[:80] + "...",
                                **{k: s.get(k) for k in ("storageLevel", "numPartitions",
                                                         "numCachedPartitions", "memoryUsed", "diskUsed")}}
                               for s in storage],
        "unpersisted": True,
    }
    return result


def run_partition_pruning(spark, papers_path, paths: Paths) -> dict:
    """Read the *root* of the year-partitioned output and filter on the partition column."""
    def query(df):
        return (df.groupBy("submitted_month")
                .agg(F.count("*").alias("papers"), F.avg(F.col("ai_related").cast("int")).alias("ai_share"))
                .orderBy("submitted_month"))

    out = {}
    for label, year_filter in (("all_years", None), ("year_2025", 2025)):
        df = spark.read.parquet(papers_path)               # dataset root, not a year sub-folder
        if year_filter is not None:
            df = df.filter(F.col("submitted_year") == year_filter)
        q = query(df)
        desc = f"Partition pruning check: {label}"
        save_plan(q, paths.plans / f"partition_pruning_{label}.txt")
        spark.sparkContext.setJobDescription(desc)
        t = time.perf_counter()
        rows = q.collect()
        secs = round(time.perf_counter() - t, 3)
        spark.sparkContext.setJobDescription(None)
        ex = sql_executions(spark, desc)
        out[label] = {"seconds": secs, "result_rows": len(rows),
                      "scan_metrics": scan_metrics(ex[-1]) if ex else None,
                      "sql_execution_id": ex[-1]["id"] if ex else None}
    return out


def spot_check_sample(raw: DataFrame, rules: dict, per_stratum=6) -> DataFrame:
    """Small, deterministic sample of matched / unmatched records for manual review."""
    papers = add_ai_flags(add_time_columns(with_submission_date(raw.select(*NEEDED_COLUMNS))), rules)
    papers = papers.filter((F.col("submitted_date") >= F.lit(STUDY_START).cast("date"))
                           & (F.col("submitted_date") < F.lit(STUDY_END).cast("date")))
    norm = F.regexp_replace(F.concat_ws(" ", "title", "abstract"), r"[\s\-‐-―_]+", " ")
    # GenAI match that comes only from the bare "LLM" acronym (no GenAI phrase)
    acronym_only = F.col("mentions_genai_llm") & ~norm.rlike(rules["genai_llm_phrase_regex"])
    ambiguous = norm.rlike(r"(?i)\b(?:transformers?|diffusion|agents?|attention)\b")
    stratum = (F.when(F.col("is_ai_category") & F.col("mentions_ai_terms"), "AI: category + text")
               .when(F.col("is_ai_category"), "AI: category only")
               .when(acronym_only, "AI: text only, GenAI via 'LLM' acronym only")
               .when(F.col("mentions_ai_terms"), "AI: text only")
               .when(ambiguous, "Not AI: has ambiguous word (transformer/diffusion/agent/attention)")
               .otherwise("Not AI: other"))
    w = Window.partitionBy("stratum").orderBy(F.xxhash64("id"))
    return (papers.withColumn("stratum", stratum)
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") <= per_stratum)
            .select("stratum", "id", "title", "categories",
                    F.regexp_extract(norm, rules["ai_term_regex"], 0).alias("first_ai_term"),
                    F.regexp_extract(norm, rules["genai_llm_regex"], 0).alias("first_genai_term"),
                    "is_ai_category", "mentions_ai_terms", "mentions_genai_llm", "ai_related",
                    F.substring("abstract", 1, 300).alias("abstract_start"))
            .orderBy("stratum", "id"))


# --------------------------------------------------------------------------------------
# Environment record
# --------------------------------------------------------------------------------------
def total_ram_gb():
    try:
        if os.name == "nt":
            import ctypes

            class MEMSTAT(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]
            st = MEMSTAT()
            st.dwLength = ctypes.sizeof(MEMSTAT)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
            return round(st.ullTotalPhys / 2**30, 1)
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except Exception:  # informational only
        return None


def environment_info(spark: SparkSession) -> dict:
    sc = spark.sparkContext
    conf = dict(sc.getConf().getAll())
    keys = ["spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions",
            "spark.sql.adaptive.enabled", "spark.sql.session.timeZone", "spark.eventLog.enabled",
            "spark.eventLog.dir", "spark.hadoop.fs.file.impl"]
    runtime = {k: spark.conf.get(k) for k in ["spark.sql.adaptive.enabled", "spark.sql.adaptive.coalescePartitions.enabled",
                                              "spark.sql.autoBroadcastJoinThreshold", "spark.sql.files.maxPartitionBytes",
                                              "spark.sql.parquet.filterPushdown", "spark.sql.ansi.enabled"]}
    return {
        "run_started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "os": platform.platform(),
        "python": platform.python_version(),
        "java": spark._jvm.System.getProperty("java.version"),
        "spark": spark.version,
        "mode": "single-machine local mode (driver and executor threads in one JVM); not a multi-node cluster",
        "default_parallelism": sc.defaultParallelism,
        "cpu_count": os.cpu_count(),
        "ram_gb": total_ram_gb(),
        "application_id": sc.applicationId,
        "ui_web_url": sc.uiWebUrl,
        "conf": {k: conf.get(k) for k in keys},
        "sql_conf": runtime,
        "shuffle_partitions_reason": ("48 = 2 x 24 local cores. The default 200 would create many tiny "
                                      "tasks for the small shuffles here; AQE coalesces further."),
    }


# --------------------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dev", action="store_true", help="development run on shard 0 only")
    parser.add_argument("--skip-cache", action="store_true", help="skip the optional cache experiment")
    parser.add_argument("--capture-ui", action="store_true", help="screenshot the live Spark UI at the end")
    args = parser.parse_args()

    paths = Paths(args.dev)
    setup_logging(paths.reports / "run_log.txt")
    rules = load_rules()
    t_start = time.perf_counter()

    spark = build_spark("arxiv-ai-pipeline" + ("-dev" if args.dev else ""),
                        shuffle_partitions=SHUFFLE_PARTITIONS, event_log_dir=EVENT_LOG_DIR)
    env = environment_info(spark)
    env["run_mode"] = "dev (shard 0 only)" if args.dev else "full (all 11 shards)"
    write_json(paths.reports / "environment.json", env)
    log.info(f"Spark {env['spark']} | Java {env['java']} | Python {env['python']} | {env['os']}")
    log.info(f"Application {env['application_id']} | UI {env['ui_web_url']} | run mode: {env['run_mode']}")

    capture = None
    if args.capture_ui:
        from capture_screenshots import LiveUICapture
        capture = LiveUICapture(spark, paths.screenshots)

    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "passed": bool(ok), "detail": str(detail)})
        log.info(f"  check {'PASS' if ok else 'FAIL'}: {name} {detail}")

    # ---------------------------------------------------------------- Stage A: load
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    shard_files = sorted(RAW_DIR.glob("*.parquet"))
    if args.dev:
        shard_files = shard_files[:1]
    input_bytes = sum(f.stat().st_size for f in shard_files)
    with stage(spark, "A1_load", "A1 Load raw Parquet + row count"):
        raw = spark.read.parquet(*[safe_path(f) for f in shard_files])
        (paths.reports / "raw_schema.txt").write_text(raw._jdf.schema().treeString(), encoding="utf-8")
        raw_rows = raw.count()
    log.info(f"Input: {len(shard_files)} Parquet files, {input_bytes:,} bytes ({input_bytes / 1e9:.3f} GB), "
             f"{raw_rows:,} rows, {raw.rdd.getNumPartitions()} input partitions")
    if not args.dev:
        check("all 11 shards read", len(shard_files) == 11, f"{len(shard_files)} files")
        check("input size > 1 GB", input_bytes > 1e9, f"{input_bytes:,} bytes")
        check("Spark row count == source-declared rows", raw_rows == EXPECTED_ROWS,
              f"{raw_rows:,} vs {EXPECTED_ROWS:,}")
        manifest["locally_measured"]["spark_row_count"] = raw_rows
        manifest["locally_measured"]["spark_row_count_measured_utc"] = env["run_started_utc"]
        manifest["locally_measured"]["spark_application_id"] = env["application_id"]
        write_json(MANIFEST, manifest)

    with stage(spark, "A2_quality", "A2 Data quality checks (QA aggregation, monthly coverage, duplicate ids)"):
        qa = run_quality_checks(raw)
        coverage = monthly_coverage(raw).toPandas()
        dups = duplicate_id_report(raw).toPandas()
    qa["input_files"] = len(shard_files)
    qa["input_bytes"] = input_bytes
    write_json(paths.reports / "data_quality.json", qa)
    coverage.to_csv(paths.reports / "monthly_coverage.csv", index=False)
    dups.to_csv(paths.reports / "duplicate_ids.csv", index=False)
    log.info("QA: " + json.dumps(qa, default=str))
    check("no v1 parse failures", qa["v1_parse_failures"] == 0, qa["v1_parse_failures"])
    check("update_date pre-filter is lossless (no in-window paper with update_date < start)",
          qa["in_window_but_update_before_start"] == 0, qa["in_window_but_update_before_start"])
    check("snapshot covers the whole study window (max v1 date >= window end)",
          str(qa["max_v1_ts"])[:10] >= STUDY_END or args.dev, f"max v1 = {qa['max_v1_ts']}")
    use_prefilter = qa["in_window_but_update_before_start"] == 0
    needs_dedupe = qa["duplicate_rows_in_window"] > 0
    log.info(f"Duplicate id rows: {qa['duplicate_rows_total']} overall, {qa['duplicate_rows_in_window']} "
             f"inside the study window -> dedupe step {'ON' if needs_dedupe else 'not needed'}")

    # ---------------------------------------------------------------- Stage B: clean + write
    papers_path = paths.out("papers_clean")
    with stage(spark, "B1_clean_write", "B1 Clean papers (filters + withColumn + AI flags) and write Parquet by year"):
        papers = clean_papers(raw, rules, use_update_prefilter=use_prefilter)
        if needs_dedupe:
            papers = dedupe_papers(papers)
        save_plan(papers, paths.plans / "B1_clean_papers.txt")
        (papers.repartition("submitted_year")      # one file per year folder instead of ~25 x 7 small files
         .write.mode("overwrite").partitionBy("submitted_year").parquet(papers_path))

    with stage(spark, "B2_read_back", "B2 Read back cleaned papers and register view"):
        papers = spark.read.parquet(papers_path)
        papers.createOrReplaceTempView("papers")
        papers_stats = papers.agg(
            F.count("*").alias("rows"), F.countDistinct("id").alias("ids"),
            F.min("submitted_date").alias("min_date"), F.max("submitted_date").alias("max_date"),
            F.sum(F.col("ai_related").cast("int")).alias("ai"),
            F.sum(F.col("mentions_genai_llm").cast("int")).alias("genai"),
            F.sum((~F.col("text_available")).cast("int")).alias("no_text"),
            F.sum((F.size("category_list") == 0).cast("int")).alias("no_categories"),
        ).collect()[0].asDict()
    log.info(f"Cleaned papers: {papers_stats}")
    check("cleaned rows == distinct in-window ids from QA", papers_stats["rows"] == qa["distinct_ids_in_window"],
          f"{papers_stats['rows']:,} vs {qa['distinct_ids_in_window']:,}")
    check("ids unique in cleaned table", papers_stats["rows"] == papers_stats["ids"])
    check("dates inside [start, end)", str(papers_stats["min_date"]) >= STUDY_START
          and str(papers_stats["max_date"]) < STUDY_END,
          f"{papers_stats['min_date']} .. {papers_stats['max_date']}")
    year_dirs = sorted(p.name for p in Path(papers_path).iterdir() if p.name.startswith("submitted_year="))
    files_per_year = {d: len(list((Path(papers_path) / d).glob("*.parquet"))) for d in year_dirs}
    log.info(f"papers_clean layout: {files_per_year}")

    with stage(spark, "B3_join", "B3 Explode categories + broadcast LEFT join with taxonomy + dedupe (id, subject_group)"):
        categories = load_categories(spark)
        cat_stats = categories.agg(F.count("*").alias("rows"), F.countDistinct("category_id").alias("ids")).collect()[0]
        paper_cats = explode_categories(papers)
        joined = join_subjects(paper_cats, categories)
        save_plan(joined, paths.plans / "B3_join_subjects.txt")
        exploded_rows = paper_cats.count()
        join_stats = joined.agg(
            F.count("*").alias("rows"),
            F.sum((F.col("subject_group") == "Unknown").cast("int")).alias("unknown_rows")).collect()[0]
        unknown = (joined.filter(F.col("subject_group") == "Unknown")
                   .groupBy("category_id").agg(F.count("*").alias("rows"), F.countDistinct("id").alias("papers"))
                   .orderBy(F.desc("rows")).toPandas())
        # Written partitioned by year as well, so SQL 2 (2025 + 2026 only) can prune
        # the other year folders. repartition -> one file per year folder.
        subjects = build_paper_subjects(papers, categories).repartition("submitted_year")
        save_plan(subjects, paths.plans / "B3_paper_subjects.txt")
        subjects.write.mode("overwrite").partitionBy("submitted_year").parquet(paths.out("paper_subjects"))
    unknown.to_csv(paths.reports / "unknown_categories.csv", index=False)
    check("taxonomy key category_id is unique", cat_stats["rows"] == cat_stats["ids"],
          f"{cat_stats['rows']} rows / {cat_stats['ids']} ids")
    check("LEFT join did not multiply rows", join_stats["rows"] == exploded_rows,
          f"{exploded_rows:,} exploded -> {join_stats['rows']:,} joined")
    log.info(f"Unknown category rows: {join_stats['unknown_rows']:,} ({len(unknown)} distinct codes)")

    with stage(spark, "B4_groupby", "B4 DataFrame groupBy: annual summary + multi-group share"):
        subjects = spark.read.parquet(paths.out("paper_subjects"))
        subjects.createOrReplaceTempView("paper_subjects")
        subj_stats = subjects.agg(F.count("*").alias("rows"), F.countDistinct("id").alias("ids"),
                                  F.countDistinct("id", "subject_group").alias("pairs")).collect()[0]
        annual = annual_summary(papers)
        save_plan(annual, paths.plans / "B4_annual_summary_groupby.txt")
        annual_pd = annual.toPandas()
        multi = multi_group_summary(subjects)
        multi_pd = multi.toPandas()
        crosslist_check(papers).toPandas().to_csv(paths.reports / "cs_lg_stat_ml_crosslist.csv", index=False)
        annual.write.mode("overwrite").parquet(paths.out("annual_summary"))
        multi.write.mode("overwrite").parquet(paths.out("multi_group_by_year"))
    check("paper_subjects unique on (id, subject_group)", subj_stats["rows"] == subj_stats["pairs"])
    check("every cleaned paper has >= 1 subject row", subj_stats["ids"] == papers_stats["rows"],
          f"{subj_stats['ids']:,} vs {papers_stats['rows']:,}")

    # Two clearly separated tables: complete years only, and Jan..Sep of every year.
    full_years = annual_pd[annual_pd["submitted_year"] < YTD_YEAR].copy()
    full_years["ai_share"] = full_years["ai_papers"] / full_years["total_papers"]
    full_years["genai_llm_share"] = full_years["genai_llm_papers"] / full_years["total_papers"]
    full_years["ai_papers_pct_change"] = 100 * full_years["ai_papers"].pct_change()
    full_years["total_papers_pct_change"] = 100 * full_years["total_papers"].pct_change()
    full_years["ai_share_change_pp"] = 100 * full_years["ai_share"].diff()
    full_years.insert(1, "period", "full year (Jan-Dec)")
    full_years[["submitted_year", "period", "total_papers", "ai_papers", "ai_share", "genai_llm_papers",
                "genai_llm_share", "ai_category_papers", "months_covered", "ai_papers_pct_change",
                "total_papers_pct_change", "ai_share_change_pp"]].to_csv(
        paths.reports / "annual_full_years.csv", index=False)

    ytd = annual_pd[["submitted_year", "ytd_total_papers", "ytd_ai_papers", "ytd_genai_llm_papers"]].copy()
    ytd.insert(1, "period", f"Jan-Sep (months 1-{COMPARE_LAST_MONTH})")
    ytd["ytd_ai_share"] = ytd["ytd_ai_papers"] / ytd["ytd_total_papers"]
    ytd["ytd_genai_llm_share"] = ytd["ytd_genai_llm_papers"] / ytd["ytd_total_papers"]
    ytd["ytd_ai_papers_pct_change"] = 100 * ytd["ytd_ai_papers"].pct_change()
    ytd["ytd_total_papers_pct_change"] = 100 * ytd["ytd_total_papers"].pct_change()
    ytd["ytd_ai_share_change_pp"] = 100 * ytd["ytd_ai_share"].diff()
    ytd["ytd_genai_llm_share_change_pp"] = 100 * ytd["ytd_genai_llm_share"].diff()
    ytd.to_csv(paths.reports / "same_months_jan_sep_by_year.csv", index=False)
    multi_pd.to_csv(paths.reports / "multi_group_by_year.csv", index=False)

    # ---------------------------------------------------------------- Stage C: SQL
    with stage(spark, "C1_sql_monthly", "C1 SQL 1: monthly AI research trends"):
        sql1 = spark.sql(read_sql("monthly_trends.sql"))
        save_plan(sql1, paths.plans / "C1_sql1_monthly_trends.txt")
        sql1.write.mode("overwrite").parquet(paths.out("sql1_monthly_trends"))
    with stage(spark, "C2_sql_fields", "C2 SQL 2: 2025 vs 2026 cross-disciplinary comparison"):
        sql2 = spark.sql(read_sql("field_comparison.sql"), args={"last_month": COMPARE_LAST_MONTH})
        save_plan(sql2, paths.plans / "C2_sql2_field_comparison.txt")
        sql2.write.mode("overwrite").parquet(paths.out("sql2_field_comparison"))

    # ---------------------------------------------------------------- Stage D: read back + verify
    with stage(spark, "D1_read_back_verify", "D1 Read back SQL outputs and verify"):
        monthly = spark.read.parquet(paths.out("sql1_monthly_trends")).orderBy("month_start").toPandas()
        fields = spark.read.parquet(paths.out("sql2_field_comparison")).orderBy(F.desc("total_2026")).toPandas()
        ytd_subject_totals = (subjects.filter(F.col("submitted_month") <= COMPARE_LAST_MONTH)
                              .groupBy("submitted_year").count().toPandas().set_index("submitted_year")["count"])
        ytd_paper_totals = (papers.filter(F.col("submitted_month") <= COMPARE_LAST_MONTH)
                            .groupBy("submitted_year").count().toPandas().set_index("submitted_year")["count"])
        annual_back = spark.read.parquet(paths.out("annual_summary")).count()
        multi_back = spark.read.parquet(paths.out("multi_group_by_year")).count()
    monthly.to_csv(paths.reports / "monthly_ai_trends.csv", index=False)
    fields.to_csv(paths.reports / "field_comparison_2025_2026.csv", index=False)

    months = list(monthly["month_start"].astype(str))
    expected_months = [f"{y}-{m:02d}-01" for y in range(2020, 2027) for m in range(1, 13)
                       if f"{y}-{m:02d}-01" < STUDY_END]
    if not args.dev:
        check("SQL1: months contiguous and complete for the window", months == expected_months,
              f"{len(months)} months")
    check("SQL1: sum(total_papers) == cleaned papers", monthly["total_papers"].sum() == papers_stats["rows"])
    check("SQL1: sum(ai_papers) == cleaned AI papers", monthly["ai_papers"].sum() == papers_stats["ai"])
    check("SQL1: genai <= ai <= total each month",
          ((monthly["genai_llm_papers"] <= monthly["ai_papers"]) & (monthly["ai_papers"] <= monthly["total_papers"])).all())
    check("SQL1: shares within [0, 1]", monthly[["ai_share", "genai_llm_share"]].stack().between(0, 1).all())
    sql1_by_year = monthly.groupby("submitted_year")[["total_papers", "ai_papers"]].sum()
    gb_by_year = annual_pd.set_index("submitted_year")[["total_papers", "ai_papers"]]
    check("DataFrame groupBy totals == SQL1 totals per year",
          sql1_by_year.sort_index().equals(gb_by_year.sort_index().astype(sql1_by_year.dtypes.to_dict())))
    if not args.dev:
        check("SQL2: 2025 group totals == unique (id, group) rows Jan-Sep 2025",
              fields["total_2025"].sum() == ytd_subject_totals.get(2025))
        check("SQL2: 2026 group totals == unique (id, group) rows Jan-Sep 2026",
              fields["total_2026"].sum() == ytd_subject_totals.get(2026))
        check("SQL2: group totals >= paper totals (multi-group papers counted per group)",
              fields["total_2026"].sum() >= ytd_paper_totals.get(2026))
    check("SQL2: ai <= total per group",
          ((fields["ai_2025"] <= fields["total_2025"]) & (fields["ai_2026"] <= fields["total_2026"])).all())
    check("SQL2: shares within [0, 1]",
          fields[["ai_share_2025", "ai_share_2026"]].stack().dropna().between(0, 1).all())
    check("SQL2: pct change NULL when base is 0 (no infinity)",
          not fields["ai_papers_pct_change"].isin([float("inf"), float("-inf")]).any())
    check("annual/multi-group outputs read back", annual_back == len(annual_pd) and multi_back == len(multi_pd))

    # ---------------------------------------------------------------- Performance evidence
    perf = {}
    with stage(spark, "P1_partition_pruning", "P1 Partition pruning check on year-partitioned output"):
        perf["partition_pruning"] = run_partition_pruning(spark, papers_path, paths)
    with stage(spark, "P2_lazy_demo", "P2 Actions vs transformations demo"):
        perf["lazy_demo"] = run_lazy_demo(spark, papers_path, paths)
    check("lazy demo: no job while defining transformations",
          perf["lazy_demo"]["jobs_after_transformations"] == perf["lazy_demo"]["jobs_before"])
    check("lazy demo: action triggered >= 1 job",
          perf["lazy_demo"]["jobs_after_action"] > perf["lazy_demo"]["jobs_after_transformations"])
    if not args.skip_cache:
        with stage(spark, "P3_cache", "P3 Cache experiment (optional bonus)"):
            perf["cache_experiment"] = run_cache_experiment(
                spark, raw, rules, paths, capture=capture.storage if capture else None)
        check("cache experiment: cached and uncached results identical",
              perf["cache_experiment"]["results_identical"])

    with stage(spark, "P4_spot_check", "P4 Spot-check sample of AI rule matches"):
        spot_check_sample(raw, rules).toPandas().to_csv(paths.reports / "spot_check_sample.csv", index=False)

    perf["timings"] = TIMINGS
    perf["input_files"] = len(shard_files)
    perf["input_bytes_on_disk"] = input_bytes
    perf["papers_clean_files_per_year"] = files_per_year

    # Real metrics from the Spark REST API of this application.
    execs = sql_executions(spark)
    write_json(paths.rest / "sql_executions.json", execs)
    write_json(paths.rest / "stages.json", rest_get(spark, "stages"))
    write_json(paths.rest / "jobs.json", rest_get(spark, "jobs"))
    perf["sql_executions"] = [{"id": e["id"], "description": e["description"], "status": e["status"],
                               "duration_ms": e["duration"], "scan_and_exchange_nodes": scan_metrics(e)}
                              for e in execs]
    write_json(paths.reports / "performance_metrics.json", perf)
    write_json(paths.reports / "validation_checks.json", checks)

    if capture:
        capture.final_pages()

    total = time.perf_counter() - t_start
    failed = [c for c in checks if not c["passed"]]
    log.info(f"Pipeline finished in {total:.1f} s; {len(checks) - len(failed)}/{len(checks)} checks passed")
    log.info(f"Event log: spark-events/{spark.sparkContext.applicationId}")
    spark.stop()

    from make_charts import make_charts
    make_charts(paths.reports)
    if failed:
        raise SystemExit(f"{len(failed)} validation check(s) failed: {[c['check'] for c in failed]}")
    log.info("SUCCESS: all validation checks passed")


if __name__ == "__main__":
    main()
