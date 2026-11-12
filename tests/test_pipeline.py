"""Small correctness tests for pipeline.py.

All rows below are hand-made TEST DATA (not arXiv records). They check the
logic only and are never used as analysis results or as evidence of data size.

Run with either:
    python tests/test_pipeline.py
    python -m pytest -q tests
"""

import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

import pipeline as P  # noqa: E402
from spark_session import build_spark  # noqa: E402

_SPARK = None


def spark():
    global _SPARK
    if _SPARK is None:
        _SPARK = build_spark("arxiv-pipeline-tests", master="local[2]", shuffle_partitions=4,
                             driver_memory="1g")
    return _SPARK


RAW_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("abstract", T.StringType()),
    T.StructField("categories", T.StringType()),
    T.StructField("versions", T.ArrayType(T.StructType([
        T.StructField("version", T.StringType()),
        T.StructField("created", T.StringType()),
    ]))),
    T.StructField("update_date", T.TimestampNTZType()),
])


def raw_df(rows):
    """rows: (id, title, abstract, categories, versions, update_date)"""
    return spark().createDataFrame(rows, RAW_SCHEMA)


def v(*pairs):
    return [Row(version=a, created=b) for a, b in pairs]


UPD = dt.datetime(2026, 10, 1)
RULES = P.load_rules()


def by_id(df):
    return {r["id"]: r for r in df.collect()}


# ----------------------------------------------------------------------------- dates
def test_v1_date_parsing_and_window():
    rows = [
        # v1 is selected by its label even when it is not the first element
        ("a", "t", "x", "math.CO", v(("v2", "Mon, 3 Feb 2020 10:00:00 GMT"), ("v1", "Wed, 1 Jan 2020 00:00:00 GMT")), UPD),
        ("before", "t", "x", "math.CO", v(("v1", "Tue, 31 Dec 2019 23:59:59 GMT")), UPD),
        ("last_in", "t", "x", "math.CO", v(("v1", "Wed, 30 Sep 2026 23:59:59 GMT")), UPD),
        ("after", "t", "x", "math.CO", v(("v1", "Thu, 1 Oct 2026 00:00:00 GMT")), UPD),
        ("bad", "t", "x", "math.CO", v(("v1", "not a date")), UPD),
        ("no_v1", "t", "x", "math.CO", v(("v2", "Mon, 3 Feb 2020 10:00:00 GMT")), UPD),
        ("two_digit_day", "t", "x", "math.CO", v(("v1", "Fri, 01 May 2020 01:52:14 GMT")), UPD),
    ]
    parsed = by_id(P.add_time_columns(P.with_submission_date(raw_df(rows))))
    assert parsed["a"]["submitted_date"] == dt.date(2020, 1, 1)
    assert parsed["two_digit_day"]["submitted_date"] == dt.date(2020, 5, 1)
    # invalid / missing v1 -> NULL, never filled from update_date
    assert parsed["bad"]["submitted_ts"] is None
    assert parsed["no_v1"]["submitted_ts"] is None and parsed["no_v1"]["v1_count"] == 0

    kept = set(by_id(P.clean_papers(raw_df(rows), RULES)))
    assert kept == {"a", "last_in", "two_digit_day"}, kept   # start inclusive, end exclusive


def test_update_prefilter_does_not_change_result_when_lossless():
    rows = [("p1", "t", "x", "cs.LG", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), dt.datetime(2020, 1, 6)),
            ("old", "t", "x", "cs.LG", v(("v1", "Mon, 6 Jan 2014 09:00:00 GMT")), dt.datetime(2015, 1, 1))]
    with_pf = sorted(by_id(P.clean_papers(raw_df(rows), RULES, use_update_prefilter=True)))
    without = sorted(by_id(P.clean_papers(raw_df(rows), RULES, use_update_prefilter=False)))
    assert with_pf == without == ["p1"]


# ----------------------------------------------------------------------------- AI rules
def flags(title, abstract="", categories="math.CO"):
    df = raw_df([("x", title, abstract, categories, v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), UPD)])
    return P.clean_papers(df, RULES).collect()[0]


def test_ai_terms_and_ambiguous_words():
    assert flags("A deep-learning approach")["mentions_ai_terms"]
    assert flags("Graph Neural Networks for X")["mentions_ai_terms"]
    assert flags("Self-supervised learning of Y")["mentions_ai_terms"]
    # ambiguous words alone must not match
    r = flags("Transformer substation diffusion of heat with agent-based attention")
    assert not r["mentions_ai_terms"] and not r["ai_related"]
    # no bare 'ai' substring matching
    assert not flags("Domain walls in trained chains", "We maintain AI-free notation.")["mentions_ai_terms"]


def test_genai_llm_rules():
    assert flags("Large-Language-Models as tutors")["mentions_genai_llm"]
    assert flags("Evaluating ChatGPT on exams")["mentions_genai_llm"]
    assert flags("GPT-4 for proofs")["mentions_genai_llm"]
    assert flags("Generative AI in classrooms")["mentions_genai_llm"]
    r = flags("LLMs can reason?")
    assert r["mentions_genai_llm"] and r["mentions_ai_terms"] and r["ai_related"]
    # acronym is case sensitive; lower-case token does not count
    assert not flags("the llm parameter of the fit")["mentions_genai_llm"]


def test_category_flag_and_non_ai_kept():
    r = flags("Some title", categories="cs.LG stat.ME")
    assert r["is_ai_category"] and r["ai_related"] and not r["mentions_ai_terms"]
    r = flags("Prime gaps", categories="math.NT")
    assert not r["ai_related"]      # non-AI paper is still a row (stays in denominator)


# ----------------------------------------------------------------------------- join / subjects
def test_subject_join_alias_unknown_and_dedupe():
    rows = [
        ("alias", "t", "x", "math.NA cs.NA", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), UPD),
        ("cross", "t", "x", "cs.LG q-bio.NC cs.AI", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), UPD),
        ("unknown", "t", "x", "foo.BAR", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), UPD),
        ("empty", "t", "x", "  ", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), UPD),
    ]
    papers = P.clean_papers(raw_df(rows), RULES)
    cats = P.load_categories(spark())
    keys = cats.groupBy("category_id").count().filter("count > 1").count()
    assert keys == 0, "taxonomy keys must be unique"

    exploded = P.explode_categories(papers)
    joined = P.join_subjects(exploded, cats)
    assert joined.count() == exploded.count()          # LEFT join does not multiply rows

    subj = {(r["id"], r["subject_group"]) for r in P.build_paper_subjects(papers, cats).collect()}
    assert subj == {("alias", "Mathematics"),                 # cs.NA is an alias of math.NA
                    ("cross", "Computer Science"), ("cross", "Quantitative Biology"),
                    ("unknown", "Unknown"), ("empty", "Unknown")}, subj


def test_dedupe_keeps_latest_update():
    rows = [("d", "old title", "x", "math.CO", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), dt.datetime(2020, 2, 1)),
            ("d", "new title", "x", "math.CO", v(("v1", "Mon, 6 Jan 2020 09:00:00 GMT")), dt.datetime(2021, 2, 1))]
    out = P.dedupe_papers(P.clean_papers(raw_df(rows), RULES)).collect()
    assert len(out) == 1 and out[0]["title"] == "new title"


# ----------------------------------------------------------------------------- SQL
def _papers_for_sql():
    def row(i, created, cats, title):
        return (f"p{i}", title, "x", cats, v(("v1", created)), UPD)
    rows = [
        row(1, "Mon, 6 Jan 2025 09:00:00 GMT", "cs.LG", "t"),
        row(2, "Mon, 6 Jan 2025 09:00:00 GMT", "math.NT", "t"),
        row(3, "Mon, 6 Jan 2025 09:00:00 GMT", "math.NT q-bio.NC", "Large language models for proofs"),
        row(4, "Tue, 6 Jan 2026 09:00:00 GMT", "math.NT", "t"),
        row(5, "Tue, 6 Jan 2026 09:00:00 GMT", "econ.GN", "Machine learning in labour markets"),
        row(6, "Thu, 1 Oct 2026 09:00:00 GMT", "cs.LG", "t"),      # outside window
    ]
    return P.clean_papers(raw_df(rows), RULES)


def test_sql_monthly_trends():
    s = spark()
    _papers_for_sql().createOrReplaceTempView("papers")
    out = {str(r["month_start"]): r for r in s.sql(P.read_sql("monthly_trends.sql")).collect()}
    jan25 = out["2025-01-01"]
    assert (jan25["total_papers"], jan25["ai_papers"], jan25["genai_llm_papers"]) == (3, 2, 1)
    assert abs(jan25["ai_share"] - 2 / 3) < 1e-12
    assert jan25["ai_text_only_papers"] == 1 and jan25["ai_category_papers"] == 1
    assert "2026-10-01" not in out


def test_sql_field_comparison_pp_and_zero_base():
    s = spark()
    papers = _papers_for_sql()
    P.build_paper_subjects(papers, P.load_categories(s)).createOrReplaceTempView("paper_subjects")
    out = {r["subject_group"]: r for r in s.sql(P.read_sql("field_comparison.sql"), args={"last_month": 9}).collect()}
    math = out["Mathematics"]
    assert (math["total_2025"], math["ai_2025"], math["total_2026"], math["ai_2026"]) == (2, 1, 1, 0)
    assert abs(math["ai_share_change_pp"] - (0 - 50.0)) < 1e-9          # percentage points
    econ = out["Economics"]
    assert econ["total_2025"] == 0 and econ["ai_share_2025"] is None
    assert econ["ai_papers_pct_change"] is None                          # zero base -> NULL, not infinity
    # a paper in two groups counts in each group
    assert out["Quantitative Biology"]["total_2025"] == 1


ALL_TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]

if __name__ == "__main__":
    failed = 0
    for t in ALL_TESTS:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    if _SPARK is not None:
        _SPARK.stop()
    print(f"{len(ALL_TESTS) - failed}/{len(ALL_TESTS)} tests passed")
    sys.exit(1 if failed else 0)
