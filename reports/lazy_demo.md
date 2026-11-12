# Actions vs transformations - recorded output

Code (from `pipeline.py`):

```python
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
```

* Spark jobs in group `lazy-demo` before: **0**
* after defining 2 filters + withColumn (transformations): **0** (16.5 ms to build the plan)
* after `collect()` on the aggregate (action): **4** (0.38 s)

Action result:

| mentions_genai_llm | papers | avg_title_words |
|---|---:|---:|
| False | 89,484 | 10.2 |
| True | 40,474 | 10.45 |

Plan printed by `demo.explain('formatted')` before the action (no job is run):

```text
== Physical Plan ==
* Project (4)
+- * Filter (3)
   +- * ColumnarToRow (2)
      +- Scan parquet  (1)


(1) Scan parquet 
Output [14]: [id#1942, title#1943, update_date#1944, submitted_date#1945, submitted_month#1946, month_start#1947, category_list#1948, is_ai_category#1949, mentions_ai_terms#1950, mentions_genai_llm#1951, ai_related#1952, text_available#1953, abstract_available#1954, submitted_year#1955]
Batched: true
Location: InMemoryFileIndex [file:/C:/Users/yuhai/Desktop/201E~1/arxiv-ai-pyspark/outputs/papers_clean]
PartitionFilters: [isnotnull(submitted_year#1955), (submitted_year#1955 = 2025)]
PushedFilters: [IsNotNull(ai_related), EqualTo(ai_related,true)]
ReadSchema: struct<id:string,title:string,update_date:timestamp_ntz,submitted_date:date,submitted_month:int,month_start:date,category_list:array<string>,is_ai_category:boolean,mentions_ai_terms:boolean,mentions_genai_llm:boolean,ai_related:boolean,text_available:boolean,abstract_available:boolean>

(2) ColumnarToRow [codegen id : 1]
Input [14]: [id#1942, title#1943, update_date#1944, submitted_date#1945, submitted_month#1946, month_start#1947, category_list#1948, is_ai_category#1949, mentions_ai_terms#1950, mentions_genai_llm#1951, ai_related#1952, text_available#1953, abstract_available#1954, submitted_year#1955]

(3) Filter [codegen id : 1]
Input [14]: [id#1942, title#1943, update_date#1944, submitted_date#1945, submitted_month#1946, month_start#1947, category_list#1948, is_ai_category#1949, mentions_ai_terms#1950, mentions_genai_llm#1951, ai_related#1952, text_available#1953, abstract_available#1954, submitted_year#1955]
Condition : (isnotnull(ai_related#1952) AND ai_related#1952)

(4) Project [codegen id : 1]
Output [15]: [id#1942, title#1943, update_date#1944, submitted_date#1945, submitted_month#1946, month_start#1947, category_list#1948, is_ai_category#1949, mentions_ai_terms#1950, mentions_genai_llm#1951, ai_related#1952, text_available#1953, abstract_available#1954, submitted_year#1955, size(split(trim(title#1943, None), \s+, -1), false) AS title_words#1957]
Input [14]: [id#1942, title#1943, update_date#1944, submitted_date#1945, submitted_month#1946, month_start#1947, category_list#1948, is_ai_category#1949, mentions_ai_terms#1950, mentions_genai_llm#1951, ai_related#1952, text_available#1953, abstract_available#1954, submitted_year#1955]
```
