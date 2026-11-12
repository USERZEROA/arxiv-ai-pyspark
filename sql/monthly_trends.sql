-- SQL 1: Monthly AI research trends (RQ1)
--
-- Input view `papers`: one row per unique arXiv paper (id), with its first
-- submission month (v1 `created`, UTC) inside the study window.
-- Denominator = all valid papers of the month, AI and non-AI.
-- mentions_genai_llm implies mentions_ai_terms implies ai_related, so
-- genai_llm_papers <= ai_papers <= total_papers.
WITH monthly AS (
    SELECT
        month_start,
        submitted_year,
        submitted_month,
        COUNT(*)                                                              AS total_papers,
        SUM(CASE WHEN ai_related THEN 1 ELSE 0 END)                           AS ai_papers,
        SUM(CASE WHEN is_ai_category THEN 1 ELSE 0 END)                       AS ai_category_papers,
        SUM(CASE WHEN mentions_ai_terms AND NOT is_ai_category THEN 1 ELSE 0 END) AS ai_text_only_papers,
        SUM(CASE WHEN mentions_genai_llm THEN 1 ELSE 0 END)                   AS genai_llm_papers
    FROM papers
    GROUP BY month_start, submitted_year, submitted_month
)
SELECT
    month_start,
    submitted_year,
    submitted_month,
    total_papers,
    ai_papers,
    ai_papers / total_papers                                   AS ai_share,
    ai_category_papers,
    ai_text_only_papers,
    genai_llm_papers,
    genai_llm_papers / total_papers                            AS genai_llm_share,
    genai_llm_papers / NULLIF(ai_papers, 0)                    AS genai_llm_share_of_ai,
    -- Same month one year earlier. pipeline.py checks that the months are
    -- contiguous, so a 12-row lag is exactly 12 calendar months.
    LAG(ai_papers / total_papers, 12) OVER (ORDER BY month_start)            AS ai_share_prev_year,
    100 * (ai_papers / total_papers
           - LAG(ai_papers / total_papers, 12) OVER (ORDER BY month_start))  AS ai_share_yoy_change_pp
FROM monthly
ORDER BY month_start
