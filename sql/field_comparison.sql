-- SQL 2: Cross-disciplinary AI comparison, 2025 vs 2026, same months (RQ2)
--
-- Input view `paper_subjects`: one row per unique (paper id, subject_group).
-- A paper listed in two subject groups counts once in each group, so group
-- totals add up to more than the number of papers and must not be summed
-- into a global total.
--
-- Parameter :last_month - compare January..:last_month of both years only
-- (2026 is a partial year in this snapshot, so it is never compared with the
-- full 2025 year).
--
-- *_change_pp   = difference of two shares, in percentage points
-- *_pct_change  = relative change of a count, in percent (NULL when the 2025
--                 base is zero, instead of an infinite growth rate)
WITH by_group_year AS (
    SELECT
        subject_group,
        submitted_year,
        COUNT(*)                                              AS total_papers,
        SUM(CASE WHEN ai_related THEN 1 ELSE 0 END)           AS ai_papers,
        SUM(CASE WHEN mentions_genai_llm THEN 1 ELSE 0 END)   AS genai_llm_papers
    FROM paper_subjects
    WHERE submitted_year IN (2025, 2026)
      AND submitted_month BETWEEN 1 AND :last_month
    GROUP BY subject_group, submitted_year
),
pivoted AS (
    SELECT
        subject_group,
        SUM(CASE WHEN submitted_year = 2025 THEN total_papers     ELSE 0 END) AS total_2025,
        SUM(CASE WHEN submitted_year = 2025 THEN ai_papers        ELSE 0 END) AS ai_2025,
        SUM(CASE WHEN submitted_year = 2025 THEN genai_llm_papers ELSE 0 END) AS genai_llm_2025,
        SUM(CASE WHEN submitted_year = 2026 THEN total_papers     ELSE 0 END) AS total_2026,
        SUM(CASE WHEN submitted_year = 2026 THEN ai_papers        ELSE 0 END) AS ai_2026,
        SUM(CASE WHEN submitted_year = 2026 THEN genai_llm_papers ELSE 0 END) AS genai_llm_2026
    FROM by_group_year
    GROUP BY subject_group
),
shares AS (
    SELECT
        *,
        ai_2025 / NULLIF(total_2025, 0)        AS ai_share_2025,
        ai_2026 / NULLIF(total_2026, 0)        AS ai_share_2026,
        genai_llm_2025 / NULLIF(total_2025, 0) AS genai_llm_share_2025,
        genai_llm_2026 / NULLIF(total_2026, 0) AS genai_llm_share_2026
    FROM pivoted
)
SELECT
    subject_group,
    :last_month                                              AS months_compared,
    total_2025,
    ai_2025,
    ai_share_2025,
    genai_llm_2025,
    genai_llm_share_2025,
    total_2026,
    ai_2026,
    ai_share_2026,
    genai_llm_2026,
    genai_llm_share_2026,
    100 * (ai_share_2026 - ai_share_2025)                    AS ai_share_change_pp,
    100 * (genai_llm_share_2026 - genai_llm_share_2025)      AS genai_llm_share_change_pp,
    100 * (ai_2026 - ai_2025) / NULLIF(ai_2025, 0)           AS ai_papers_pct_change,
    100 * (total_2026 - total_2025) / NULLIF(total_2025, 0)  AS total_papers_pct_change
FROM shares
ORDER BY total_2026 DESC
