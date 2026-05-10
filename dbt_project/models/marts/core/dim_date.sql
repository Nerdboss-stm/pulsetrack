{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- Calendar dimension with medical-domain attributes (flu-season, pollen-season,
-- daylight-savings flags). Generated from the project-wide date_spine macro.

WITH spine AS (
    {{ pulsetrack.date_spine('2020-01-01', '2035-12-31') }}
)

SELECT
    -- Surrogate key — date as YYYYMMDD integer (the BI-tools-friendly format).
    CAST(STRFTIME(date_day, '%Y%m%d') AS INTEGER)         AS date_key,
    date_day                                              AS full_date,
    EXTRACT(YEAR FROM date_day)                           AS year,
    EXTRACT(QUARTER FROM date_day)                        AS quarter,
    EXTRACT(MONTH FROM date_day)                          AS month,
    EXTRACT(WEEK FROM date_day)                           AS iso_week,
    EXTRACT(DAY FROM date_day)                            AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_day)                      AS day_of_week,
    EXTRACT(DAYOFYEAR FROM date_day)                      AS day_of_year,

    -- Day-of-week name + weekend flag.
    DAYNAME(date_day)                                     AS day_name,
    EXTRACT(DAYOFWEEK FROM date_day) IN (0, 6)            AS is_weekend,

    MONTHNAME(date_day)                                   AS month_name,

    -- Medical calendar attributes:
    -- Flu season (Northern Hemisphere): October through March.
    EXTRACT(MONTH FROM date_day) IN (10, 11, 12, 1, 2, 3) AS is_flu_season,

    -- Pollen / allergy season: April through September.
    EXTRACT(MONTH FROM date_day) IN (4, 5, 6, 7, 8, 9)    AS is_pollen_season,

    -- DST is roughly 2nd Sunday of March → 1st Sunday of November (US).
    -- Approximation: months Mar-Oct.
    EXTRACT(MONTH FROM date_day) IN (3, 4, 5, 6, 7, 8, 9, 10)
                                                          AS is_daylight_savings,

    -- Quarter + fiscal-year shifted (April-March fiscal year, common in
    -- healthcare orgs).
    'FY' || CAST(
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 4
                THEN EXTRACT(YEAR FROM date_day)
            ELSE EXTRACT(YEAR FROM date_day) - 1
        END AS VARCHAR)                                   AS fiscal_year_label
FROM spine
