DROP TABLE IF EXISTS dbo.dim_date;
GO
 
CREATE TABLE dbo.dim_date AS
WITH digits AS (
    SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
),
tally AS (
    SELECT a.n + 10*b.n + 100*c.n + 1000*e.n AS n
    FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits e
),
dates AS (
    SELECT DATEADD(day, n, CAST('2023-01-01' AS DATE)) AS d
    FROM tally
    WHERE n <= DATEDIFF(day, '2023-01-01', '2026-12-31')
)
SELECT
    -- ISNULL is not decoration: in a CTAS, ISNULL() makes the column NOT NULL
    -- (COALESCE would NOT — it stays nullable). PKs in Section 5 require NOT NULL.
    ISNULL(CAST(CONVERT(varchar(8), d, 112) AS INT), 0)           AS date_key,      -- 20260730
    d                                                             AS [date],
    YEAR(d)                                                       AS [year],
    CAST(YEAR(d) AS VARCHAR(4)) +'-' + CAST(MONTH(d) AS VARCHAR)  AS [year-mm],
    DATEPART(quarter, d)                                          AS [quarter],
    -- FABRIC WAREHOUSE RULE: nvarchar is NOT supported. DATENAME() (and some
    -- string builders) return nvarchar, so every constructed string in a CTAS
    -- must be explicitly CAST to varchar(n) or the CREATE fails with Msg 24574.
    CAST(CONCAT('Q', DATEPART(quarter, d), '-', YEAR(d)) AS varchar(8))
                                                                  AS quarter_label,
    MONTH(d)                                                      AS month_number,
    CAST(DATENAME(month, d) AS varchar(15))                       AS month_name,
    CAST(LEFT(DATENAME(month, d), 3) AS varchar(3))               AS month_short,
    DATEPART(iso_week, d)                                         AS iso_week,
    DAY(d)                                                        AS day_of_month,
    -- Deterministic ISO weekday (Mon=1..Sun=7) regardless of server DATEFIRST:
    -- 1900-01-01 was a Monday, so days-since mod 7 gives the ISO position.
    (DATEDIFF(day, '19000101', d) % 7) + 1                        AS day_of_week,
    CAST(DATENAME(weekday, d) AS varchar(10))                     AS day_name,
    CASE WHEN DATENAME(weekday, d) IN ('Saturday','Sunday')
         THEN 1 ELSE 0 END                                        AS is_weekend,
    -- Meridian fiscal year starts July 1: Jul 2025 -> FY2026
    CAST(CONCAT('FY', CASE WHEN MONTH(d) >= 7 THEN YEAR(d) + 1
                           ELSE YEAR(d) END) AS varchar(6))       AS fiscal_year
FROM dates;
GO
 
-- Verify: exactly 1,461 days
SELECT COUNT(*) AS dim_date_rows FROM dbo.dim_date;   -- expect 1461
 
-- Compare YOUR generated calendar against the reference one in the Lakehouse
-- (cross-database query + EXCEPT = an instant diff). Both queries should return 0.
SELECT COUNT(*) AS rows_only_in_mine
FROM (
    SELECT date_key, is_weekend, fiscal_year FROM dbo.dim_date
    EXCEPT
    SELECT date_key, is_weekend, fiscal_year FROM meridian_lh.dbo.silver_date
) x;
 
SELECT COUNT(*) AS rows_only_in_reference
FROM (
    SELECT date_key, is_weekend, fiscal_year FROM meridian_lh.dbo.silver_date
    EXCEPT
    SELECT date_key, is_weekend, fiscal_year FROM dbo.dim_date
) x;
GO
 