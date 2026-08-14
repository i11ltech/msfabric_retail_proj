DROP TABLE IF EXISTS dbo.dim_customer;
GO
 
CREATE TABLE dbo.dim_customer AS
SELECT
    -- ROW_NUMBER() is nullable in CTAS; ISNULL forces NOT NULL for the PK
    ISNULL(ROW_NUMBER() OVER (ORDER BY customer_id), -1)  AS customer_key,
    customer_id,
    customer_code,
    first_name,
    last_name,
    segment,
    preferred_channel,
    home_region,
    age_band,
    is_churned
FROM meridian_lh.dbo.silver_customer
UNION ALL
SELECT -1, -1, 'UNKNOWN', 'Unknown', 'Customer', 'Unknown',
       'Unknown', 'Unknown', 'Unknown', CAST(0 AS BIT);
GO
 
SELECT COUNT(*) AS dim_customer_rows FROM dbo.dim_customer;   -- expect 1201 (1200 + Unknown)
