-- 6a. Row parity: both numbers identical
SELECT
    (SELECT COUNT(*) FROM dbo.fact_sales)                    AS fact_rows,
    (SELECT COUNT(*) FROM lh_meridian.dbo.silver_sales)      AS silver_rows;
 
-- 6b. Revenue parity: difference must be exactly 0.00
SELECT
    (SELECT SUM(net_amount) FROM dbo.fact_sales)             AS fact_net,
    (SELECT SUM(quantity * unit_price - discount_amount)
       FROM lh_meridian.dbo.silver_sales)                    AS silver_net;
 
-- 6c. Referential integrity (remember: constraints are NOT ENFORCED — prove it yourself)
SELECT COUNT(*) AS orphan_or_null_keys
FROM dbo.fact_sales f
LEFT JOIN dbo.dim_date     d  ON d.date_key     = f.date_key
LEFT JOIN dbo.dim_customer dc ON dc.customer_key = f.customer_key
LEFT JOIN dbo.dim_store    ds ON ds.store_key    = f.store_key
LEFT JOIN dbo.dim_product  dp ON dp.product_key  = f.product_key
WHERE d.date_key IS NULL OR dc.customer_key IS NULL
   OR ds.store_key IS NULL OR dp.product_key IS NULL;        -- expect 0
 
-- 6d. Unknown-member usage: the guest checkouts, visible and honest
SELECT COUNT(*) AS guest_lines_on_unknown_customer
FROM dbo.fact_sales
WHERE customer_key = -1;                                     -- expect 43,044+ (grows with incrementals)
GO
 