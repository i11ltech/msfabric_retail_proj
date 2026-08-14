DROP TABLE IF EXISTS dbo.fact_sales;
GO
 
CREATE TABLE dbo.fact_sales AS
SELECT
    CAST(CONVERT(varchar(8), s.transaction_ts, 112) AS INT)   AS date_key,
    COALESCE(dc.customer_key, -1)                             AS customer_key,
    COALESCE(ds.store_key,    -1)                             AS store_key,
    COALESCE(dp.product_key,  -1)                             AS product_key,
    s.order_id,
    s.line_number,
    s.transaction_ts,
    s.payment_method,
    s.order_status,
    s.source_system,
    s.quantity,
    s.unit_price,
    s.discount_amount,
    CAST(s.quantity * s.unit_price - s.discount_amount
         AS DECIMAL(12,2))                                    AS net_amount,
    CAST(s.quantity * s.unit_price - s.discount_amount
         - s.quantity * dp.unit_cost AS DECIMAL(12,2))        AS margin
FROM meridian_lh.dbo.silver_sales AS s
LEFT JOIN dbo.dim_customer dc ON dc.customer_id = s.customer_id
LEFT JOIN dbo.dim_store    ds ON ds.store_id    = s.store_id
LEFT JOIN dbo.dim_product  dp ON dp.product_id  = s.product_id;
GO
 