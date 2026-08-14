SELECT st.region,
       SUM(f.net_amount) AS net_revenue,
       SUM(f.margin)     AS margin
FROM dbo.fact_sales f
JOIN dbo.dim_store st ON st.store_key = f.store_key
WHERE f.order_status = 'Completed'
GROUP BY st.region
ORDER BY net_revenue DESC;
 