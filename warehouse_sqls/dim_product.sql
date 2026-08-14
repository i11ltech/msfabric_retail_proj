DROP TABLE IF EXISTS dbo.dim_store;
GO
CREATE TABLE dbo.dim_store AS
SELECT
    ISNULL(ROW_NUMBER() OVER (ORDER BY store_id), -1)  AS store_key,
    store_id, store_code, store_name, city, region, store_type
FROM meridian_lh.dbo.silver_store
UNION ALL
SELECT -1, -1, 'UNKNOWN', 'Unknown Store', 'Unknown', 'Unknown', 'Unknown';
GO
SELECT COUNT(*) AS dim_store_rows FROM dbo.dim_store;         -- expect 22 (21 + Unknown)
 
DROP TABLE IF EXISTS dbo.dim_product;
GO
CREATE TABLE dbo.dim_product AS
SELECT
    ISNULL(ROW_NUMBER() OVER (ORDER BY product_id), -1)  AS product_key,
    product_id, sku, product_name, category, subcategory, brand,
    unit_cost, list_price, is_active
FROM meridian_lh.dbo.silver_products
UNION ALL
SELECT -1, -1, 'UNKNOWN', 'Unknown Product', 'Unknown', 'Unknown', 'Unknown',
       CAST(0.00 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(0 AS BIT);
GO
SELECT COUNT(*) AS dim_product_rows FROM dbo.dim_product;     -- expect 251 (250 + Unknown)
