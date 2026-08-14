ALTER TABLE dbo.dim_customer ADD CONSTRAINT pk_dim_customer
    PRIMARY KEY NONCLUSTERED (customer_key) NOT ENFORCED;
ALTER TABLE dbo.dim_store    ADD CONSTRAINT pk_dim_store
    PRIMARY KEY NONCLUSTERED (store_key)    NOT ENFORCED;
ALTER TABLE dbo.dim_product  ADD CONSTRAINT pk_dim_product
    PRIMARY KEY NONCLUSTERED (product_key)  NOT ENFORCED;
ALTER TABLE dbo.dim_date     ADD CONSTRAINT pk_dim_date
    PRIMARY KEY NONCLUSTERED (date_key)     NOT ENFORCED;
GO