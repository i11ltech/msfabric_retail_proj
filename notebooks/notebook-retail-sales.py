#!/usr/bin/env python
# coding: utf-8

# ## notebook-retail-sales
# 
# New notebook

# In[6]:


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

try:
    fs = notebookutils.fs
except NameError:
    fs = mssparkutils.fs

raw_folder = "Files/raw"

def find_file(filename):
        hits = [f.name for f in fs.ls(raw_folder) if f.name.startswith(filename)]
        if len(hits) == 1:
            return hits[0]
        # elif len(hits) == 0:
        #     raise FileNotFoundError(f"'{filename}' not found in {raw_folder}")
        # else:
        #     raise ValueError(f"Multiple matches for '{filename}' in {raw_folder}: {hits}")


# In[2]:


from pyspark.sql import functions as F

def isNotDuplicateFile(fname, date, table_name="load_control"):
    """
    Checks the audit table for an existing row matching this filename + date.
    Returns True if no such record exists yet (safe to process),
    False if it's already been logged (duplicate/idempotency guard).
    """
    # Table may not exist yet on the very first run
    if not spark.catalog.tableExists(table_name):
        return True

    match_count = (
        spark.table(table_name)
        .filter(
            (F.col("source_file") == fname) &
            (F.to_date(F.col("loaded_at")) == F.to_date(F.lit(date)))
        )
        .count()
    )

    return match_count == 0


# In[3]:


def write_csv_to_bronze(to_bronze_df, fname):
    to_bronze_df.write.format("delta").mode("append").saveAsTable(fname)
    #n = len(to_bronze_df)
    n = spark.table(fname).count()
    print(f"{fname} : {n:,} rows ")
    return n


# In[9]:


from pyspark.sql import functions as F

audit_table_name = "load_control"

spark.sql("""CREATE TABLE IF NOT EXISTS load_control (
    source_file STRING, loaded_at TIMESTAMP, rows_merged INT, rows_updated INT, rows_inserted INT
) USING DELTA""")

def write_audit_record(filename: str, file_date, **extra_cols):
    # Build the row as a dict so extra columns are easy to bolt on
    row = {"source_file": filename, "file_date": file_date, **extra_cols} 

    df = spark.createDataFrame([tuple(row.values())], list(row.keys()))
    #df = df.withColumn("loaded_at", F.to_timestamp(F.col("loaded_at")))
    #df = df.withColumn("rows_inserted", F.lit(dataframe.count()))
    df = df.withColumn("loaded_at", F.current_timestamp())

    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table_name)
    print(f"AUDIT: logged {filename} - {file_date}")


# In[12]:


#read raw folder
#loop thru all files in list, pick from folder, split name & timestamp
# write timestamp & file to audit table
# write content to bronze
#mv file to processed

DIM_STORE_SCHEMA = ["store_id", "store_code", "store_name", "city", "region", "store_type",
             "open_date", "square_feet", "employee_count"]

DIM_PROMOTION_SCHEMA = ["promotion_id", "promo_code", "promotion_name", "promo_type",
                  "discount_pct", "start_date", "end_date", "applies_to_category"]

DIM_DATE_SCHEMA = ["date_key", "date", "year", "quarter", "quarter_label", "month_number",
            "month_name", "month_short", "iso_week", "day_of_month", "day_of_week",
            "day_name", "is_weekend", "holiday_name", "is_holiday", "fiscal_year"]

DIM_CUSTOMER_SCHEMA = ["customer_id", "customer_code", "first_name", "last_name", "email",
                 "segment", "preferred_channel", "home_region", "join_date",
                 "age_band", "is_churned"]

SALES_COLS_SCHEMA = ["order_id","line_number","transaction_ts","store_id","customer_id","product_id",
              "quantity","unit_price","discount_amount","payment_method","order_status","source_system"]

files_schemas_tables = [
    ("dim_store",DIM_STORE_SCHEMA,"bronze_store"),
    ("dim_promotion",DIM_PROMOTION_SCHEMA,"bronze_promotion"),
    ("dim_date",DIM_DATE_SCHEMA,"bronze_date"),
    ("dim_customer",DIM_CUSTOMER_SCHEMA,"bronze_customer"),
    ("fullload_sales",SALES_COLS_SCHEMA,"bronze_sales")
]

def readCSVFile(filename, schema_cols):
    schema = StructType([StructField(c, StringType(), True) for c in schema_cols])
    df = (spark.read
        .schema(schema)                              
        .option("header", True)
        .option("badRecordsPath", "Files/quarantine/badrecords")   # structural failures go here
        .csv("Files/raw/" + filename))
    display(filename)
    return df

def getFileDate(filename):
    fname, date_with_ext = filename.rsplit("_", 1)
    date = date_with_ext.replace(".csv", "")
    return date

def mvFileToProcessed(file):
    mssparkutils.fs.mv(
        f"Files/raw/{file}",
        f"Files/processed/{file}",
        True
    )

for fname, schema, tblname in files_schemas_tables: 
    file = find_file(fname)
    if file:
        df = readCSVFile(file, schema)
        date = getFileDate(file)
        if isNotDuplicateFile(file, date):
            write_csv_to_bronze(df,tblname)
            write_audit_record(fname, date)
            mvFileToProcessed(file)

        


# In[13]:


brnz_slvr_tabels = {
    "bronze_store" : "silver_store",
    "bronze_promotion" : "silver_promotion",
    "bronze_date" : "silver_date",
    "bronze_customer" : "silver_customer"
}

def clean_df(df):
    df = df.dropDuplicates()
    df = df.dropna(how = "all")
    return df

def write_delta(df, table_name):
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)


for brnz_tbl, silver_tbl in brnz_slvr_tabels.items():
    df = spark.table(brnz_tbl) 
    print(f"Cleaning...: {brnz_tbl} - {df.count()}")
    df = clean_df(df)
    print(f"Cleaned: Records - {df.count()}")
    write_delta(df,silver_tbl)


# In[13]:


def clean_sales(df_raw):
    """Bronze -> Silver transform for Meridian sales. Returns (valid_df, quarantine_df)."""
    typed = (df_raw
        .dropDuplicates(["order_id", "line_number"])                              # pattern 1
        .withColumn("transaction_ts",
            F.coalesce(F.to_timestamp("transaction_ts"),                          # pattern 2
                       F.to_timestamp("transaction_ts", "MM/dd/yyyy HH:mm")))
        .withColumn("store_id",    F.col("store_id").cast("int"))
        .withColumn("customer_id",
            F.coalesce(F.col("customer_id").cast("int"), F.lit(-1)))             # pattern 4b: guest -> -1
        .withColumn("product_id",  F.col("product_id").cast("int"))
        .withColumn("quantity",    F.col("quantity").cast("int"))
        .withColumn("unit_price",
            F.regexp_replace("unit_price", "[$]", "").cast("decimal(10,2)"))     # pattern 3
        .withColumn("discount_amount", F.col("discount_amount").cast("decimal(10,2)"))
        .withColumn("order_status", F.initcap("order_status")))                   # pattern 4a

    is_valid = ((F.col("quantity") > 0) &                                         # pattern 5
                F.col("transaction_ts").isNotNull() &
                F.col("unit_price").isNotNull())
    return typed.filter(is_valid), typed.filter(~is_valid)


# In[14]:


## Silver Sales Full Load
silver_df, quarantine_df = clean_sales(spark.table("bronze_sales"))
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_sales")
quarantine_df.write.format("delta").mode("overwrite").saveAsTable("quarantine_sales")

print(f"silver_sales     : {spark.table('silver_sales').count():,} ")
print(f"quarantine_sales : {spark.table('quarantine_sales').count():,} ")
print(f"guest lines (-1) : {spark.table('silver_sales').filter('customer_id = -1').count():,} ")


# In[25]:


## Silver Products
product_file = find_file("products.json")
if product_file:
    silver_products = (spark.read
        .option("multiline", "true")
        .json("Files/raw/" + product_file)
        .select(
            "product_id", "sku", "product_name", "category", "subcategory", "brand",
            F.col("unit_cost").cast("decimal(10,2)").alias("unit_cost"),
            F.col("list_price").cast("decimal(10,2)").alias("list_price"),
            "launch_date", "is_active",
            F.col("attributes.color").alias("attr_color"),
            F.col("attributes.weight_kg").alias("attr_weight_kg"),
            F.col("attributes.rating").alias("attr_rating")))

    silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")
    print(f"silver_products: {spark.table('silver_products').count()} rows (expect 250, 13 flat columns)")
    mvFileToProcessed("products.json")


# In[24]:


## Bronze Sales Incremental append
#raw_batch = (spark.read.schema(sales_schema).option("header", True).csv(incremental_file_path))

incremental_sales_file = "incremental_sales"

incre_sales_dated_file = find_file(incremental_sales_file)
if incre_sales_dated_file:
    raw_batch_df = readCSVFile(incre_sales_dated_file, SALES_COLS_SCHEMA)
    date = getFileDate(incre_sales_dated_file)
    if isNotDuplicateFile(incre_sales_dated_file, date):
        write_csv_to_bronze(raw_batch_df,"bronze_sales")
        write_audit_record(incremental_sales_file, date)
        mvFileToProcessed(incre_sales_dated_file)

print("Incremental sales processed")


# In[22]:


## Silver Sales Incremental merge
from delta.tables import DeltaTable

batch, batch_quar = clean_sales(raw_batch_df)
batch = batch.cache()

print(f"incoming raw rows : {raw_batch_df.count():,}")
print(f"clean merge batch : {batch.count():,}")
print(f"batch quarantined : {batch_quar.count():,}")


silver_target = DeltaTable.forName(spark, "silver_sales")

(silver_target.alias("t")
    .merge(batch.alias("s"),
           "t.order_id = s.order_id AND t.line_number = s.line_number")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())


# In[ ]:


## Log the load and move the file (idempotency)
merge_metrics = silver_target.history(1).select("operationMetrics").collect()[0]["operationMetrics"]
rows_updated  = int(merge_metrics.get("numTargetRowsUpdated", 0))
rows_inserted = int(merge_metrics.get("numTargetRowsInserted", 0))

write_audit_record(incre_sales_dated_file, None, rows_merged=batch.count(), rows_updated=rows_updated, rows_inserted=rows_inserted)


# In[ ]:


##????????????
abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Files/<path>
abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table>
### Delta Change Data Feed (CDF) — enable it on bronze 
(ALTER TABLE bronze_sales SET TBLPROPERTIES (delta.enableChangeDataFeed = true))

### then read only what changed since the last processed version with 
spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", last_version).table("bronze_sales") 
### This lets silver stay in sync purely from bronze's transaction log, with no need to track incoming files at all.


# In[ ]:


# spark.sql("""CREATE TABLE IF NOT EXISTS load_control (
#     source_file STRING, loaded_at TIMESTAMP, rows_merged INT, rows_updated INT, rows_inserted INT
# ) USING DELTA""")

# this_file = file.name

# already = spark.table("load_control").filter(F.col("source_file") == this_file).count() > 0
# if already:
#     print(f"SKIP: {this_file} already loaded — idempotency check working.")
# else:
#     spark.createDataFrame(
#         [(this_file, None, batch.count(), 119, 2482)],
#         "source_file STRING, loaded_at TIMESTAMP, rows_merged INT, rows_updated INT, rows_inserted INT"
#     ).withColumn("loaded_at", F.current_timestamp()) \
#     .write.format("delta").mode("append").saveAsTable("load_control")
#     print(f"LOGGED: {this_file}")

# spark.table("load_control").show(truncate=False)

#     before_rows     = spark.table("silver_sales").count()
# before_returned = spark.table("silver_sales").filter("order_status = 'Returned'").count()

# # The matched keys' CURRENT status in silver — spoiler: all Completed, about to flip
# prior = (spark.table("silver_sales").alias("t")
#          .join(batch.select("order_id","line_number"), ["order_id","line_number"], "inner")
#          .groupBy("order_status").count())
# print(f"silver rows     : {before_rows:,}  ")
# print(f"Returned lines  : {before_returned:,}  ")
# prior.show()

# target = DeltaTable.forName(spark, "silver_sales")

# (target.alias("t")
#     .merge(batch.alias("s"),
#            "t.order_id = s.order_id AND t.line_number = s.line_number")
#     .whenMatchedUpdateAll()
#     .whenNotMatchedInsertAll()
#     .execute())

# # Delta logged exactly what happened — read it back from the table history
# metrics = (target.history(1).select("operation", "operationMetrics").collect()[0])
# m = metrics.operationMetrics
# print("operation:", metrics.operation)
# print(f"  rows updated : {m.get('numTargetRowsUpdated')} ")
# print(f"  rows inserted: {m.get('numTargetRowsInserted')}")

# after_rows     = spark.table("silver_sales").count()
# after_returned = spark.table("silver_sales").filter("order_status = 'Returned'").count()
# post = (spark.table("silver_sales")
#         .join(batch.select("order_id","line_number"), ["order_id","line_number"], "inner")
#         .groupBy("order_status").count())
# print(f"silver rows after     : {after_rows:,}   ")
# print(f"Returned lines after  : {after_returned:,} ")
# post.show()


# In[ ]:




