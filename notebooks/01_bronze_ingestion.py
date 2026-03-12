#!/usr/bin/env python
# coding: utf-8

# ## 01_bronze_ingestion
# 
# null

# # Bronze Layer Ingestion
# Raw files → Delta tables with no transformation.
# The Bronze layer preserves the original data exactly as received.

# In[1]:


# Read Shopify CSV — note: inferSchema=True lets Spark auto-detect data types
# header=True uses the first row as column names
df_shopify = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/raw/shopify_orders.csv")

print(f"Shopify orders: {df_shopify.count()} rows")
df_shopify.printSchema()
df_shopify.show(5, truncate=False)


# In[2]:


# Read Amazon TSV — delimiter is tab (\t), not comma
df_amazon_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .load("Files/raw/amazon_orders.tsv")

print(f"Amazon orders: {df_amazon_orders.count()} rows")
df_amazon_orders.printSchema()
df_amazon_orders.show(5, truncate=False)


# In[3]:


# Read Amazon Settlement TSV (EAV format — each order has multiple rows)
df_amazon_settlement = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .load("Files/raw/amazon_settlement.tsv")

print(f"Amazon settlement: {df_amazon_settlement.count()} rows")
df_amazon_settlement.printSchema()
df_amazon_settlement.show(10, truncate=False)


# In[4]:


# Read TikTok XLSX — need to install/use pandas for Excel, then convert to Spark DF
# Fabric Spark doesn't natively read .xlsx, so we use pandas as a bridge
import pandas as pd

# Read Orders sheet
pdf_tiktok_orders = pd.read_excel(
    "/lakehouse/default/Files/raw/tiktok_settlement.xlsx",
    sheet_name="Orders"
)
df_tiktok_orders = spark.createDataFrame(pdf_tiktok_orders)

# Read Adjustments sheet
pdf_tiktok_adj = pd.read_excel(
    "/lakehouse/default/Files/raw/tiktok_settlement.xlsx",
    sheet_name="Adjustments"
)
df_tiktok_adjustments = spark.createDataFrame(pdf_tiktok_adj)

print(f"TikTok orders: {df_tiktok_orders.count()} rows")
print(f"TikTok adjustments: {df_tiktok_adjustments.count()} rows")
df_tiktok_orders.printSchema()
df_tiktok_orders.show(5, truncate=False)


# In[6]:


# Save all dataframes as Delta tables in the Lakehouse
# mode="overwrite" ensures idempotency — re-running won't create duplicates
# Enable column mapping to allow spaces in column names                                 
spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
spark.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "2")
spark.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "5")

df_shopify.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("shopify_orders_raw")

df_amazon_orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("amazon_orders_raw")

df_amazon_settlement.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("amazon_settlement_raw")

df_tiktok_orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("tiktok_orders_raw")

df_tiktok_adjustments.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("tiktok_adjustments_raw")

print("All Bronze tables created successfully!")


# In[7]:


# Quick validation — check row counts match source files
tables = [
    "shopify_orders_raw",
    "amazon_orders_raw",
    "amazon_settlement_raw",
    "tiktok_orders_raw",
    "tiktok_adjustments_raw",
]

print("Bronze Layer Validation:")
print("-" * 40)
for table in tables:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")

