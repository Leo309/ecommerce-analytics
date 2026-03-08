# Databricks notebook source / Microsoft Fabric Notebook
# MAGIC %md
# # Silver Layer — Data Cleaning & Standardization
#
# Transform raw Bronze tables into a clean, unified star schema.
#
# **Key transformations:**
# - Date normalization (3 formats → UTC timestamp)
# - Cross-platform SKU mapping → unified product dimension
# - Amazon Settlement EAV pivot → wide financial table
# - Currency conversion (CAD → USD)
# - Deduplication & null handling
# - Filter out test/voided orders

# COMMAND ----------

# Cell 1: Configuration & Imports

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CAD to USD exchange rate (fixed rate for simulation)
# 实际项目中会调 API 获取历史汇率，这里用固定汇率简化
CAD_TO_USD = 0.74

# COMMAND ----------

# Cell 2: SKU Mapping Table — Cross-Platform Product Master
#
# In real e-commerce, each sales channel uses its own SKU naming convention.
# A SKU mapping table is essential for unified product analytics.
# This is the foundation of the product dimension (dim_products).
#
# 三个平台的 SKU 命名完全不同，必须建映射表才能做跨平台产品分析。
# 这是数据建模中 "Conformed Dimension"（一致性维度）的经典案例。

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

sku_mapping_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("shopify_sku", StringType()),
    StructField("amazon_sku", StringType()),
    StructField("tiktok_sku", StringType()),
    StructField("base_price_usd", DoubleType()),
    StructField("weight_oz", IntegerType()),
])

sku_data = [
    ("P001", "Original Concentrate 6oz", "Concentrates", "original-concentrate-6oz", "JAVY-ORIG-6OZ", "TS-ORIGINAL-6", 24.99, 6),
    ("P002", "Original Concentrate 18oz", "Concentrates", "original-concentrate-18oz", "JAVY-ORIG-18OZ", "TS-ORIGINAL-18", 54.99, 18),
    ("P003", "Vanilla Concentrate 6oz", "Concentrates", "vanilla-concentrate-6oz", "JAVY-VAN-6OZ", "TS-VANILLA-6", 26.99, 6),
    ("P004", "Vanilla Concentrate 18oz", "Concentrates", "vanilla-concentrate-18oz", "JAVY-VAN-18OZ", "TS-VANILLA-18", 59.99, 18),
    ("P005", "Mocha Concentrate 6oz", "Concentrates", "mocha-concentrate-6oz", "JAVY-MOC-6OZ", "TS-MOCHA-6", 26.99, 6),
    ("P006", "Mocha Concentrate 18oz", "Concentrates", "mocha-concentrate-18oz", "JAVY-MOC-18OZ", "TS-MOCHA-18", 59.99, 18),
    ("P007", "Caramel Concentrate 6oz", "Concentrates", "caramel-concentrate-6oz", "JAVY-CAR-6OZ", "TS-CARAMEL-6", 26.99, 6),
    ("P008", "Caramel Concentrate 18oz", "Concentrates", "caramel-concentrate-18oz", "JAVY-CAR-18OZ", "TS-CARAMEL-18", 59.99, 18),
    ("P009", "Decaf Concentrate 6oz", "Concentrates", "decaf-concentrate-6oz", "JAVY-DEC-6OZ", "TS-DECAF-6", 27.99, 6),
    ("P010", "Decaf Concentrate 18oz", "Concentrates", "decaf-concentrate-18oz", "JAVY-DEC-18OZ", "TS-DECAF-18", 62.99, 18),
    ("P011", "Travel Mug", "Accessories", "travel-mug", "JAVY-MUG", "TS-MUG", 19.99, 12),
    ("P012", "Cold Brew Kit", "Accessories", "cold-brew-kit", "JAVY-CBKIT", "TS-CBKIT", 34.99, 24),
    ("P013", "Starter Kit", "Bundles", "starter-kit", "JAVY-START", "TS-STARTER", 44.99, 20),
    ("P014", "Variety Pack", "Bundles", "variety-pack", "JAVY-VARIETY", "TS-VARIETY", 49.99, 24),
]

df_sku_mapping = spark.createDataFrame(sku_data, schema=sku_mapping_schema)
df_sku_mapping.show(truncate=False)

# COMMAND ----------

# Cell 3: dim_products — Product Dimension Table
#
# The product dimension is the single source of truth for product attributes.
# All fact tables reference product_id from this table (star schema design).
#
# 产品维度表是星型模型的核心，所有 fact 表通过 product_id 关联到这里。

df_dim_products = df_sku_mapping.select(
    "product_id",
    "product_name",
    "category",
    "shopify_sku",
    "amazon_sku",
    "tiktok_sku",
    "base_price_usd",
    "weight_oz"
)

print(f"dim_products: {df_dim_products.count()} products")
df_dim_products.show(truncate=False)

# COMMAND ----------

# Cell 4: Clean Shopify Orders
#
# Transformations:
# 1. Filter out voided/test orders (Financial Status = 'voided')
# 2. Parse ISO datetime with timezone offset → UTC timestamp
# 3. Map Shopify SKU → unified product_id
# 4. Handle missing emails (flag as 'unknown')
#
# 过滤测试订单、统一日期格式、映射 SKU、处理空邮箱

df_shopify_raw = spark.table("shopify_orders_raw")

# Step 1: Remove test orders (voided)
# 真实 Shopify 数据中，运营测试产生的 voided 订单不应进入分析
df_shopify_clean = df_shopify_raw.filter(
    F.col("Financial Status") != "voided"
)
voided_count = df_shopify_raw.count() - df_shopify_clean.count()
print(f"Removed {voided_count} voided/test orders from Shopify")

# Step 2: Standardize date to UTC timestamp
# Shopify exports dates as ISO with timezone offset (e.g., 2025-03-15T14:30:00-05:00)
df_shopify_clean = df_shopify_clean.withColumn(
    "order_date_utc",
    F.to_utc_timestamp(F.to_timestamp("Paid at"), "UTC")
)

# Step 3: Map Shopify SKU → product_id via lookup join
df_shopify_clean = df_shopify_clean.join(
    df_sku_mapping.select("shopify_sku", "product_id"),
    df_shopify_clean["Lineitem sku"] == df_sku_mapping["shopify_sku"],
    "left"
)

# Step 4: Handle missing emails — replace nulls/empty with 'unknown'
# 真实场景：guest checkout 可能没有邮箱
df_shopify_clean = df_shopify_clean.withColumn(
    "customer_email",
    F.when(
        (F.col("Email").isNull()) | (F.col("Email") == ""),
        F.lit("unknown")
    ).otherwise(F.col("Email"))
)

print(f"Shopify clean: {df_shopify_clean.count()} rows")
df_shopify_clean.select(
    "Name", "customer_email", "Financial Status", "order_date_utc",
    "Lineitem sku", "product_id", "Lineitem quantity", "Total"
).show(10, truncate=False)

# COMMAND ----------

# Cell 5: Clean Amazon Orders
#
# Transformations:
# 1. Deduplicate orders (remove duplicate amazon-order-id rows)
# 2. Parse ISO UTC datetime (already clean, just cast)
# 3. Map Amazon SKU → unified product_id
#
# Amazon 的日期格式最规范（ISO UTC），但有重复订单需要去重

df_amazon_raw = spark.table("amazon_orders_raw")

# Step 1: Deduplicate — keep the first occurrence of each order
# Window function 按 order_id 分组，保留第一条（row_number = 1）
# 面试高频题：去重的几种方式（DISTINCT / GROUP BY / ROW_NUMBER）
window_dedup = Window.partitionBy("amazon-order-id").orderBy("purchase-date")
df_amazon_dedup = df_amazon_raw.withColumn(
    "_row_num", F.row_number().over(window_dedup)
).filter(F.col("_row_num") == 1).drop("_row_num")

dup_count = df_amazon_raw.count() - df_amazon_dedup.count()
print(f"Removed {dup_count} duplicate Amazon orders")

# Step 2: Date is already timestamp type from Bronze (inferSchema detected it)
# Just rename for consistency
df_amazon_clean = df_amazon_dedup.withColumn(
    "order_date_utc",
    F.col("purchase-date")
)

# Step 3: Map Amazon SKU → product_id
df_amazon_clean = df_amazon_clean.join(
    df_sku_mapping.select("amazon_sku", "product_id"),
    df_amazon_clean["sku"] == df_sku_mapping["amazon_sku"],
    "left"
)

print(f"Amazon clean: {df_amazon_clean.count()} rows")
df_amazon_clean.select(
    "amazon-order-id", "order_date_utc", "order-status",
    "fulfillment-channel", "sku", "product_id", "quantity", "item-price"
).show(10, truncate=False)

# COMMAND ----------

# Cell 6: Clean TikTok Orders
#
# Transformations:
# 1. Parse MM/DD/YYYY date string → UTC timestamp
# 2. Convert CAD → USD for cross-border orders (~5% of total)
# 3. Map TikTok SKU → unified product_id (handle ~1% missing SKUs)
#
# TikTok 的日期格式最"脏"（MM/DD/YYYY），还有混合货币问题

df_tiktok_raw = spark.table("tiktok_orders_raw")

# Step 1: Parse date — TikTok uses MM/DD/YYYY format (common in US business exports)
# to_timestamp 需要指定格式才能正确解析，否则会变 null
df_tiktok_clean = df_tiktok_raw.withColumn(
    "order_date_utc",
    F.to_timestamp("Order Date", "MM/dd/yyyy")
)

# Verify: check for null dates (parsing failures)
null_dates = df_tiktok_clean.filter(F.col("order_date_utc").isNull()).count()
print(f"TikTok date parsing failures: {null_dates}")

# Step 2: Currency conversion — normalize CAD to USD
# 标记原始货币，然后统一转换为 USD
cad_count = df_tiktok_clean.filter(F.col("Currency") == "CAD").count()
print(f"TikTok CAD orders to convert: {cad_count}")

money_columns = [
    "Gross Sales", "Seller Discount", "Net Sales",
    "Transaction Fee", "Referral Fee", "Affiliate Commission", "Settlement Amount"
]

for col_name in money_columns:
    df_tiktok_clean = df_tiktok_clean.withColumn(
        col_name,
        F.when(F.col("Currency") == "CAD", F.round(F.col(col_name) * CAD_TO_USD, 2))
         .otherwise(F.col(col_name))
    )

# Mark all as USD after conversion
df_tiktok_clean = df_tiktok_clean.withColumn(
    "original_currency", F.col("Currency")
).withColumn(
    "Currency", F.lit("USD")
)

# Step 3: Map TikTok SKU → product_id
# ~1% of rows have empty SKU — these will get null product_id (flagged for review)
df_tiktok_clean = df_tiktok_clean.join(
    df_sku_mapping.select("tiktok_sku", "product_id"),
    df_tiktok_clean["SKU ID"] == df_sku_mapping["tiktok_sku"],
    "left"
)

unmapped = df_tiktok_clean.filter(F.col("product_id").isNull()).count()
print(f"TikTok orders with unmapped/missing SKU: {unmapped}")

print(f"TikTok clean: {df_tiktok_clean.count()} rows")
df_tiktok_clean.select(
    "Order ID", "order_date_utc", "SKU ID", "product_id",
    "original_currency", "Gross Sales", "Net Sales", "Affiliate Creator"
).show(10, truncate=False)

# COMMAND ----------

# Cell 7: Amazon Settlement Pivot — EAV to Wide Table
#
# The Amazon Settlement report uses Entity-Attribute-Value (EAV) format:
# each order has multiple rows for different fee types (Principal, Tax,
# Commission, FBA Fee, etc.).
#
# We need to PIVOT this into a wide table with one row per order,
# where each fee type becomes its own column.
#
# EAV → Wide Table 是经典的数据转换面试题。
# Amazon 真实的 Settlement Report 就是这个格式，非常常见。

df_settlement_raw = spark.table("amazon_settlement_raw")

# Pivot: group by order-id + sku, spread amount-description into columns
df_settlement_pivot = df_settlement_raw.groupBy("settlement-id", "order-id", "sku") \
    .pivot("amount-description") \
    .agg(F.sum("amount"))

# Rename columns for clarity — these are standard Amazon fee categories
df_settlement_pivot = df_settlement_pivot \
    .withColumnRenamed("Principal", "item_revenue") \
    .withColumnRenamed("Tax", "item_tax") \
    .withColumnRenamed("Shipping", "shipping_revenue") \
    .withColumnRenamed("Commission", "referral_fee") \
    .withColumnRenamed("FBAPerUnitFulfillmentFee", "fba_fee")

# Fill nulls with 0 (not all orders have every fee type)
for col_name in ["item_revenue", "item_tax", "shipping_revenue", "referral_fee", "fba_fee"]:
    df_settlement_pivot = df_settlement_pivot.fillna({col_name: 0})

# Calculate net revenue per order (revenue minus all fees)
df_settlement_pivot = df_settlement_pivot.withColumn(
    "net_revenue",
    F.round(
        F.col("item_revenue") + F.col("shipping_revenue") +
        F.col("referral_fee") + F.col("fba_fee"),  # fees are already negative
        2
    )
)

print(f"Amazon settlement (pivoted): {df_settlement_pivot.count()} rows")
df_settlement_pivot.show(10, truncate=False)

# COMMAND ----------

# Cell 8: Build fact_orders — Unified Order Fact Table
#
# Combine orders from all 3 platforms into a single fact table.
# This is the core of the star schema — one row per order with standardized fields.
# Each order links to dim_products via product_id.
#
# 三个平台的订单合并成统一的事实表，字段名统一、类型统一。
# 这就是 "Single Source of Truth"（SSOT）的体现。

# --- Shopify orders ---
df_fact_shopify = df_shopify_clean.select(
    F.col("Name").alias("order_id"),
    F.lit("Shopify").alias("platform"),
    F.col("order_date_utc"),
    F.col("product_id"),
    F.col("Lineitem name").alias("product_name"),
    F.col("Lineitem quantity").cast("int").alias("quantity"),
    F.col("Lineitem price").cast("double").alias("unit_price"),
    F.col("Subtotal").cast("double").alias("gross_sales"),
    F.col("Discount Amount").cast("double").alias("discount_amount"),
    F.col("Shipping").cast("double").alias("shipping"),
    F.col("Taxes").cast("double").alias("tax"),
    F.col("Total").cast("double").alias("total"),
    F.col("Financial Status").alias("order_status"),
    F.col("customer_email"),
    F.col("Billing Province").alias("state"),
    F.lit("USD").alias("currency"),
)

# --- Amazon orders ---
df_fact_amazon = df_amazon_clean.select(
    F.col("amazon-order-id").alias("order_id"),
    F.lit("Amazon").alias("platform"),
    F.col("order_date_utc"),
    F.col("product_id"),
    F.col("product-name").alias("product_name"),
    F.col("quantity"),
    (F.col("item-price") / F.col("quantity")).alias("unit_price"),
    F.col("item-price").alias("gross_sales"),
    F.lit(0.0).alias("discount_amount"),
    F.col("shipping-price").alias("shipping"),
    F.col("item-tax").alias("tax"),
    (F.col("item-price") + F.col("shipping-price") + F.col("item-tax")).alias("total"),
    F.col("order-status").alias("order_status"),
    F.lit("unknown").alias("customer_email"),  # Amazon doesn't share customer emails
    F.col("ship-state").alias("state"),
    F.lit("USD").alias("currency"),
)

# --- TikTok orders ---
df_fact_tiktok = df_tiktok_clean.select(
    F.col("Order ID").alias("order_id"),
    F.lit("TikTok").alias("platform"),
    F.col("order_date_utc"),
    F.col("product_id"),
    F.col("Product Name").alias("product_name"),
    F.col("Quantity").cast("int").alias("quantity"),
    (F.col("Gross Sales") / F.col("Quantity")).alias("unit_price"),
    F.col("Gross Sales").alias("gross_sales"),
    F.col("Seller Discount").alias("discount_amount"),
    F.lit(0.0).alias("shipping"),
    F.lit(0.0).alias("tax"),
    F.col("Settlement Amount").alias("total"),
    F.lit("completed").alias("order_status"),
    F.lit("unknown").alias("customer_email"),  # TikTok doesn't share customer emails
    F.lit("unknown").alias("state"),  # TikTok settlement doesn't include ship-to state
    F.lit("USD").alias("currency"),
)

# Union all three platforms
df_fact_orders = df_fact_shopify.unionByName(df_fact_amazon).unionByName(df_fact_tiktok)

print(f"fact_orders total: {df_fact_orders.count()} rows")
print("\nBreakdown by platform:")
df_fact_orders.groupBy("platform").count().orderBy("platform").show()

# COMMAND ----------

# Cell 9: Build fact_financials — Revenue & Fee Breakdown
#
# Financial fact table showing revenue, fees, and net amounts per order.
# Combines Shopify (no platform fees in data), Amazon (settlement fees),
# and TikTok (transaction + referral + affiliate fees).
#
# 财务事实表：展示每单的收入、费用和净收入。
# 这是做 Channel Deep Dive 和 Platform Fee Ratio 分析的基础。

# --- Shopify financials (no platform fee data available) ---
df_fin_shopify = df_shopify_clean.select(
    F.col("Name").alias("order_id"),
    F.lit("Shopify").alias("platform"),
    F.col("order_date_utc"),
    F.col("product_id"),
    F.col("Subtotal").cast("double").alias("gross_revenue"),
    F.lit(0.0).alias("platform_fee"),
    F.lit(0.0).alias("fulfillment_fee"),
    F.lit(0.0).alias("affiliate_commission"),
    F.col("Subtotal").cast("double").alias("net_revenue"),  # Simplified: no fee data
)

# --- Amazon financials (from pivoted settlement) ---
df_fin_amazon = df_settlement_pivot.join(
    df_sku_mapping.select("amazon_sku", "product_id"),
    df_settlement_pivot["sku"] == df_sku_mapping["amazon_sku"],
    "left"
).select(
    F.col("order-id").alias("order_id"),
    F.lit("Amazon").alias("platform"),
    # Join with amazon_orders to get the date
    F.lit(None).cast("timestamp").alias("order_date_utc"),  # Will be filled below
    F.col("product_id"),
    F.col("item_revenue").alias("gross_revenue"),
    F.col("referral_fee").alias("platform_fee"),        # Amazon referral commission (~15%)
    F.col("fba_fee").alias("fulfillment_fee"),           # FBA per-unit fee
    F.lit(0.0).alias("affiliate_commission"),
    F.col("net_revenue"),
)

# Backfill Amazon order dates from the orders table
df_fin_amazon = df_fin_amazon.alias("fin").join(
    df_amazon_clean.select(
        F.col("amazon-order-id"),
        F.col("order_date_utc").alias("amazon_date")
    ).alias("ord"),
    F.col("fin.order_id") == F.col("ord.amazon-order-id"),
    "left"
).withColumn(
    "order_date_utc", F.col("amazon_date")
).drop("amazon-order-id", "amazon_date")

# --- TikTok financials ---
df_fin_tiktok = df_tiktok_clean.select(
    F.col("Order ID").alias("order_id"),
    F.lit("TikTok").alias("platform"),
    F.col("order_date_utc"),
    F.col("product_id"),
    F.col("Net Sales").alias("gross_revenue"),
    (F.col("Transaction Fee") + F.col("Referral Fee")).alias("platform_fee"),
    F.lit(0.0).alias("fulfillment_fee"),
    F.col("Affiliate Commission").alias("affiliate_commission"),
    F.col("Settlement Amount").alias("net_revenue"),
)

# Union all financials
df_fact_financials = df_fin_shopify.unionByName(df_fin_amazon).unionByName(df_fin_tiktok)

print(f"fact_financials total: {df_fact_financials.count()} rows")
df_fact_financials.groupBy("platform").count().orderBy("platform").show()

# COMMAND ----------

# Cell 10: Build dim_customers — Customer Dimension (Shopify only)
#
# Only Shopify provides customer-level data (email, name, location).
# Amazon and TikTok don't share customer PII in their reports.
# This dimension enables repeat purchase analysis for the DTC channel.
#
# 只有 Shopify 有客户数据，所以 Repeat Purchase Rate 只能算 DTC 渠道的。
# 面试时可以主动说明这个 limitation，体现你理解数据边界。

df_dim_customers = df_shopify_clean.filter(
    F.col("customer_email") != "unknown"
).select(
    F.col("customer_email"),
    F.col("Billing Name").alias("customer_name"),
    F.col("Billing City").alias("city"),
    F.col("Billing Province").alias("state"),
    F.col("Billing Zip").alias("zip_code"),
).dropDuplicates(["customer_email"])

print(f"dim_customers: {df_dim_customers.count()} unique customers")
df_dim_customers.show(10, truncate=False)

# COMMAND ----------

# Cell 11: Write All Silver Tables to Lakehouse
#
# Save all dimension and fact tables as Delta tables.
# mode="overwrite" ensures idempotency — safe to re-run anytime.
#
# 写入顺序：先维度表，再事实表（虽然 Delta 没有外键约束，但养成好习惯）

silver_tables = {
    "dim_products": df_dim_products,
    "dim_customers": df_dim_customers,
    "fact_orders": df_fact_orders,
    "fact_financials": df_fact_financials,
}

for table_name, df in silver_tables.items():
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Saved {table_name}: {df.count():,} rows")

print("\nAll Silver tables created successfully!")

# COMMAND ----------

# Cell 12: Silver Layer Validation
#
# Quick data quality checks to verify the transformations worked correctly.

print("=" * 60)
print("Silver Layer Validation Report")
print("=" * 60)

# 1. Row counts
print("\n--- Table Row Counts ---")
for table in ["dim_products", "dim_customers", "fact_orders", "fact_financials"]:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")

# 2. Platform distribution in fact_orders
print("\n--- fact_orders: Orders by Platform ---")
spark.table("fact_orders").groupBy("platform").agg(
    F.count("*").alias("order_count"),
    F.round(F.sum("gross_sales"), 2).alias("total_gmv"),
    F.round(F.avg("gross_sales"), 2).alias("avg_order_value"),
).orderBy("platform").show()

# 3. Check for nulls in key columns
print("--- Null Check: product_id in fact_orders ---")
null_product = spark.table("fact_orders").filter(F.col("product_id").isNull()).count()
print(f"  Orders with null product_id: {null_product}")
print(f"  (Expected: ~15 from TikTok missing SKUs)")

# 4. Date range verification
print("\n--- Date Range by Platform ---")
spark.table("fact_orders").groupBy("platform").agg(
    F.min("order_date_utc").alias("earliest_order"),
    F.max("order_date_utc").alias("latest_order"),
).orderBy("platform").show(truncate=False)

# 5. Financial summary
print("--- fact_financials: Revenue by Platform ---")
spark.table("fact_financials").groupBy("platform").agg(
    F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
    F.round(F.sum("platform_fee"), 2).alias("platform_fees"),
    F.round(F.sum("fulfillment_fee"), 2).alias("fulfillment_fees"),
    F.round(F.sum("affiliate_commission"), 2).alias("affiliate_costs"),
    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
).orderBy("platform").show(truncate=False)
