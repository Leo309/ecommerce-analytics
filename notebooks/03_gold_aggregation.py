# Databricks notebook source / Microsoft Fabric Notebook
# MAGIC %md
# # Gold Layer — Business KPI Aggregations
#
# Transform Silver layer fact/dim tables into business-ready aggregation tables
# optimized for Power BI consumption.
#
# **Output tables:**
# - daily_sales_summary (Date × Platform granularity)
# - product_performance (Product × Platform × Month)
# - channel_analysis (Platform × Month)
# - customer_cohort (Cohort × Period, Shopify only)

# COMMAND ----------

# Cell 1: Imports

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Cell 2: daily_sales_summary — Time Series Analysis
#
# Grain: one row per date per platform
# Purpose: GMV trend lines, MoM growth, seasonality detection in Power BI
#
# 按日期+平台汇总，这是 Executive Summary 页面折线图的数据源。

df_orders = spark.table("fact_orders")

df_daily_sales = df_orders.filter(
    F.col("order_date_utc").isNotNull()
).groupBy(
    F.date_format("order_date_utc", "yyyy-MM-dd").alias("order_date"),
    "platform"
).agg(
    F.count("*").alias("order_count"),
    F.countDistinct("order_id").alias("unique_orders"),
    F.round(F.sum("gross_sales"), 2).alias("gmv"),
    F.round(F.sum("discount_amount"), 2).alias("total_discounts"),
    F.round(F.sum("total"), 2).alias("total_revenue"),
    F.round(F.avg("gross_sales"), 2).alias("avg_order_value"),
    F.sum("quantity").alias("units_sold"),
)

# Add month column for easier Power BI slicing
df_daily_sales = df_daily_sales.withColumn(
    "order_month", F.substring("order_date", 1, 7)  # "2025-03"
)

print(f"daily_sales_summary: {df_daily_sales.count()} rows")
df_daily_sales.orderBy("order_date", "platform").show(15, truncate=False)

# COMMAND ----------

# Cell 3: product_performance — SKU-Level Analytics
#
# Grain: one row per product per platform per month
# Purpose: identify top-selling products, compare performance across channels
#
# 按产品+平台+月份汇总，用于 Product Performance 报表页面。
# 面试时可以讨论：为什么用月粒度而不是日粒度？
# 答：产品维度的分析不需要每天看，月度趋势更有业务价值，也减少了数据量。

df_orders = spark.table("fact_orders")
df_products = spark.table("dim_products")

df_product_perf = df_orders.filter(
    F.col("product_id").isNotNull()
).withColumn(
    "order_month", F.date_format("order_date_utc", "yyyy-MM")
).groupBy(
    "product_id", "platform", "order_month"
).agg(
    F.count("*").alias("order_count"),
    F.sum("quantity").alias("units_sold"),
    F.round(F.sum("gross_sales"), 2).alias("gmv"),
    F.round(F.avg("gross_sales"), 2).alias("avg_selling_price"),
)

# Enrich with product attributes from dim_products
df_product_perf = df_product_perf.join(
    df_products.select("product_id", "product_name", "category", "base_price_usd"),
    on="product_id",
    how="left"
)

# Add refund data — count refunded orders per product
# Shopify: Financial Status = 'refunded', Amazon: order-status = 'Cancelled'
df_refunds = df_orders.filter(
    (F.col("order_status") == "refunded") | (F.col("order_status") == "Cancelled")
).withColumn(
    "order_month", F.date_format("order_date_utc", "yyyy-MM")
).groupBy(
    "product_id", "platform", "order_month"
).agg(
    F.count("*").alias("refund_count")
)

# Left join refunds to product performance
df_product_perf = df_product_perf.join(
    df_refunds,
    on=["product_id", "platform", "order_month"],
    how="left"
).fillna({"refund_count": 0})

# Calculate refund rate
df_product_perf = df_product_perf.withColumn(
    "refund_rate",
    F.round(F.col("refund_count") / F.col("order_count") * 100, 2)
)

print(f"product_performance: {df_product_perf.count()} rows")
df_product_perf.orderBy(F.desc("gmv")).show(15, truncate=False)

# COMMAND ----------

# Cell 4: channel_analysis — Platform Comparison
#
# Grain: one row per platform per month
# Purpose: compare revenue, fees, margins across Shopify vs Amazon vs TikTok
#
# 渠道对比分析，回答核心问题："哪个平台最赚钱？"
# 这是 Channel Deep Dive 报表页面的数据源。

df_financials = spark.table("fact_financials")

df_channel = df_financials.filter(
    F.col("order_date_utc").isNotNull()
).withColumn(
    "order_month", F.date_format("order_date_utc", "yyyy-MM")
).groupBy(
    "platform", "order_month"
).agg(
    F.count("*").alias("order_count"),
    F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
    F.round(F.sum("platform_fee"), 2).alias("platform_fees"),
    F.round(F.sum("fulfillment_fee"), 2).alias("fulfillment_fees"),
    F.round(F.sum("affiliate_commission"), 2).alias("affiliate_costs"),
    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
)

# Calculate key ratios
df_channel = df_channel.withColumn(
    "total_fees",
    F.round(F.col("platform_fees") + F.col("fulfillment_fees") + F.col("affiliate_costs"), 2)
).withColumn(
    "fee_ratio_pct",
    F.round(F.abs(F.col("total_fees")) / F.col("gross_revenue") * 100, 2)
).withColumn(
    "net_margin_pct",
    F.round(F.col("net_revenue") / F.col("gross_revenue") * 100, 2)
)

print(f"channel_analysis: {df_channel.count()} rows")
df_channel.orderBy("platform", "order_month").show(20, truncate=False)

# COMMAND ----------

# Cell 5: customer_cohort — Repeat Purchase Analysis (Shopify Only)
#
# Grain: one row per cohort month per period offset
# Purpose: understand customer retention and repeat purchase behavior
#
# 只有 Shopify 有客户邮箱，所以 cohort 分析只能做 DTC 渠道。
# Cohort analysis 是衡量客户留存的经典方法：
# - 把客户按"首次购买月份"分组（cohort）
# - 追踪每个 cohort 在后续月份是否有复购
#
# 面试高频题：如何衡量客户留存？Cohort analysis 的优势？
# 答：相比整体 retention rate，cohort 能看出不同时间获取的客户质量差异。

df_orders = spark.table("fact_orders")

# Only Shopify has customer email data for cohort analysis
df_shopify = df_orders.filter(
    (F.col("platform") == "Shopify") &
    (F.col("customer_email") != "unknown") &
    (F.col("order_date_utc").isNotNull())
).withColumn(
    "order_month", F.date_format("order_date_utc", "yyyy-MM")
)

# Step 1: Find each customer's first purchase month (cohort assignment)
df_first_purchase = df_shopify.groupBy("customer_email").agg(
    F.min("order_month").alias("cohort_month")
)

# Step 2: Join back to get cohort for each order
df_cohort_orders = df_shopify.join(
    df_first_purchase, on="customer_email", how="left"
)

# Step 3: Calculate months since first purchase (period offset)
df_cohort_orders = df_cohort_orders.withColumn(
    "months_since_first",
    F.months_between(
        F.to_date("order_month", "yyyy-MM"),
        F.to_date("cohort_month", "yyyy-MM")
    ).cast("int")
)

# Step 4: Aggregate — unique customers per cohort per period
df_customer_cohort = df_cohort_orders.groupBy(
    "cohort_month", "months_since_first"
).agg(
    F.countDistinct("customer_email").alias("active_customers"),
    F.count("*").alias("order_count"),
    F.round(F.sum("gross_sales"), 2).alias("cohort_revenue"),
)

# Add cohort size (total customers who first purchased in that month)
df_cohort_size = df_first_purchase.groupBy("cohort_month").agg(
    F.count("*").alias("cohort_size")
)

df_customer_cohort = df_customer_cohort.join(
    df_cohort_size, on="cohort_month", how="left"
).withColumn(
    "retention_rate_pct",
    F.round(F.col("active_customers") / F.col("cohort_size") * 100, 2)
)

print(f"customer_cohort: {df_customer_cohort.count()} rows")
df_customer_cohort.orderBy("cohort_month", "months_since_first").show(20, truncate=False)

# COMMAND ----------

# Cell 6: Write All Gold Tables to Lakehouse

gold_tables = {
    "daily_sales_summary": df_daily_sales,
    "product_performance": df_product_perf,
    "channel_analysis": df_channel,
    "customer_cohort": df_customer_cohort,
}

for table_name, df in gold_tables.items():
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Saved {table_name}: {df.count():,} rows")

print("\nAll Gold tables created successfully!")

# COMMAND ----------

# Cell 7: Gold Layer Validation

print("=" * 60)
print("Gold Layer Validation Report")
print("=" * 60)

# 1. Table row counts
print("\n--- Table Row Counts ---")
for table in ["daily_sales_summary", "product_performance", "channel_analysis", "customer_cohort"]:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")

# 2. Monthly GMV trend (top-level business health check)
print("\n--- Monthly GMV by Platform ---")
spark.table("daily_sales_summary").groupBy("order_month", "platform") \
    .agg(F.round(F.sum("gmv"), 2).alias("monthly_gmv")) \
    .orderBy("order_month", "platform").show(36, truncate=False)

# 3. Top 5 products by total GMV
print("--- Top 5 Products by GMV ---")
spark.table("product_performance").groupBy("product_name") \
    .agg(F.round(F.sum("gmv"), 2).alias("total_gmv")) \
    .orderBy(F.desc("total_gmv")).show(5, truncate=False)

# 4. Channel net margin comparison
print("--- Channel Net Margin (Full Year) ---")
spark.table("channel_analysis").groupBy("platform").agg(
    F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
    F.round(F.avg("fee_ratio_pct"), 2).alias("avg_fee_ratio_pct"),
    F.round(F.avg("net_margin_pct"), 2).alias("avg_net_margin_pct"),
).orderBy("platform").show(truncate=False)

# 5. Cohort retention snapshot
print("--- Customer Cohort: First 3 Months Retention ---")
spark.table("customer_cohort").filter(
    F.col("months_since_first") <= 3
).orderBy("cohort_month", "months_since_first").show(20, truncate=False)
