# Fabric Hands-On Guide

Step-by-step guide for building the Medallion architecture on Microsoft Fabric.

## Prerequisites

- Microsoft Fabric trial activated (25-day limit)
- Data files generated locally (`data/raw/`)

---

## Step 2: Bronze Layer Setup

### 2.1 Create Workspace

> **Why workspace first?** Workspace 是 Fabric 的顶层组织单元，类似于一个"项目文件夹"。
> 真实工作中，一个 team 通常按项目或业务域划分 workspace。

1. 打开 [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. 左侧栏点击 **Workspaces** → **+ New workspace**
3. 配置：
   - **Name:** `EcommerceAnalytics`
   - **Description:** `Multi-channel DTC brand analytics — Medallion architecture`
   - License mode: 选 **Trial** (or Fabric capacity)
4. 点击 **Apply**

> 💡 **面试考点：** Workspace 的权限模型（Admin / Member / Contributor / Viewer）
> 是企业数据治理的基础。真实项目中会有 Dev / Staging / Prod 三个 workspace。

### 2.2 Create Lakehouse

> **Why Lakehouse?** Lakehouse = Data Lake 的灵活性 + Data Warehouse 的结构化查询能力。
> 底层是 Delta Lake 格式（Parquet + transaction log），支持 ACID 事务和 schema evolution。
> 这是 2024-2026 年数据平台的主流架构选择。

1. 在 workspace 里点击 **+ New item**
2. 选择 **Lakehouse**
3. 命名：`lh_ecommerce`（前缀 `lh_` 是常见命名规范，一眼看出是 Lakehouse）
4. 点击 **Create**

你会看到两个区域：
- **Tables/** — Delta 表存放区（结构化，可 SQL 查询）
- **Files/** — 原始文件存放区（非结构化，任意格式）

> 💡 **类比：** Tables 像是整理好的 Excel 表格，Files 像是随便丢文件的文件夹。
> Bronze 层的流程是：文件先进 Files，再用 Notebook 转成 Tables。

### 2.3 Upload Raw Data Files

> **Why 手动上传？** 真实场景中数据会通过 Pipeline / API / Connector 自动进来。
> 但对于模拟数据项目，手动上传是最合理的做法。
> 面试时可以说："In production, I'd set up a Data Pipeline with scheduled ingestion."

1. 在 Lakehouse 页面，点击 **Files/** 旁边的 **...**(三点菜单)
2. 选择 **New subfolder** → 创建 `raw`
3. 进入 `raw/` 文件夹
4. 点击 **Upload** → **Upload files**
5. 上传这 4 个文件（从你本地 `data/raw/` 目录）：
   - `shopify_orders.csv`
   - `amazon_orders.tsv`
   - `amazon_settlement.tsv`
   - `tiktok_settlement.xlsx`
6. 等待上传完成，确认 4 个文件都在 `Files/raw/` 下

### 2.4 Create Bronze Notebook

> **Why Notebook?** Notebook 是 Analytics Engineer 在 Fabric/Databricks 中最常用的工具。
> 它结合了代码执行 + 文档说明 + 可视化，非常适合 ETL 开发和调试。
> 真实工作中，成熟团队会用 Notebook 做原型开发，稳定后迁移到 Pipeline。

1. 回到 workspace 页面
2. 点击 **+ New item** → 选 **Notebook**
3. 重命名为 `01_bronze_ingestion`（数字前缀表示执行顺序，业界惯例）
4. **关键步骤：** 关联 Lakehouse
   - 左侧 **Lakehouses** 面板 → **Add** → 选择 `lh_ecommerce`
   - 这样 Notebook 里的 Spark 代码就能直接读写这个 Lakehouse

### 2.5 写 Bronze Ingestion 代码

在 Notebook 里，**逐个 cell 输入并运行**（不要一次性全贴，这样你能看到每一步的结果）：

---

**Cell 1: 配置和说明**（Markdown cell，点击 cell 类型切换为 Markdown）

```markdown
# Bronze Layer Ingestion
Raw files → Delta tables with no transformation.
The Bronze layer preserves the original data exactly as received.
```

---

**Cell 2: Read Shopify Orders**

```python
# Read Shopify CSV — note: inferSchema=True lets Spark auto-detect data types
# header=True uses the first row as column names
df_shopify = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/raw/shopify_orders.csv")

print(f"Shopify orders: {df_shopify.count()} rows")
df_shopify.printSchema()
df_shopify.show(5, truncate=False)
```

> 你应该看到 ~4,516 rows，25 个字段。花 30 秒看看 schema 对不对。

---

**Cell 3: Read Amazon Orders**

```python
# Read Amazon TSV — delimiter is tab (\t), not comma
df_amazon_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .load("Files/raw/amazon_orders.tsv")

print(f"Amazon orders: {df_amazon_orders.count()} rows")
df_amazon_orders.printSchema()
df_amazon_orders.show(5, truncate=False)
```

---

**Cell 4: Read Amazon Settlement**

```python
# Read Amazon Settlement TSV (EAV format — each order has multiple rows)
df_amazon_settlement = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .load("Files/raw/amazon_settlement.tsv")

print(f"Amazon settlement: {df_amazon_settlement.count()} rows")
df_amazon_settlement.printSchema()
df_amazon_settlement.show(10, truncate=False)
```

> 注意观察 EAV 格式：同一个 order-id 出现多次，每行是不同的费用类型。
> 这是 Amazon 真实报表的格式，Silver 层需要 pivot 成宽表。

---

**Cell 5: Read TikTok Settlement**

```python
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
```

> 💡 **面试考点：** 为什么不直接用 `spark.read.format("xlsx")`？
> 因为 Spark 原生不支持 Excel 格式。真实项目中，最佳实践是让数据源
> 导出为 CSV/Parquet，或者用 pandas 做桥接。

---

**Cell 6: Write to Delta Tables**

```python
# Save all dataframes as Delta tables in the Lakehouse
# mode="overwrite" ensures idempotency — re-running won't create duplicates

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
```

> 💡 **为什么用 `mode("overwrite")`？**
> 这保证了 **幂等性（idempotency）** — 不管跑多少次，结果都一样。
> 这是数据工程的核心原则之一。面试高频考点。

---

**Cell 7: Verify Bronze Tables**

```python
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
```

---

### 2.6 验证和截图

运行完所有 Cell 后：

1. 回到 Lakehouse 页面，刷新
2. 在 **Tables/** 下应该能看到 5 张 Delta 表
3. 点击任意表，可以预览数据
4. **截图保存**（Fabric trial 过期后这些就没了）：
   - Lakehouse Tables 列表截图
   - Notebook 运行结果截图
   - 保存到 `reports/` 目录

### 2.7 Export Notebook

> **Why 导出？** Fabric trial 过期后 Notebook 会消失。
> 导出到 GitHub 是永久存档，也是面试官会看的代码。

1. 在 Notebook 页面，点击右上角 **...** → **Export** → **Export as .py**
2. 下载的文件重命名为 `01_bronze_ingestion.py`
3. 放到项目 `notebooks/` 目录
4. commit 到 GitHub

---

## Step 3: Silver Layer — Data Cleaning & Star Schema

> **Why Silver?** Bronze 是"原始照片"，Silver 是"修好的照片"。
> 这一层是整个项目含金量最高的部分——数据清洗、标准化、建模。
> 面试中被问到最多的就是这一层的处理逻辑。

### 3.1 Create Silver Notebook

1. 回到 workspace 页面
2. 点击 **+ New item** → **Notebook**
3. 重命名为 `02_silver_cleaning`
4. 关联 Lakehouse → 选 `lh_ecommerce`

### 3.2 逐 Cell 运行代码

> 如果 Bronze 层遇到了列名空格问题，先在第一个 Cell 运行 Column Mapping 配置：
> ```python
> spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
> spark.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "2")
> spark.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "5")
> ```

---

**Cell 1: Configuration & Imports**

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CAD to USD fixed exchange rate
# 实际项目中会调 API 获取历史汇率，这里用固定汇率简化
CAD_TO_USD = 0.74
```

---

**Cell 2: SKU Mapping Table — Cross-Platform Product Master**

> **Business context:** In real e-commerce, each sales channel uses its own SKU naming convention.
> A SKU mapping table is essential for unified cross-channel product analytics.
> This creates a "Conformed Dimension" — a core concept in Kimball dimensional modeling.
>
> 三个平台的 SKU 命名完全不同，必须建映射表才能做跨平台产品分析。

```python
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
```

Expected output: 14 rows，每行一个产品，显示三平台 SKU 对照。

---

**Cell 3: dim_products — Product Dimension Table**

> **Star schema design:** The product dimension is the Single Source of Truth (SSOT) for
> product attributes. All fact tables reference `product_id` from this table.
>
> 产品维度表是星型模型的核心，所有 fact 表通过 product_id 关联到这里。

```python
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
```

Expected output: `dim_products: 14 products`

---

**Cell 4: Clean Shopify Orders**

> **Transformations:**
> 1. Filter out voided/test orders — real Shopify data often has test orders from ops team
> 2. Parse ISO datetime with timezone offset → UTC timestamp
> 3. Map Shopify SKU → unified `product_id` via lookup join
> 4. Handle missing customer emails — flag as 'unknown' (guest checkout scenario)

```python
df_shopify_raw = spark.table("shopify_orders_raw")

# Step 1: Remove test orders (Financial Status = 'voided')
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
```

Expected output:
- `Removed ~45-70 voided/test orders from Shopify`
- 数据表显示 `order_date_utc` 和 `product_id` 已填充

---

**Cell 5: Clean Amazon Orders**

> **Transformations:**
> 1. Deduplicate orders using `ROW_NUMBER()` window function
> 2. Amazon dates are already clean (ISO UTC, auto-parsed by Spark inferSchema)
> 3. Map Amazon SKU → unified `product_id`
>
> **面试考点：** 去重的三种方式：
> - `DISTINCT` — 整行完全相同才去重
> - `GROUP BY` — 按 key 聚合，但丢失明细
> - `ROW_NUMBER()` — 最灵活，可以选择保留哪一条（最新/最早/特定条件）

```python
df_amazon_raw = spark.table("amazon_orders_raw")

# Step 1: Deduplicate — keep first occurrence per order_id
# Window function: partition by order_id, order by date, keep row_number = 1
window_dedup = Window.partitionBy("amazon-order-id").orderBy("purchase-date")
df_amazon_dedup = df_amazon_raw.withColumn(
    "_row_num", F.row_number().over(window_dedup)
).filter(F.col("_row_num") == 1).drop("_row_num")

dup_count = df_amazon_raw.count() - df_amazon_dedup.count()
print(f"Removed {dup_count} duplicate Amazon orders")

# Step 2: Date is already timestamp type from Bronze (inferSchema detected it)
# Just rename for consistency across platforms
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
```

Expected output: `Removed ~25 duplicate Amazon orders`

---

**Cell 6: Clean TikTok Orders**

> **Transformations:**
> 1. Parse `MM/DD/YYYY` date string → UTC timestamp (most common "dirty" date format)
> 2. Convert CAD → USD for cross-border orders (~5% of total)
> 3. Map TikTok SKU → unified `product_id`, handle ~1% missing SKUs
>
> **Business context:** TikTok Shop is a newer channel. Data exports are less mature
> than Shopify/Amazon, which is realistic — newer platforms tend to have messier data.

```python
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
```

Expected output:
- `TikTok date parsing failures: 0`
- `TikTok CAD orders to convert: ~75`
- `TikTok orders with unmapped/missing SKU: ~15`

---

**Cell 7: Amazon Settlement Pivot — EAV to Wide Table**

> **This is the most technically interesting transformation in the project.**
>
> Amazon Settlement uses Entity-Attribute-Value (EAV) format:
> ```
> order-id | amount-description      | amount
> 123      | Principal               | 24.99
> 123      | Tax                     | 2.12
> 123      | Commission              | -3.75
> 123      | FBAPerUnitFulfillmentFee | -4.50
> ```
>
> We PIVOT this into a wide table (one row per order):
> ```
> order-id | item_revenue | item_tax | referral_fee | fba_fee | net_revenue
> 123      | 24.99        | 2.12     | -3.75        | -4.50   | 18.86
> ```
>
> **面试高频题：** EAV 模式的优缺点？
> - 优点：schema 灵活，新增费用类型不需要 ALTER TABLE
> - 缺点：查询复杂，需要 pivot，性能差（大量 self-join）
> - 适用场景：属性数量不固定的系统（电商费用、医疗记录、配置系统）

```python
df_settlement_raw = spark.table("amazon_settlement_raw")

# Pivot: group by order-id + sku, spread amount-description into columns
df_settlement_pivot = df_settlement_raw.groupBy("settlement-id", "order-id", "sku") \
    .pivot("amount-description") \
    .agg(F.sum("amount"))

# Rename columns to standard financial terms
df_settlement_pivot = df_settlement_pivot \
    .withColumnRenamed("Principal", "item_revenue") \
    .withColumnRenamed("Tax", "item_tax") \
    .withColumnRenamed("Shipping", "shipping_revenue") \
    .withColumnRenamed("Commission", "referral_fee") \
    .withColumnRenamed("FBAPerUnitFulfillmentFee", "fba_fee")

# Fill nulls with 0 (not all orders have every fee type, e.g., MFN orders have no FBA fee)
for col_name in ["item_revenue", "item_tax", "shipping_revenue", "referral_fee", "fba_fee"]:
    df_settlement_pivot = df_settlement_pivot.fillna({col_name: 0})

# Calculate net revenue per order (revenue minus all fees)
# Note: referral_fee and fba_fee are already negative values
df_settlement_pivot = df_settlement_pivot.withColumn(
    "net_revenue",
    F.round(
        F.col("item_revenue") + F.col("shipping_revenue") +
        F.col("referral_fee") + F.col("fba_fee"),
        2
    )
)

print(f"Amazon settlement (pivoted): {df_settlement_pivot.count()} rows")
df_settlement_pivot.show(10, truncate=False)
```

Expected output: ~4,700+ rows (one per order-SKU combination), 每行有 revenue + fees + net_revenue

---

**Cell 8: fact_orders — Unified Order Fact Table**

> **Core of the star schema.** Three platforms UNION into one table with standardized columns.
> This is the "Single Source of Truth" (SSOT) for all order analytics.
>
> Key design decisions:
> - Amazon doesn't share customer emails → set as 'unknown'
> - TikTok doesn't include ship-to state → set as 'unknown'
> - These data limitations should be documented, not hidden
>
> 三个平台的订单合并成统一事实表——字段名统一、类型统一。

```python
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
    F.lit("unknown").alias("customer_email"),  # Amazon doesn't share customer PII
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
    F.lit("unknown").alias("customer_email"),  # TikTok doesn't share customer PII
    F.lit("unknown").alias("state"),  # TikTok settlement has no ship-to state
    F.lit("USD").alias("currency"),
)

# Union all three platforms into one unified fact table
df_fact_orders = df_fact_shopify.unionByName(df_fact_amazon).unionByName(df_fact_tiktok)

print(f"fact_orders total: {df_fact_orders.count()} rows")
print("\nBreakdown by platform:")
df_fact_orders.groupBy("platform").count().orderBy("platform").show()
```

Expected output:
```
Platform | count
---------|------
Amazon   | ~4,975
Shopify  | ~4,450
TikTok   | 1,500
```

---

**Cell 9: fact_financials — Revenue & Fee Breakdown**

> **Business context:** This table answers the key question:
> "How much do we actually keep from each sales channel?"
>
> Fee structures vary dramatically:
> - Shopify: highest margin (only payment processing ~2.9%, not in data)
> - Amazon: ~15% referral fee + $3-6.5 FBA fulfillment fee per unit
> - TikTok: 2% transaction + 5% referral + 10-20% affiliate creator commission
>
> This is the foundation for the Channel Deep Dive report in Power BI.

```python
# --- Shopify financials (no platform fee data in Shopify export) ---
df_fin_shopify = df_shopify_clean.select(
    F.col("Name").alias("order_id"),
    F.lit("Shopify").alias("platform"),
    F.col("order_date_utc"),
    F.col("product_id"),
    F.col("Subtotal").cast("double").alias("gross_revenue"),
    F.lit(0.0).alias("platform_fee"),
    F.lit(0.0).alias("fulfillment_fee"),
    F.lit(0.0).alias("affiliate_commission"),
    F.col("Subtotal").cast("double").alias("net_revenue"),
)

# --- Amazon financials (from pivoted settlement report) ---
df_fin_amazon = df_settlement_pivot.join(
    df_sku_mapping.select("amazon_sku", "product_id"),
    df_settlement_pivot["sku"] == df_sku_mapping["amazon_sku"],
    "left"
).select(
    F.col("order-id").alias("order_id"),
    F.lit("Amazon").alias("platform"),
    F.lit(None).cast("timestamp").alias("order_date_utc"),  # Backfill below
    F.col("product_id"),
    F.col("item_revenue").alias("gross_revenue"),
    F.col("referral_fee").alias("platform_fee"),
    F.col("fba_fee").alias("fulfillment_fee"),
    F.lit(0.0).alias("affiliate_commission"),
    F.col("net_revenue"),
)

# Backfill Amazon order dates from the clean orders table
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
```

Expected output: 三个平台的财务数据都在，可以对比 gross_revenue vs net_revenue 看各平台利润率。

---

**Cell 10: dim_customers — Customer Dimension (Shopify only)**

> **Data limitation:** Only Shopify provides customer PII (email, name, address).
> Amazon and TikTok marketplace reports do not include customer contact information.
> Therefore, Repeat Purchase Rate can only be calculated for the DTC channel.
>
> 面试时主动说明数据局限性，体现你理解 data boundary。

```python
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
```

Expected output: ~2,800-2,950 unique customers

---

**Cell 11: Write All Silver Tables to Lakehouse**

> 写入顺序：先维度表（dim），再事实表（fact）。
> 虽然 Delta Lake 没有外键约束，但养成好习惯——维度先于事实。
> `mode("overwrite")` 保证幂等性（idempotency）— 重复跑结果一致。

```python
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
```

---

**Cell 12: Silver Layer Validation**

> 最后跑一次完整的数据质量验证。**这个输出是很好的截图素材。**

```python
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

# 3. Check for nulls in critical columns
print("--- Null Check: product_id in fact_orders ---")
null_product = spark.table("fact_orders").filter(F.col("product_id").isNull()).count()
print(f"  Orders with null product_id: {null_product}")
print(f"  (Expected: ~15 from TikTok missing SKUs)")

# 4. Date range verification — all should be within 2025
print("\n--- Date Range by Platform ---")
spark.table("fact_orders").groupBy("platform").agg(
    F.min("order_date_utc").alias("earliest_order"),
    F.max("order_date_utc").alias("latest_order"),
).orderBy("platform").show(truncate=False)

# 5. Financial summary — the money question
print("--- fact_financials: Revenue by Platform ---")
spark.table("fact_financials").groupBy("platform").agg(
    F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
    F.round(F.sum("platform_fee"), 2).alias("platform_fees"),
    F.round(F.sum("fulfillment_fee"), 2).alias("fulfillment_fees"),
    F.round(F.sum("affiliate_commission"), 2).alias("affiliate_costs"),
    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
).orderBy("platform").show(truncate=False)
```

Expected output:
1. 4 张 Silver 表的行数
2. 各平台订单数、GMV、AOV
3. null product_id ~15 条（TikTok 缺失 SKU）
4. 日期范围全在 2025 年内
5. 各平台收入 vs 费用 vs 净收入对比

### 3.3 验证和截图

运行完所有 Cell 后：

1. 回到 Lakehouse，刷新 Tables 列表
2. 应该看到 Bronze (5张) + Silver (4张) = 9 张表
3. 截图保存：
   - `silver_tables_overview.png` — Lakehouse 表列表
   - `silver_validation_report.png` — Cell 12 的验证输出
   - `silver_settlement_pivot.png` — Cell 7 EAV pivot 结果（面试亮点）

### 3.4 Export Notebook

1. Notebook 页面 → **...** → **Export** → **Export as .py**
2. 重命名为 `02_silver_cleaning.py`
3. 放到 `notebooks/` 目录，commit 到 GitHub

---

## 操作顺序 Checklist

### Step 2: Bronze Layer
- [ ] 创建 Workspace `EcommerceAnalytics`
- [ ] 创建 Lakehouse `lh_ecommerce`
- [ ] 上传 4 个数据文件到 `Files/raw/`
- [ ] 创建 Notebook `01_bronze_ingestion`
- [ ] 关联 Lakehouse 到 Notebook
- [ ] 逐 Cell 运行代码，确认每步结果
- [ ] 验证 5 张 Bronze 表存在且行数正确
- [ ] 截图存档
- [ ] 导出 Notebook 到 GitHub

### Step 3: Silver Layer
- [ ] 创建 Notebook `02_silver_cleaning`
- [ ] 关联 Lakehouse `lh_ecommerce`
- [ ] 运行 Column Mapping 配置（如果需要）
- [ ] 逐 Cell 运行 12 个代码块
- [ ] 验证 4 张 Silver 表存在且数据正确
- [ ] 截图存档（重点截 Validation Report 和 Pivot 结果）
- [ ] 导出 Notebook 到 GitHub
