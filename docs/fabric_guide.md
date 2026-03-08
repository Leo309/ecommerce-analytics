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

### 3.2 Column Mapping 配置

> 如果 Bronze 层遇到了列名空格问题，Silver 也需要同样的配置。
> 把这段放在 Notebook 最开头运行：

```python
# Enable column mapping for Delta tables with special characters in column names
spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
spark.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "2")
spark.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "5")
```

### 3.3 逐 Cell 运行代码

完整代码在 `notebooks/02_silver_cleaning.py`，共 12 个 Cell。以下是每个 Cell 的说明和你应该看到的结果：

---

**Cell 1: Configuration & Imports**

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAD_TO_USD = 0.74
```

---

**Cell 2: SKU Mapping Table**

> **Business context:** Cross-platform SKU mapping is the foundation of unified product analytics.
> In real e-commerce, each sales channel uses its own SKU naming convention.
> This mapping table creates a "Conformed Dimension" — a Kimball data warehouse concept.

```python
# See notebooks/02_silver_cleaning.py Cell 2 for full code
```

Expected output: 14 rows, one per product.

---

**Cell 3: dim_products — Product Dimension**

> **Star schema design:** All fact tables will reference `product_id` from this dimension.
> This is the Single Source of Truth (SSOT) for product attributes.

Expected output: `dim_products: 14 products`

---

**Cell 4: Clean Shopify Orders**

> **Transformations:**
> 1. Filter out voided/test orders — `Financial Status = 'voided'` (~1.5%)
> 2. Parse ISO datetime with timezone offset → UTC timestamp
> 3. Map Shopify SKU → unified `product_id` via lookup join
> 4. Handle missing emails — flag as 'unknown' (guest checkout scenario)

Expected output:
- `Removed ~45-70 voided/test orders from Shopify`
- Remaining rows with `order_date_utc` and `product_id` populated

---

**Cell 5: Clean Amazon Orders**

> **Transformations:**
> 1. Deduplicate using `ROW_NUMBER()` window function — classic interview question
> 2. Amazon dates are already clean (ISO UTC, auto-parsed by Spark)
> 3. Map Amazon SKU → unified `product_id`
>
> **面试考点：** 去重的三种方式：`DISTINCT` / `GROUP BY` / `ROW_NUMBER()`
> `ROW_NUMBER()` 最灵活，可以选择保留哪一条（最新/最早/特定条件）

Expected output:
- `Removed ~25 duplicate Amazon orders`

---

**Cell 6: Clean TikTok Orders**

> **Transformations:**
> 1. Parse `MM/DD/YYYY` date string → UTC timestamp（最常见的"脏"日期格式）
> 2. Convert CAD → USD for ~5% cross-border orders（货币标准化）
> 3. Map TikTok SKU → `product_id`，handle ~1% missing SKUs
>
> **Business context:** TikTok Shop is a newer channel. Data exports are less mature
> than Shopify/Amazon, which is realistic — newer platforms tend to have messier data.

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
> We PIVOT this into a wide table:
> ```
> order-id | item_revenue | item_tax | referral_fee | fba_fee | net_revenue
> 123      | 24.99        | 2.12     | -3.75        | -4.50   | 18.86
> ```
>
> **面试高频题：** EAV 模式的优缺点？
> - 优点：灵活，新增类型不需要改 schema
> - 缺点：查询复杂，需要 pivot，性能差
> - 适用场景：属性数量不固定的系统（如电商费用、医疗记录）

Expected output: ~4,700+ rows (one per order-SKU combination)

---

**Cell 8: fact_orders — Unified Order Fact Table**

> **Core of the star schema.** Three platforms UNION into one table with standardized columns.
> This is the "Single Source of Truth" for all order analytics.
>
> Key design decisions:
> - Amazon doesn't share customer emails → set as 'unknown'
> - TikTok doesn't include ship-to state → set as 'unknown'
> - These limitations should be documented, not hidden

Expected output:
```
Platform | order_count
---------|------------
Amazon   | ~4,975
Shopify  | ~4,450
TikTok   | 1,500
```

---

**Cell 9: fact_financials — Revenue & Fee Breakdown**

> **Business context:** This table answers "how much do we actually keep from each platform?"
> - Shopify: highest margin (no marketplace fees in data)
> - Amazon: ~15% referral fee + $3-6.5 FBA fee per unit
> - TikTok: 2% transaction + 5% referral + 10-20% creator commission
>
> This is the foundation for the Channel Deep Dive report in Power BI.

Expected output: financials for all three platforms with fee breakdowns.

---

**Cell 10: dim_customers — Customer Dimension (Shopify only)**

> **Limitation:** Only Shopify provides customer PII (email, name, address).
> Amazon and TikTok don't share this data in their reports.
> Repeat Purchase Rate can only be calculated for the DTC channel.
>
> In interviews, proactively mentioning data limitations shows maturity.

Expected output: ~2,800-2,950 unique customers

---

**Cell 11: Write All Silver Tables**

```python
# Writes: dim_products, dim_customers, fact_orders, fact_financials
# mode="overwrite" ensures idempotency
```

---

**Cell 12: Silver Layer Validation**

Run this last. You should see:
1. Row counts for all 4 Silver tables
2. Orders by platform with GMV and AOV
3. Null product_id count (~15, from TikTok missing SKUs)
4. Date ranges (all within 2025)
5. Revenue breakdown with fees by platform

**This validation output is a great screenshot for your portfolio.**

### 3.4 验证和截图

运行完所有 Cell 后：

1. 回到 Lakehouse，刷新 Tables 列表
2. 应该看到 Bronze (5张) + Silver (4张) = 9 张表
3. 截图保存：
   - `silver_tables_overview.png` — Lakehouse 表列表
   - `silver_validation_report.png` — Cell 12 的验证输出
   - `silver_sku_mapping.png` — SKU 映射表展示
   - `silver_settlement_pivot.png` — EAV pivot 结果（面试亮点）

### 3.5 Export Notebook

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
