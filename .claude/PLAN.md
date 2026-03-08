# E-Commerce Growth Analytics Platform — Fabric 项目方案

## Context
Hao Li 求职 Analytics Engineer / Data Analyst 方向，需要一个展示完整数据工程流程的项目。基于 Microsoft Fabric（25 天 trial），模拟 DTC 品牌多平台电商数据（Shopify + Amazon + TikTok Shop），构建 Medallion 架构，最终产出 Power BI 报表。

**核心目标：** 尽快实现一个包装好、符合岗位要求的项目，在 trial 过期前完成并截图存档。

## 整体策略：先 GitHub 仓库，再 Fabric 操作

**先建 GitHub 仓库的原因：**
- Fabric trial 25 天后可能无法访问，GitHub 是永久存档
- 面试官第一时间看的是 GitHub README + 代码，不是 Fabric workspace
- 先建仓库可以边做边 commit，避免最后赶着存档
- README 和架构图写好后，Fabric 上的操作就有明确方向

## 项目结构（GitHub 仓库）

```
ecommerce-analytics/
├── README.md                    # 项目说明、架构图、KPI 定义、截图
├── data/
│   ├── raw/                     # 生成的模拟原始数据
│   │   ├── shopify_orders.csv
│   │   ├── amazon_orders.tsv
│   │   ├── amazon_settlement.tsv
│   │   └── tiktok_settlement.xlsx
│   └── generate_data.py         # 数据生成脚本
├── notebooks/                   # Fabric Notebook 导出存档
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleaning.py
│   └── 03_gold_aggregation.py
├── sql/                         # 关键 SQL 转换逻辑
│   └── transformations.sql
├── reports/                     # Power BI 截图
│   ├── executive_summary.png
│   ├── product_performance.png
│   └── channel_analysis.png
├── docs/
│   ├── architecture.md          # 架构设计文档
│   ├── data_dictionary.md       # 数据字典
│   └── kpi_definitions.md       # KPI 定义和计算逻辑
└── ai-weekly-report/            # AI 周报脚本（加分项）
    └── generate_report.py
```

## 数据源设计

模拟一个美国 DTC 咖啡浓缩液品牌（类似 Javy Coffee），2025 全年数据（Jan-Dec）。

### 产品线（~15 个 SKU）
- **Coffee Concentrates:** Original, Vanilla, Mocha, Caramel, Decaf（主力产品，各有 1x/3x 包装）
- **Accessories:** Travel Mug, Cold Brew Kit
- **Bundles:** Starter Kit, Variety Pack

### 数据量预估
- Shopify: ~3,000 单/年（DTC 自有渠道，客单价 $25-45）
- Amazon: ~5,000 单/年（最大渠道，客单价 $18-35，FBA 为主）
- TikTok Shop: ~1,500 单/年（新兴渠道，达人带货为主，客单价 $15-30）

### Shopify Orders Export（CSV，逗号分隔）
- 主站 DTC 自有渠道，~3,000 单/年
- 关键字段：Name, Email, Financial Status, Paid at, Currency (USD), Subtotal, Shipping, Taxes, Total, Discount Code, Discount Amount, Lineitem quantity/name/price/sku, Billing/Shipping 地址
- 特点：有完整客户 PII，多 line item 同一订单多行
- 折扣码：WELCOME10, SUBSCRIBE20, HOLIDAY25 等

### Amazon All Orders Report（TSV，Tab 分隔）
- 最大渠道，~5,000 单/年，FBA 为主
- 关键字段：amazon-order-id, purchase-date, order-status, fulfillment-channel (AFN/MFN), sku, asin, quantity, currency, item-price, item-tax, shipping-price, ship-city/state/postal-code/country
- 特点：无客户邮箱/姓名，日期格式 ISO UTC
- SKU 命名风格：JAVY-ORIG-6OZ（跟 Shopify 的 original-concentrate-6oz 不同）

### Amazon Settlement Report V2（TSV）
- 财务视角，EAV 模式（需要 pivot）
- 关键字段：settlement-id, order-id, sku, amount-type, amount-description, amount
- 用途：计算 FBA 费用、佣金、净收入

### TikTok Shop Settlement Report（XLSX，多 sheet）
- 新兴渠道，~1,500 单/年，达人带货为主
- 关键字段：Order/adjustment ID, SKU ID, Product name, Quantity, Gross sales, Net sales, Seller discount, Transaction fee, Referral fee, Affiliate commission
- 特点：有达人佣金数据，费用拆分极细
- SKU 命名风格：TS-ORIGINAL-6（又不一样）

### 故意制造的数据质量问题（Silver 层展示用）
- 日期格式不统一（Shopify ISO+时区 vs Amazon ISO UTC vs TikTok MM/DD/YYYY）
- Amazon Settlement 用 EAV 模式需要 pivot 成宽表
- 同一产品三个平台 SKU 不同（需 SKU mapping 表）
  - Shopify: `original-concentrate-6oz`
  - Amazon: `JAVY-ORIG-6OZ`
  - TikTok: `TS-ORIGINAL-6`
- 少量空值、重复订单
- TikTok 部分订单含 CAD 货币（加拿大跨境）
- Shopify 有少量测试订单（Financial Status = voided）

## Medallion 架构

### Bronze 层（Raw Data Landing）
- **Fabric 操作：** Data Pipeline + Copy Activity 上传 CSV/TSV/XLSX 到 Lakehouse
- **结果：** 4 张原始表，字段和格式保持原样

### Silver 层（Cleaned & Standardized）
- **Fabric 操作：** Notebook（Spark SQL + PySpark）
- **处理逻辑：**
  - 日期标准化为 UTC timestamp
  - 货币统一为 USD
  - Amazon Settlement pivot 成宽表
  - SKU 标准化映射
  - 去重、空值处理
- **结果：** 统一的 dim_products, dim_customers, fact_orders, fact_order_items, fact_financials

### Gold 层（Business Aggregations）
- **Fabric 操作：** Notebook（Spark SQL）
- **KPI 宽表：**
  - daily_sales_summary（按日期+平台汇总）
  - product_performance（按 SKU 汇总）
  - channel_analysis（按渠道对比）
  - customer_cohort（基于 Shopify 客户数据）

## KPI 体系（8-10 个）

### Revenue & Growth
1. **Total GMV** — 按平台拆分
2. **Net Revenue** — 扣除平台佣金、FBA 费用、达人佣金
3. **Revenue Growth Rate** — MoM 月环比
4. **AOV (Average Order Value)** — 按平台对比

### Product Performance
5. **Top SKUs by Revenue**
6. **Return/Refund Rate** — 按平台对比
7. **Platform Fee Ratio** — 各平台费用占比

### Customer & Channel
8. **Channel Revenue Mix** — Shopify vs Amazon vs TikTok 占比趋势
9. **Repeat Purchase Rate** — 仅 Shopify（有客户数据）
10. **TikTok Creator ROI** — 达人佣金 vs 达人带来的 GMV

## Power BI 报表（3 页）

1. **Executive Summary** — GMV 趋势、渠道占比、MoM 增长、核心 KPI 卡片
2. **Product Performance** — SKU 排名、退货率、各平台产品表现对比
3. **Channel Deep Dive** — 三平台费用结构对比、净利润率、TikTok 达人 ROI

## 实施步骤

### Step 1: GitHub 仓库 + 数据生成（本地，~2 小时）
1. 创建 GitHub 仓库 `ecommerce-analytics`
2. 写 README 骨架（项目介绍、架构图占位）
3. 写 Python 数据生成脚本，生成 4 个文件
4. 写数据字典和 KPI 定义文档
5. 提交推送

### Step 2: Fabric Bronze 层（Fabric Web，~1 小时）
1. 创建 Workspace + Lakehouse
2. 上传 4 个数据文件到 Lakehouse Files
3. 用 Notebook 将文件读入 Delta 表
4. 截图存档

### Step 3: Fabric Silver 层（Fabric Web，~3 小时）
1. 创建 Notebook 做数据清洗和标准化
2. 输出 dim/fact 表到 Lakehouse Tables
3. 导出 Notebook 代码到 GitHub

### Step 4: Fabric Gold 层（Fabric Web，~2 小时）
1. 创建 Notebook 做 KPI 聚合
2. 输出 Gold 表
3. 导出代码到 GitHub

### Step 5: Power BI 报表（Fabric Web，~3 小时）
1. 用 DirectLake 模式连接 Gold 表
2. 搭建 3 页报表
3. 截图存入 GitHub

### Step 6: 包装和文档（本地，~2 小时）
1. 完善 README（加截图、架构图、KPI 说明）
2. 更新 portfolio 网站的项目链接
3. 重新部署 portfolio

### Step 7（加分项）: AI 周报
- 用 Python 调 LLM API 基于 KPI 数据生成自然语言周报

## 验证方式
1. GitHub README 有架构图、截图、清晰的项目说明
2. 数据生成脚本可以本地运行，产出符合真实格式的数据文件
3. Fabric Notebook 代码逻辑清晰，有注释
4. Power BI 报表截图展示 3 页完整看板
5. Portfolio 网站项目卡片链接正确
