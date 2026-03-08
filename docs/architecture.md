# Architecture Design

## Overview

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) on Microsoft Fabric to process multi-channel e-commerce data into business-ready analytics.

## Why Medallion Architecture?

The Medallion pattern progressively refines data quality at each layer:

- **Bronze:** Raw data preservation — audit trail, reprocessing capability
- **Silver:** Cleaned and standardized — single source of truth
- **Gold:** Business aggregations — optimized for BI consumption

This is an industry standard pattern (popularized by Databricks) widely used in modern data platforms. Microsoft Fabric natively supports this with Lakehouses and Delta tables.

## Data Flow

```
[Source Files]
    │
    ▼
[Bronze Layer] ─── Raw Delta Tables (4 tables)
    │                 • shopify_orders_raw
    │                 • amazon_orders_raw
    │                 • amazon_settlement_raw
    │                 • tiktok_settlement_raw
    ▼
[Silver Layer] ─── Cleaned Tables (5 tables)
    │                 • dim_products (SKU mapping, product attributes)
    │                 • dim_customers (Shopify customers, deduped)
    │                 • fact_orders (unified orders, all platforms)
    │                 • fact_order_items (line-item detail)
    │                 • fact_financials (revenue, fees, net amounts)
    ▼
[Gold Layer] ──── Business Tables (4 tables)
    │                 • daily_sales_summary
    │                 • product_performance
    │                 • channel_analysis
    │                 • customer_cohort
    ▼
[Power BI] ─────── 3-page Dashboard (DirectLake mode)
```

## Silver Layer Transformations

| Transformation | Why |
|---------------|-----|
| Date standardization | Three different formats → unified UTC timestamp |
| Currency conversion | CAD → USD for TikTok cross-border orders |
| Amazon Settlement pivot | EAV rows → wide table (revenue, fees, net per order) |
| SKU mapping | Three naming conventions → single product ID |
| Deduplication | Remove duplicate Amazon orders (~0.5%) |
| Null handling | Fill/flag missing emails, SKUs |
| Test order filtering | Remove Shopify voided orders |

## Gold Layer Aggregations

| Table | Grain | Purpose |
|-------|-------|---------|
| daily_sales_summary | Date × Platform | Time series analysis, trend detection |
| product_performance | Product × Platform × Month | SKU-level revenue, units, returns |
| channel_analysis | Platform × Month | Fee structure comparison, net margins |
| customer_cohort | Cohort Month × Period | Repeat purchase analysis (Shopify only) |

## Technology Choices

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Platform | Microsoft Fabric | Unified analytics platform, growing enterprise adoption |
| Storage | Lakehouse (Delta) | ACID transactions, schema evolution, time travel |
| Processing | PySpark / Spark SQL | Native Fabric support, scalable, industry standard |
| BI Tool | Power BI (DirectLake) | Zero-copy from Lakehouse, real-time refresh |
| Data Format | Delta Lake | Open format, compatible with Spark/Fabric/Databricks |
