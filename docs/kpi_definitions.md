# KPI Definitions

## Revenue & Growth

### 1. Total GMV (Gross Merchandise Value)
- **Definition:** Total sales value before any deductions
- **Formula:** `SUM(gross_sales)` per platform
- **Granularity:** Daily / Monthly / By Platform
- **Source:** Shopify Subtotal, Amazon item-price, TikTok Gross Sales

### 2. Net Revenue
- **Definition:** Revenue after platform fees, FBA costs, and creator commissions
- **Formula:** `GMV - platform_fees - fba_fees - creator_commissions - refunds`
- **Granularity:** Daily / Monthly / By Platform
- **Source:** Settlement reports from all platforms

### 3. Revenue Growth Rate (MoM)
- **Definition:** Month-over-month revenue growth
- **Formula:** `(current_month_revenue - previous_month_revenue) / previous_month_revenue × 100`
- **Granularity:** Monthly
- **Note:** Expect seasonal patterns (holiday spike in Nov-Dec)

### 4. AOV (Average Order Value)
- **Definition:** Average revenue per order
- **Formula:** `SUM(order_total) / COUNT(DISTINCT order_id)`
- **Granularity:** Monthly / By Platform
- **Expected Range:** Shopify $25-45, Amazon $18-35, TikTok $15-30

## Product Performance

### 5. Top SKUs by Revenue
- **Definition:** Product ranking by total revenue contribution
- **Formula:** `SUM(line_item_revenue) GROUP BY standardized_sku ORDER BY revenue DESC`
- **Granularity:** Monthly / Quarterly
- **Note:** Requires SKU mapping table to unify cross-platform SKUs

### 6. Return/Refund Rate
- **Definition:** Percentage of orders that were refunded or returned
- **Formula:** `COUNT(refunded_orders) / COUNT(total_orders) × 100`
- **Granularity:** Monthly / By Platform / By SKU
- **Source:** Shopify (Financial Status = refunded), Amazon (Cancelled), TikTok (Adjustments sheet)

### 7. Platform Fee Ratio
- **Definition:** Total platform fees as percentage of GMV
- **Formula:** `SUM(all_fees) / SUM(gmv) × 100`
- **Granularity:** Monthly / By Platform
- **Components:**
  - Amazon: Referral fee (15%) + FBA fee ($3-6.5/unit)
  - TikTok: Transaction fee (2%) + Referral fee (5%) + Creator commission (10-20%)
  - Shopify: Payment processing only (not in data, ~2.9% + $0.30)

## Customer & Channel

### 8. Channel Revenue Mix
- **Definition:** Revenue contribution percentage by channel
- **Formula:** `platform_revenue / total_revenue × 100`
- **Granularity:** Monthly (trend over time)
- **Expected:** Amazon ~55%, Shopify ~30%, TikTok ~15%

### 9. Repeat Purchase Rate
- **Definition:** Percentage of customers who made more than one purchase
- **Formula:** `COUNT(customers with orders > 1) / COUNT(total_customers) × 100`
- **Granularity:** Quarterly
- **Limitation:** Only available for Shopify (has customer email data)

### 10. TikTok Creator ROI
- **Definition:** Return on creator commission investment
- **Formula:** `SUM(creator_driven_gmv) / ABS(SUM(affiliate_commission))`
- **Granularity:** Monthly / By Creator
- **Interpretation:** ROI of 5x means every $1 in commission generates $5 in GMV
