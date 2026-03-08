# Data Dictionary

## Source Files

### 1. Shopify Orders (`shopify_orders.csv`)

| Column | Type | Description |
|--------|------|-------------|
| Name | string | Order number (e.g., #JC1001) |
| Email | string | Customer email (may be empty ~2%) |
| Financial Status | string | paid / refunded / voided |
| Paid at | string | ISO datetime with timezone offset |
| Currency | string | Always USD |
| Subtotal | float | Line item subtotal before discounts |
| Shipping | float | Shipping cost |
| Taxes | float | Tax amount |
| Total | float | Final total |
| Discount Code | string | Coupon code if applied |
| Discount Amount | float | Dollar amount discounted |
| Lineitem quantity | int | Quantity of this line item |
| Lineitem name | string | Product name |
| Lineitem price | float | Unit price |
| Lineitem sku | string | Shopify SKU format (e.g., original-concentrate-6oz) |
| Billing/Shipping Name | string | Customer full name |
| Billing/Shipping City | string | City |
| Billing/Shipping Province | string | US state code |
| Billing/Shipping Zip | string | ZIP code |
| Billing/Shipping Country | string | Always US |

**Note:** One order can have multiple rows (one per line item).

### 2. Amazon All Orders Report (`amazon_orders.tsv`)

| Column | Type | Description |
|--------|------|-------------|
| amazon-order-id | string | Format: 111-1234567-1234567 |
| purchase-date | string | ISO UTC format (e.g., 2025-03-15T19:30:00+00:00) |
| order-status | string | Shipped / Cancelled / Pending |
| fulfillment-channel | string | Amazon (FBA) / Merchant (FBM) |
| sku | string | Amazon SKU format (e.g., JAVY-ORIG-6OZ) |
| asin | string | Amazon Standard Identification Number |
| product-name | string | Product name |
| quantity | int | Quantity ordered |
| currency | string | Always USD |
| item-price | float | Total item price (unit price × quantity) |
| item-tax | float | Tax amount |
| shipping-price | float | Shipping charge |
| ship-city | string | Ship-to city |
| ship-state | string | Ship-to state code |
| ship-postal-code | string | Ship-to ZIP |
| ship-country | string | Always US |

**Note:** ~0.5% duplicate order IDs (data quality issue).

### 3. Amazon Settlement Report V2 (`amazon_settlement.tsv`)

| Column | Type | Description |
|--------|------|-------------|
| settlement-id | int | Settlement batch ID |
| order-id | string | Amazon order ID |
| sku | string | Amazon SKU |
| amount-type | string | ItemPrice / ItemFees |
| amount-description | string | Principal / Tax / Shipping / Commission / FBAPerUnitFulfillmentFee |
| amount | float | Dollar amount (negative for fees) |

**Note:** EAV (Entity-Attribute-Value) format — needs pivot to wide table in Silver layer.

### 4. TikTok Shop Settlement (`tiktok_settlement.xlsx`)

#### Sheet: Orders

| Column | Type | Description |
|--------|------|-------------|
| Order ID | string | Format: TT + 12 digits |
| Order Date | string | MM/DD/YYYY format |
| SKU ID | string | TikTok SKU format (e.g., TS-ORIGINAL-6), ~1% empty |
| Product Name | string | Product name |
| Quantity | int | Quantity ordered |
| Currency | string | USD or CAD (~5% Canadian cross-border) |
| Gross Sales | float | Before discounts |
| Seller Discount | float | Seller-funded discount |
| Net Sales | float | After discounts |
| Transaction Fee | float | ~2% payment processing (negative) |
| Referral Fee | float | ~5% platform fee (negative) |
| Affiliate Creator | string | Creator username (empty if organic) |
| Affiliate Commission | float | 10-20% creator commission (negative) |
| Settlement Amount | float | Final amount after all fees |

#### Sheet: Adjustments

| Column | Type | Description |
|--------|------|-------------|
| Adjustment ID | string | Format: ADJ + 8 digits |
| Adjustment Date | string | MM/DD/YYYY format |
| Related Order ID | string | Original order ID |
| Adjustment Type | string | Refund / Correction |
| Adjustment Amount | float | Refund amount (negative) |
| Currency | string | USD or CAD |

## SKU Mapping

| Product | Shopify SKU | Amazon SKU | TikTok SKU |
|---------|-------------|------------|------------|
| Original Concentrate 6oz | original-concentrate-6oz | JAVY-ORIG-6OZ | TS-ORIGINAL-6 |
| Original Concentrate 18oz | original-concentrate-18oz | JAVY-ORIG-18OZ | TS-ORIGINAL-18 |
| Vanilla Concentrate 6oz | vanilla-concentrate-6oz | JAVY-VAN-6OZ | TS-VANILLA-6 |
| Vanilla Concentrate 18oz | vanilla-concentrate-18oz | JAVY-VAN-18OZ | TS-VANILLA-18 |
| Mocha Concentrate 6oz | mocha-concentrate-6oz | JAVY-MOC-6OZ | TS-MOCHA-6 |
| Mocha Concentrate 18oz | mocha-concentrate-18oz | JAVY-MOC-18OZ | TS-MOCHA-18 |
| Caramel Concentrate 6oz | caramel-concentrate-6oz | JAVY-CAR-6OZ | TS-CARAMEL-6 |
| Caramel Concentrate 18oz | caramel-concentrate-18oz | JAVY-CAR-18OZ | TS-CARAMEL-18 |
| Decaf Concentrate 6oz | decaf-concentrate-6oz | JAVY-DEC-6OZ | TS-DECAF-6 |
| Decaf Concentrate 18oz | decaf-concentrate-18oz | JAVY-DEC-18OZ | TS-DECAF-18 |
| Travel Mug | travel-mug | JAVY-MUG | TS-MUG |
| Cold Brew Kit | cold-brew-kit | JAVY-CBKIT | TS-CBKIT |
| Starter Kit | starter-kit | JAVY-START | TS-STARTER |
| Variety Pack | variety-pack | JAVY-VARIETY | TS-VARIETY |

## Known Data Quality Issues

| Issue | Source | Description |
|-------|--------|-------------|
| Missing emails | Shopify | ~2% of orders have empty Email field |
| Voided orders | Shopify | ~1.5% test orders with Financial Status = "voided" |
| Duplicate orders | Amazon | ~0.5% duplicate amazon-order-id values |
| EAV format | Amazon Settlement | Requires pivot to wide table |
| Date format mismatch | All | ISO+TZ (Shopify) vs ISO UTC (Amazon) vs MM/DD/YYYY (TikTok) |
| Mixed currency | TikTok | ~5% orders in CAD instead of USD |
| Missing SKUs | TikTok | ~1% orders with empty SKU ID |
| SKU inconsistency | All | Same product has different SKU across platforms |
