"""
E-Commerce Data Generator

Generates simulated e-commerce data for a US-based DTC coffee concentrate brand
across three channels: Shopify, Amazon, and TikTok Shop.

Output files:
- raw/shopify_orders.csv    (~3,000 orders)
- raw/amazon_orders.tsv     (~5,000 orders)
- raw/amazon_settlement.tsv (financial breakdown, EAV format)
- raw/tiktok_settlement.xlsx(~1,500 orders, multi-sheet)

Data quality issues are intentionally introduced for Silver layer cleaning demo.
"""

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# Product catalog — same products, different SKUs per platform
# ============================================================

PRODUCTS = [
    # (product_name, shopify_sku, amazon_sku, tiktok_sku, base_price, weight_oz)
    ("Original Concentrate 6oz", "original-concentrate-6oz", "JAVY-ORIG-6OZ", "TS-ORIGINAL-6", 24.99, 6),
    ("Original Concentrate 18oz", "original-concentrate-18oz", "JAVY-ORIG-18OZ", "TS-ORIGINAL-18", 54.99, 18),
    ("Vanilla Concentrate 6oz", "vanilla-concentrate-6oz", "JAVY-VAN-6OZ", "TS-VANILLA-6", 26.99, 6),
    ("Vanilla Concentrate 18oz", "vanilla-concentrate-18oz", "JAVY-VAN-18OZ", "TS-VANILLA-18", 59.99, 18),
    ("Mocha Concentrate 6oz", "mocha-concentrate-6oz", "JAVY-MOC-6OZ", "TS-MOCHA-6", 26.99, 6),
    ("Mocha Concentrate 18oz", "mocha-concentrate-18oz", "JAVY-MOC-18OZ", "TS-MOCHA-18", 59.99, 18),
    ("Caramel Concentrate 6oz", "caramel-concentrate-6oz", "JAVY-CAR-6OZ", "TS-CARAMEL-6", 26.99, 6),
    ("Caramel Concentrate 18oz", "caramel-concentrate-18oz", "JAVY-CAR-18OZ", "TS-CARAMEL-18", 59.99, 18),
    ("Decaf Concentrate 6oz", "decaf-concentrate-6oz", "JAVY-DEC-6OZ", "TS-DECAF-6", 27.99, 6),
    ("Decaf Concentrate 18oz", "decaf-concentrate-18oz", "JAVY-DEC-18OZ", "TS-DECAF-18", 62.99, 18),
    ("Travel Mug", "travel-mug", "JAVY-MUG", "TS-MUG", 19.99, 12),
    ("Cold Brew Kit", "cold-brew-kit", "JAVY-CBKIT", "TS-CBKIT", 34.99, 24),
    ("Starter Kit", "starter-kit", "JAVY-START", "TS-STARTER", 44.99, 20),
    ("Variety Pack", "variety-pack", "JAVY-VARIETY", "TS-VARIETY", 49.99, 24),
]

# Weight distribution: concentrates sell more than accessories/bundles
PRODUCT_WEIGHTS = [15, 8, 12, 6, 10, 5, 10, 5, 8, 4, 5, 4, 4, 4]

DISCOUNT_CODES = {
    "WELCOME10": 0.10,
    "SUBSCRIBE20": 0.20,
    "HOLIDAY25": 0.25,
    "SUMMER15": 0.15,
    "FLASH30": 0.30,
}

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]

# Seasonal multipliers: holiday season boost, summer dip
MONTHLY_MULTIPLIER = {
    1: 0.9, 2: 0.85, 3: 0.95, 4: 1.0, 5: 1.0, 6: 0.9,
    7: 0.85, 8: 0.9, 9: 1.0, 10: 1.1, 11: 1.3, 12: 1.4,
}

TIKTOK_CREATORS = [
    "coffeewithjess", "morningbrewmike", "drinkhacks", "caffeinequeen",
    "barista.at.home", "sipandtell", "thecoffeeguy", "brewtifully",
    "latte.lover", "coffeetok.daily",
]


def random_date_in_2025() -> datetime:
    """Generate a random datetime in 2025 with monthly seasonality."""
    # Pick month with seasonal weighting
    months = list(range(1, 13))
    weights = [MONTHLY_MULTIPLIER[m] for m in months]
    month = random.choices(months, weights=weights, k=1)[0]

    # Random day and time within that month
    if month == 12:
        max_day = 31
    elif month == 2:
        max_day = 28
    elif month in (4, 6, 9, 11):
        max_day = 30
    else:
        max_day = 31

    day = random.randint(1, max_day)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return datetime(2025, month, day, hour, minute, second)


def pick_product():
    """Pick a random product weighted by popularity."""
    return random.choices(PRODUCTS, weights=PRODUCT_WEIGHTS, k=1)[0]


# ============================================================
# Shopify Orders (CSV)
# ============================================================

def generate_shopify_orders(n_orders=3000):
    """
    Generate Shopify orders export CSV.
    Format: multi-row per order (one row per line item).
    Date format: ISO with timezone offset (e.g. 2025-03-15T14:30:00-05:00).
    Intentional issues: some voided test orders, occasional missing emails.
    """
    rows = []
    order_number = 1001

    for _ in range(n_orders):
        order_date = random_date_in_2025()
        # Shopify date format: ISO with US timezone offset
        tz_offsets = ["-05:00", "-06:00", "-07:00", "-08:00"]
        date_str = order_date.strftime("%Y-%m-%dT%H:%M:%S") + random.choice(tz_offsets)

        # Customer info
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"

        # ~2% chance of missing email (data quality issue)
        if random.random() < 0.02:
            email = ""

        state = random.choice(US_STATES)
        city = fake.city()
        zip_code = fake.zipcode()

        # 1-3 line items per order
        n_items = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1], k=1)[0]
        selected_products = [pick_product() for _ in range(n_items)]

        # Discount
        has_discount = random.random() < 0.25
        discount_code = ""
        discount_pct = 0
        if has_discount:
            discount_code = random.choice(list(DISCOUNT_CODES.keys()))
            discount_pct = DISCOUNT_CODES[discount_code]

        # Financial status: ~1.5% voided (test orders), ~3% refunded
        rand_status = random.random()
        if rand_status < 0.015:
            financial_status = "voided"
        elif rand_status < 0.045:
            financial_status = "refunded"
        else:
            financial_status = "paid"

        subtotal = 0
        order_name = f"#JC{order_number}"

        for product in selected_products:
            name, sku, _, _, price, _ = product
            qty = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05], k=1)[0]
            line_total = round(price * qty, 2)
            subtotal += line_total

            discount_amount = round(line_total * discount_pct, 2) if has_discount else 0
            shipping = round(random.choice([0, 5.99, 7.99, 9.99]), 2)
            tax_rate = round(random.uniform(0.05, 0.10), 4)
            taxes = round((line_total - discount_amount) * tax_rate, 2)
            total = round(line_total - discount_amount + shipping + taxes, 2)

            rows.append({
                "Name": order_name,
                "Email": email,
                "Financial Status": financial_status,
                "Paid at": date_str if financial_status == "paid" else "",
                "Currency": "USD",
                "Subtotal": line_total,
                "Shipping": shipping,
                "Taxes": taxes,
                "Total": total,
                "Discount Code": discount_code,
                "Discount Amount": discount_amount,
                "Lineitem quantity": qty,
                "Lineitem name": name,
                "Lineitem price": price,
                "Lineitem sku": sku,
                "Billing Name": f"{first_name} {last_name}",
                "Billing City": city,
                "Billing Province": state,
                "Billing Zip": zip_code,
                "Billing Country": "US",
                "Shipping Name": f"{first_name} {last_name}",
                "Shipping City": city,
                "Shipping Province": state,
                "Shipping Zip": zip_code,
                "Shipping Country": "US",
            })

        order_number += 1

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "shopify_orders.csv")
    df.to_csv(path, index=False)
    print(f"Generated {path}: {len(df)} rows ({n_orders} orders)")
    return df


# ============================================================
# Amazon All Orders Report (TSV)
# ============================================================

def generate_amazon_orders(n_orders=5000):
    """
    Generate Amazon All Orders report TSV.
    Date format: ISO UTC (e.g. 2025-03-15T19:30:00+00:00).
    One row per item (an order can have multiple items).
    Intentional issues: some duplicate order IDs, mixed fulfillment channels.
    """
    rows = []

    for i in range(n_orders):
        order_date = random_date_in_2025()
        # Amazon date: ISO UTC format
        date_str = order_date.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"

        # Amazon order ID format: 111-1234567-1234567
        order_id = f"{random.randint(100,999)}-{random.randint(1000000,9999999)}-{random.randint(1000000,9999999)}"

        # ~0.5% duplicate orders (data quality issue)
        if random.random() < 0.005 and i > 0:
            order_id = rows[-1]["amazon-order-id"]

        product = pick_product()
        name, _, sku, _, price, _ = product
        # Amazon prices slightly different (marketplace competition)
        price = round(price * random.uniform(0.90, 1.05), 2)
        qty = random.choices([1, 2, 3], weights=[0.75, 0.2, 0.05], k=1)[0]

        # Order status
        status_rand = random.random()
        if status_rand < 0.03:
            order_status = "Cancelled"
        elif status_rand < 0.06:
            order_status = "Pending"
        else:
            order_status = "Shipped"

        # Fulfillment: 85% FBA (AFN), 15% merchant (MFN)
        fulfillment = "Amazon" if random.random() < 0.85 else "Merchant"

        state = random.choice(US_STATES)
        city = fake.city()
        zip_code = fake.zipcode()

        item_price = round(price * qty, 2)
        item_tax = round(item_price * random.uniform(0.05, 0.10), 2)
        shipping_price = round(random.choice([0, 3.99, 5.99]), 2)

        rows.append({
            "amazon-order-id": order_id,
            "purchase-date": date_str,
            "order-status": order_status,
            "fulfillment-channel": fulfillment,
            "sku": sku,
            "asin": f"B0{random.randint(10000000, 99999999)}",
            "product-name": name,
            "quantity": qty,
            "currency": "USD",
            "item-price": item_price,
            "item-tax": item_tax,
            "shipping-price": shipping_price,
            "ship-city": city,
            "ship-state": state,
            "ship-postal-code": zip_code,
            "ship-country": "US",
        })

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "amazon_orders.tsv")
    df.to_csv(path, sep="\t", index=False)
    print(f"Generated {path}: {len(df)} rows ({n_orders} orders)")
    return df


# ============================================================
# Amazon Settlement Report V2 (TSV, EAV format)
# ============================================================

def generate_amazon_settlement(amazon_orders_df):
    """
    Generate Amazon Settlement Report in EAV (Entity-Attribute-Value) format.
    Each order gets multiple rows for different fee types.
    This needs to be pivoted in Silver layer — a classic interview question.
    """
    rows = []
    settlement_id = 17239876543

    # Only shipped orders have settlements
    shipped = amazon_orders_df[amazon_orders_df["order-status"] == "Shipped"]

    for _, order in shipped.iterrows():
        order_id = order["amazon-order-id"]
        sku = order["sku"]
        item_price = order["item-price"]

        # Revenue row
        rows.append({
            "settlement-id": settlement_id,
            "order-id": order_id,
            "sku": sku,
            "amount-type": "ItemPrice",
            "amount-description": "Principal",
            "amount": item_price,
        })

        # Tax row
        rows.append({
            "settlement-id": settlement_id,
            "order-id": order_id,
            "sku": sku,
            "amount-type": "ItemPrice",
            "amount-description": "Tax",
            "amount": order["item-tax"],
        })

        # Shipping revenue
        if order["shipping-price"] > 0:
            rows.append({
                "settlement-id": settlement_id,
                "order-id": order_id,
                "sku": sku,
                "amount-type": "ItemPrice",
                "amount-description": "Shipping",
                "amount": order["shipping-price"],
            })

        # Amazon referral fee (typically 15%)
        referral_fee = round(item_price * -0.15, 2)
        rows.append({
            "settlement-id": settlement_id,
            "order-id": order_id,
            "sku": sku,
            "amount-type": "ItemFees",
            "amount-description": "Commission",
            "amount": referral_fee,
        })

        # FBA fee (if Amazon fulfilled)
        if order["fulfillment-channel"] == "Amazon":
            fba_fee = round(-1 * random.uniform(3.0, 6.5), 2)
            rows.append({
                "settlement-id": settlement_id,
                "order-id": order_id,
                "sku": sku,
                "amount-type": "ItemFees",
                "amount-description": "FBAPerUnitFulfillmentFee",
                "amount": fba_fee,
            })

        # Occasionally rotate settlement ID (batches of ~500 orders)
        if random.random() < 0.002:
            settlement_id += 1

    df = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "amazon_settlement.tsv")
    df.to_csv(path, sep="\t", index=False)
    print(f"Generated {path}: {len(df)} rows (EAV format)")
    return df


# ============================================================
# TikTok Shop Settlement Report (XLSX, multi-sheet)
# ============================================================

def generate_tiktok_settlement(n_orders=1500):
    """
    Generate TikTok Shop settlement report as XLSX with multiple sheets.
    Date format: MM/DD/YYYY (intentionally different from other platforms).
    Intentional issues: ~5% CAD currency orders, some missing SKUs.
    """
    order_rows = []
    adjustment_rows = []

    for _ in range(n_orders):
        order_date = random_date_in_2025()
        # TikTok date format: MM/DD/YYYY (data quality issue — different from others)
        date_str = order_date.strftime("%m/%d/%Y")

        order_id = f"TT{random.randint(100000000000, 999999999999)}"

        product = pick_product()
        name, _, _, sku, price, _ = product
        # TikTok prices slightly lower (competitive pricing)
        price = round(price * random.uniform(0.85, 0.98), 2)
        qty = random.choices([1, 2], weights=[0.8, 0.2], k=1)[0]

        gross_sales = round(price * qty, 2)

        # ~5% CAD currency (Canadian cross-border, data quality issue)
        currency = "CAD" if random.random() < 0.05 else "USD"

        # Seller discount (~15% of orders)
        seller_discount = 0
        if random.random() < 0.15:
            seller_discount = round(gross_sales * random.uniform(0.05, 0.20), 2)

        net_sales = round(gross_sales - seller_discount, 2)

        # TikTok fees
        transaction_fee = round(net_sales * -0.02, 2)  # ~2% payment processing
        referral_fee = round(net_sales * -0.05, 2)     # ~5% platform fee

        # Creator commission (~60% of TikTok orders are creator-driven)
        creator = ""
        affiliate_commission = 0
        if random.random() < 0.60:
            creator = random.choice(TIKTOK_CREATORS)
            commission_rate = random.uniform(0.10, 0.20)  # 10-20% creator commission
            affiliate_commission = round(net_sales * -commission_rate, 2)

        # ~1% missing SKU (data quality issue)
        if random.random() < 0.01:
            sku = ""

        order_rows.append({
            "Order ID": order_id,
            "Order Date": date_str,
            "SKU ID": sku,
            "Product Name": name,
            "Quantity": qty,
            "Currency": currency,
            "Gross Sales": gross_sales,
            "Seller Discount": seller_discount,
            "Net Sales": net_sales,
            "Transaction Fee": transaction_fee,
            "Referral Fee": referral_fee,
            "Affiliate Creator": creator,
            "Affiliate Commission": affiliate_commission,
            "Settlement Amount": round(net_sales + transaction_fee + referral_fee + affiliate_commission, 2),
        })

    # Generate some adjustments (refunds, corrections)
    n_adjustments = int(n_orders * 0.04)  # ~4% adjustment rate
    for _ in range(n_adjustments):
        adj_date = random_date_in_2025()
        # Pick a random order to adjust
        original = random.choice(order_rows)
        adjustment_rows.append({
            "Adjustment ID": f"ADJ{random.randint(10000000, 99999999)}",
            "Adjustment Date": adj_date.strftime("%m/%d/%Y"),
            "Related Order ID": original["Order ID"],
            "Adjustment Type": random.choice(["Refund", "Refund", "Correction"]),
            "Adjustment Amount": round(-abs(original["Net Sales"]) * random.uniform(0.5, 1.0), 2),
            "Currency": original["Currency"],
        })

    orders_df = pd.DataFrame(order_rows)
    adjustments_df = pd.DataFrame(adjustment_rows)

    path = os.path.join(OUTPUT_DIR, "tiktok_settlement.xlsx")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        orders_df.to_excel(writer, sheet_name="Orders", index=False)
        adjustments_df.to_excel(writer, sheet_name="Adjustments", index=False)

    print(f"Generated {path}: {len(orders_df)} orders + {len(adjustments_df)} adjustments")
    return orders_df


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Generating e-commerce data for 2025...")
    print("=" * 60)

    print("\n[1/4] Shopify Orders...")
    shopify_df = generate_shopify_orders()

    print("\n[2/4] Amazon Orders...")
    amazon_df = generate_amazon_orders()

    print("\n[3/4] Amazon Settlement...")
    generate_amazon_settlement(amazon_df)

    print("\n[4/4] TikTok Shop Settlement...")
    generate_tiktok_settlement()

    print("\n" + "=" * 60)
    print("Done! Files saved to data/raw/")
    print("=" * 60)
