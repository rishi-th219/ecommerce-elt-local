"""
E-Commerce Synthetic Data Generator
Generates realistic, referentially consistent e-commerce data:
- Customers (with simulated address/profile updates for SCD Type 2)
- Products (with unit costs, retail prices, categories)
- Web Sessions (traffic sources, devices, page views)
- Orders (with payment methods, statuses, order timestamps)
- Order Items (with line-level quantities, prices, discounts)
"""

import json
import os
import random
from datetime import datetime, timedelta

import yaml
from faker import Faker

from ingestion.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_SEED = 42

fake = Faker()


def set_seed(seed=DEFAULT_SEED):
    """Seed both RNGs so a given config reproduces byte-identical bronze files."""
    Faker.seed(seed)
    random.seed(seed)


set_seed()

CATEGORIES = {
    "Electronics": [
        ("Wireless Noise-Canceling Headphones", 75.0, 149.99),
        ("Mechanical Gaming Keyboard", 45.0, 89.99),
        ("4K Ultra HD Monitor 27-inch", 180.0, 329.99),
        ("Smart Fitness Watch", 60.0, 129.99),
        ("USB-C Multiport Docking Station", 25.0, 59.99),
        ("Bluetooth Portable Speaker", 20.0, 49.99),
        ("Ergonomic Vertical Mouse", 18.0, 39.99),
        ("1080p Streaming Webcam", 22.0, 49.99),
        ("MagSafe Wireless Charger", 12.0, 29.99),
        ("Noise-Isolating Earbuds", 15.0, 34.99),
    ],
    "Apparel": [
        ("Classic Organic Cotton T-Shirt", 8.0, 24.99),
        ("Slim-Fit Denim Jeans", 22.0, 68.00),
        ("Merino Wool Crewneck Sweater", 35.0, 88.00),
        ("Water-Resistant Trail Jacket", 45.0, 119.00),
        ("Breathable Running Shorts", 12.0, 32.00),
        ("Fleece Zip-Up Hoodie", 20.0, 54.99),
        ("Casual Canvas Sneakers", 25.0, 65.00),
        ("Thermal Base Layer Top", 14.0, 38.00),
        ("Everyday Ankle Socks (3-Pack)", 4.0, 14.99),
        ("Wool Blend Overcoat", 65.0, 175.00),
    ],
    "Home & Kitchen": [
        ("Stainless Steel French Press", 14.0, 34.99),
        ("Cast Iron Skillet 10-inch", 18.0, 42.00),
        ("Chef's Knife 8-inch High Carbon", 28.0, 69.99),
        ("Aroma Oil Diffuser & Humidifier", 15.0, 36.00),
        ("Bamboo Cutting Board Set", 12.0, 29.99),
        ("Insulated Travel Tumbler 20oz", 10.0, 26.50),
        ("Non-Stick Ceramic Baking Sheet", 9.0, 22.00),
        ("Electric Gooseneck Kettle", 30.0, 64.99),
        ("100% Linen Napkins (Set of 4)", 8.0, 24.00),
        ("Countertop Compost Bin", 11.0, 28.00),
    ],
    "Beauty & Personal Care": [
        ("Hydrating Hyaluronic Acid Serum", 9.0, 28.00),
        ("Gentle Foaming Daily Cleanser", 7.0, 19.50),
        ("SPF 50 Mineral Sunscreen 100ml", 8.5, 24.00),
        ("Organic Argan Hair Oil", 10.0, 26.00),
        ("Exfoliating Coffee Body Scrub", 6.0, 18.00),
        ("Vitamin C Brightening Moisturizer", 11.0, 32.00),
        ("Natural Clay Detox Face Mask", 8.0, 22.50),
        ("Nourishing Shea Lip Balm (2-Pack)", 3.0, 9.99),
        ("Rose Quartz Facial Roller", 5.0, 16.00),
        ("Botanical Hand Cream 75ml", 4.5, 14.00),
    ],
    "Sports & Outdoors": [
        ("Ultra-Lightweight Yoga Mat 6mm", 12.0, 34.00),
        ("Resistance Bands Set (5 Levels)", 7.0, 21.99),
        ("Insulated Hydration Backpack", 22.0, 58.00),
        ("Adjustable Speed Jump Rope", 4.0, 15.00),
        ("Compact Trekking Poles Pair", 18.0, 49.99),
        ("Heavy-Duty Foam Roller 18-inch", 10.0, 27.50),
        ("Waterproof Camping Lantern", 11.0, 29.99),
        ("Quick-Dry Microfiber Camp Towel", 5.0, 16.00),
        ("Stainless Double-Wall Canteen 32oz", 9.0, 26.00),
        ("Cycling Helmet with Rear LED", 24.0, 59.99),
    ],
    "Books & Stationery": [
        ("Hardcover Dotted Grid Journal", 5.0, 16.99),
        ("Precision Metal Fountain Pen", 12.0, 32.00),
        ("Minimalist Desk Planner Pad", 4.0, 14.00),
        ("Archival Pigment Fineliner Set", 6.0, 18.50),
        ("Leather Bookmark with Brass Rivet", 2.5, 9.99),
        ("Book Light with Warm LED Clip", 5.5, 17.00),
        ("Japanese Sticky Notes Palette", 3.0, 8.50),
        ("Desk Cable Organizer Weighted", 4.5, 12.99),
        ("Reading Stand Bamboo Foldable", 8.0, 23.50),
        ("Premium Heavyweight Sketchbook", 7.0, 20.00),
    ]
}

TRAFFIC_SOURCES = ["organic_search", "direct", "cpc_ad", "email_campaign", "social_media", "affiliate"]
DEVICES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "debit_card"]
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "returned", "cancelled", "processing"]

def load_config(config_path="ingestion/config.yaml"):
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    return {
        "scale": {
            "num_customers": 250,
            "num_products": 60,
            "num_orders": 1200,
            "max_items_per_order": 4,
            "num_sessions": 2000,
            "customer_updates_pct": 0.20
        },
        "date_range": {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31"
        }
    }

def generate_dataset(config=None):
    if config is None:
        config = load_config()

    set_seed(config.get("seed", DEFAULT_SEED))

    scale = config["scale"]
    start_dt = datetime.strptime(config["date_range"]["start_date"], "%Y-%m-%d")
    end_dt = datetime.strptime(config["date_range"]["end_date"], "%Y-%m-%d")
    total_days = (end_dt - start_dt).days

    logger.info("Generating synthetic e-commerce data over a %s day period", total_days)

    # 1. Products
    products = []
    prod_idx = 1
    for category, item_list in CATEGORIES.items():
        for name, cost, price in item_list:
            sku = f"SKU-{category[:3].upper()}-{prod_idx:03d}"
            products.append({
                "product_id": f"PROD-{prod_idx:03d}",
                "sku": sku,
                "product_name": name,
                "category": category,
                "unit_cost": round(cost, 2),
                "retail_price": round(price, 2),
                "created_at": (start_dt - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
            })
            prod_idx += 1

    # 2. Customers & SCD2 updates
    customers_raw = []
    customers_registry = []
    num_customers = scale["num_customers"]
    update_pct = scale.get("customer_updates_pct", 0.20)

    for i in range(1, num_customers + 1):
        cust_id = f"CUST-{i:04d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        domain = fake.free_email_domain()
        email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
        state = fake.state_abbr()
        country = "USA"
        city = fake.city()
        postal_code = fake.postcode()
        
        # Initial signup date between start_dt - 180 days and start_dt + 120 days
        signup_offset = random.randint(-180, 120)
        created_at = start_dt + timedelta(days=signup_offset, seconds=random.randint(0, 86399))
        created_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

        initial_record = {
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "country": country,
            "created_at": created_str,
            "updated_at": created_str
        }
        customers_raw.append(initial_record)

        # Store for reference
        versions = [initial_record]

        # Check if customer has an update (simulating relocation / profile change for SCD2)
        if random.random() < update_pct:
            update_offset = random.randint(30, 200)
            updated_at = created_at + timedelta(days=update_offset, seconds=random.randint(0, 86399))
            if updated_at <= end_dt:
                new_state = fake.state_abbr()
                while new_state == state:
                    new_state = fake.state_abbr()
                new_city = fake.city()
                new_postal = fake.postcode()
                updated_str = updated_at.strftime("%Y-%m-%d %H:%M:%S")

                update_record = {
                    "customer_id": cust_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "city": new_city,
                    "state": new_state,
                    "postal_code": new_postal,
                    "country": country,
                    "created_at": created_str,
                    "updated_at": updated_str
                }
                customers_raw.append(update_record)
                versions.append(update_record)

        customers_registry.append({
            "customer_id": cust_id,
            "versions": versions
        })

    # 3. Sessions
    sessions = []
    num_sessions = scale["num_sessions"]
    for i in range(1, num_sessions + 1):
        sess_id = f"SESS-{i:06d}"
        cust = random.choice(customers_registry)
        # Session timestamp between start_dt and end_dt
        day_offset = random.randint(0, total_days)
        sess_start = start_dt + timedelta(days=day_offset, seconds=random.randint(0, 80000))
        duration = random.randint(35, 1500)
        sess_end = sess_start + timedelta(seconds=duration)
        device = random.choice(DEVICES)
        source = random.choice(TRAFFIC_SOURCES)
        page_views = random.randint(1, 14)

        sessions.append({
            "session_id": sess_id,
            "customer_id": cust["customer_id"],
            "session_start": sess_start.strftime("%Y-%m-%d %H:%M:%S"),
            "session_end": sess_end.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration,
            "device_type": device,
            "traffic_source": source,
            "page_views_count": page_views,
            "converted": False
        })

    # 4. Orders & Order Items
    num_orders = scale["num_orders"]
    orders = []
    order_items = []
    item_counter = 1

    # Select sessions to convert
    available_sessions = list(sessions)
    random.shuffle(available_sessions)

    for i in range(1, num_orders + 1):
        order_id = f"ORD-{i:05d}"
        
        # Associate with a session
        if available_sessions:
            sess = available_sessions.pop()
            sess["converted"] = True
            sess_dt = datetime.strptime(sess["session_start"], "%Y-%m-%d %H:%M:%S")
            # Order happens during the session
            order_dt = sess_dt + timedelta(seconds=min(sess["duration_seconds"] - 5, random.randint(20, sess["duration_seconds"])))
            sess_id = sess["session_id"]
            cust_id = sess["customer_id"]
        else:
            cust = random.choice(customers_registry)
            cust_id = cust["customer_id"]
            sess_id = None
            day_offset = random.randint(0, total_days)
            order_dt = start_dt + timedelta(days=day_offset, seconds=random.randint(0, 86399))

        status = random.choice(ORDER_STATUSES)
        payment_method = random.choice(PAYMENT_METHODS)

        # Generate 1 to max_items
        num_items = random.randint(1, scale.get("max_items_per_order", 4))
        selected_prods = random.sample(products, k=min(num_items, len(products)))

        order_gross = 0.0
        order_discount = 0.0

        for prod in selected_prods:
            qty = random.choices([1, 2, 3, 4], weights=[0.65, 0.22, 0.09, 0.04])[0]
            unit_price = prod["retail_price"]
            gross_item = round(qty * unit_price, 2)
            # 25% chance of item discount
            discount_item = 0.0
            if random.random() < 0.25:
                discount_rate = random.choice([0.05, 0.10, 0.15, 0.20])
                discount_item = round(gross_item * discount_rate, 2)

            net_item = round(gross_item - discount_item, 2)
            order_gross += gross_item
            order_discount += discount_item

            order_items.append({
                "order_item_id": f"ITEM-{item_counter:06d}",
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount_amount": discount_item,
                "net_amount": net_item
            })
            item_counter += 1

        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "session_id": sess_id,
            "order_date": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "order_status": status,
            "payment_method": payment_method,
            "gross_amount": round(order_gross, 2),
            "discount_amount": round(order_discount, 2),
            "net_revenue": round(order_gross - order_discount, 2)
        })

    logger.info(
        "Generated %s customer event records (from %s customers)", len(customers_raw), num_customers
    )
    logger.info("Generated %s products", len(products))
    logger.info("Generated %s web sessions", len(sessions))
    logger.info("Generated %s orders", len(orders))
    logger.info("Generated %s order items", len(order_items))

    return {
        "customers": customers_raw,
        "products": products,
        "sessions": sessions,
        "orders": orders,
        "order_items": order_items
    }

if __name__ == "__main__":
    data = generate_dataset()
    # Quick sanity check
    assert len(data["customers"]) > 0
    assert len(data["products"]) > 0
    assert len(data["orders"]) > 0
    assert len(data["order_items"]) > 0
    logger.info("Self-test passed: dataset generation successful.")
