"""
data/generate_sample_data.py
─────────────────────────────
Populates MongoDB Atlas with realistic sample data for all 5 collections.
Run once to bootstrap your source system.

Usage:
  python generate_sample_data.py --uri "mongodb+srv://..." --records 500
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, ASCENDING
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

STATUSES      = ["active", "inactive", "suspended"]
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "completed", "cancelled"]
PAY_METHODS   = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
PAY_GATEWAYS  = ["stripe", "paypal", "braintree", "square"]
CATEGORIES    = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"]
CURRENCIES    = ["USD", "EUR", "GBP", "INR", "AUD"]


def ts(days_back: int = 0, jitter_hours: int = 72) -> datetime:
    """Generate a random timestamp within the last N days."""
    base = datetime.now(timezone.utc) - timedelta(days=days_back)
    jitter = timedelta(hours=random.randint(0, jitter_hours))
    return (base - jitter).replace(tzinfo=None)


# ──────────────────────────────────────────────────────────────
# Generators
# ──────────────────────────────────────────────────────────────

def gen_customers(n: int) -> list:
    docs = []
    for _ in range(n):
        created = ts(365)
        docs.append({
            "customer_id": f"CUST-{uuid.uuid4().hex[:8].upper()}",
            "first_name":  fake.first_name(),
            "last_name":   fake.last_name(),
            "email":       fake.unique.email(),
            "phone":       fake.phone_number(),
            "address":     fake.street_address(),
            "city":        fake.city(),
            "state":       fake.state(),
            "country":     fake.country_code(),
            "zip_code":    fake.postcode(),
            "status":      random.choice(STATUSES),
            "preferences": {
                "newsletter":   random.choice([True, False]),
                "sms_alerts":   random.choice([True, False]),
                "language":     random.choice(["en", "es", "fr", "de", "hi"]),
            },
            "loyalty_points": random.randint(0, 50_000),
            "created_at":  created,
            "updated_at":  created + timedelta(days=random.randint(0, 30)),
        })
    return docs


def gen_products(n: int) -> list:
    docs = []
    for _ in range(n):
        cat      = random.choice(CATEGORIES)
        price    = round(random.uniform(5, 2000), 2)
        created  = ts(500)
        docs.append({
            "product_id":   f"PROD-{uuid.uuid4().hex[:8].upper()}",
            "name":         fake.catch_phrase(),
            "category":     cat,
            "sub_category": fake.word().capitalize(),
            "price":        price,
            "cost":         round(price * random.uniform(0.3, 0.7), 2),
            "stock_qty":    random.randint(0, 1000),
            "sku":          fake.ean(length=13),
            "weight_kg":    round(random.uniform(0.1, 20), 2),
            "attributes": {
                "color":    fake.color_name(),
                "material": random.choice(["plastic", "metal", "wood", "fabric"]),
                "brand":    fake.company(),
            },
            "status":       random.choice(["active", "discontinued", "out_of_stock"]),
            "tags":         fake.words(nb=random.randint(2, 5)),
            "created_at":  created,
            "updated_at":  created + timedelta(days=random.randint(0, 60)),
        })
    return docs


def gen_orders(n: int, customer_ids: list, product_ids: list) -> list:
    docs = []
    for _ in range(n):
        created  = ts(90)
        n_items  = random.randint(1, 5)
        items    = []
        subtotal = 0.0
        for _ in range(n_items):
            qty    = random.randint(1, 10)
            price  = round(random.uniform(5, 500), 2)
            subtotal += qty * price
            items.append({
                "product_id": random.choice(product_ids),
                "qty":        qty,
                "unit_price": price,
                "discount":   round(random.uniform(0, 0.2), 2),
            })
        shipping_cost = round(random.uniform(0, 30), 2)
        docs.append({
            "order_id":    f"ORD-{uuid.uuid4().hex[:10].upper()}",
            "customer_id": random.choice(customer_ids),
            "status":      random.choice(ORDER_STATUSES),
            "items":       items,
            "subtotal":    round(subtotal, 2),
            "shipping_cost": shipping_cost,
            "total_amount":  round(subtotal + shipping_cost, 2),
            "currency":    random.choice(CURRENCIES),
            "shipping_address": {
                "street":  fake.street_address(),
                "city":    fake.city(),
                "country": fake.country_code(),
                "zip":     fake.postcode(),
            },
            "notes":       fake.sentence() if random.random() < 0.2 else None,
            "created_at":  created,
            "updated_at":  created + timedelta(hours=random.randint(1, 48)),
        })
    return docs


def gen_payments(n: int, order_ids: list, customer_ids: list) -> list:
    docs = []
    for _ in range(n):
        created = ts(90)
        docs.append({
            "payment_id":  f"PAY-{uuid.uuid4().hex[:10].upper()}",
            "order_id":    random.choice(order_ids),
            "customer_id": random.choice(customer_ids),
            "amount":      round(random.uniform(10, 3000), 2),
            "currency":    random.choice(CURRENCIES),
            "method":      random.choice(PAY_METHODS),
            "gateway":     random.choice(PAY_GATEWAYS),
            "status":      random.choice(["success", "failed", "pending", "refunded"]),
            "transaction_id": uuid.uuid4().hex,
            "gateway_response": {
                "code":    random.choice(["00", "01", "05"]),
                "message": random.choice(["Approved", "Declined", "Error"]),
            },
            "created_at":  created,
        })
    return docs


def gen_reviews(n: int, product_ids: list, customer_ids: list) -> list:
    docs = []
    for _ in range(n):
        rating  = random.randint(1, 5)
        created = ts(180)
        docs.append({
            "review_id":   f"REV-{uuid.uuid4().hex[:8].upper()}",
            "product_id":  random.choice(product_ids),
            "customer_id": random.choice(customer_ids),
            "rating":      rating,
            "title":       fake.sentence(nb_words=6),
            "body":        fake.paragraph(nb_sentences=random.randint(1, 4)),
            "verified_purchase": random.choice([True, False]),
            "helpful_votes":     random.randint(0, 200),
            "status":      random.choice(["approved", "pending", "rejected"]),
            "sentiment":   "positive" if rating >= 4 else ("neutral" if rating == 3 else "negative"),
            "created_at":  created,
        })
    return docs


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate MongoDB sample data")
    parser.add_argument("--uri",      required=True, help="MongoDB connection URI")
    parser.add_argument("--database", default="enterprise_db")
    parser.add_argument("--records",  type=int, default=500, help="Base record count")
    args = parser.parse_args()

    client = MongoClient(args.uri)
    db     = client[args.database]
    n      = args.records

    print(f"Connected to MongoDB | database={args.database}")

    # Customers
    print(f"Inserting {n} customers…")
    cust_docs = gen_customers(n)
    db.customers.drop()
    db.customers.insert_many(cust_docs)
    db.customers.create_index([("customer_id", ASCENDING)], unique=True)
    db.customers.create_index([("updated_at",  ASCENDING)])
    customer_ids = [d["customer_id"] for d in cust_docs]
    print(f"  ✓ {n} customers inserted")

    # Products
    print(f"Inserting {n // 2} products…")
    prod_docs = gen_products(n // 2)
    db.products.drop()
    db.products.insert_many(prod_docs)
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("updated_at",  ASCENDING)])
    product_ids = [d["product_id"] for d in prod_docs]
    print(f"  ✓ {len(prod_docs)} products inserted")

    # Orders
    print(f"Inserting {n * 3} orders…")
    ord_docs = gen_orders(n * 3, customer_ids, product_ids)
    db.orders.drop()
    db.orders.insert_many(ord_docs)
    db.orders.create_index([("order_id",    ASCENDING)], unique=True)
    db.orders.create_index([("customer_id", ASCENDING)])
    db.orders.create_index([("updated_at",  ASCENDING)])
    order_ids = [d["order_id"] for d in ord_docs]
    print(f"  ✓ {len(ord_docs)} orders inserted")

    # Payments
    print(f"Inserting {n * 2} payments…")
    pay_docs = gen_payments(n * 2, order_ids, customer_ids)
    db.payments.drop()
    db.payments.insert_many(pay_docs)
    db.payments.create_index([("payment_id", ASCENDING)], unique=True)
    db.payments.create_index([("created_at", ASCENDING)])
    print(f"  ✓ {len(pay_docs)} payments inserted")

    # Reviews
    print(f"Inserting {n} reviews…")
    rev_docs = gen_reviews(n, product_ids, customer_ids)
    db.reviews.drop()
    db.reviews.insert_many(rev_docs)
    db.reviews.create_index([("review_id",  ASCENDING)], unique=True)
    db.reviews.create_index([("product_id", ASCENDING)])
    db.reviews.create_index([("created_at", ASCENDING)])
    print(f"  ✓ {len(rev_docs)} reviews inserted")

    print("\n✅ Sample data generation complete!")
    client.close()


if __name__ == "__main__":
    main()
