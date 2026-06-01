import uuid
import random
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Home", "Sports", "Toys", "Beauty"]
STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
REGIONS = ["North", "South", "East", "West", "Central", "International"]


def generate_product() -> dict:
    category = random.choice(CATEGORIES)
    return {
        "product_id": str(uuid.uuid4()),
        "name": fake.catch_phrase(),
        "category": category,
        "price": round(random.uniform(5.99, 999.99), 2),
        "stock_quantity": random.randint(0, 500),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_customer() -> dict:
    return {
        "customer_id": str(uuid.uuid4()),
        "email": fake.email(),
        "name": fake.name(),
        "region": random.choice(REGIONS),
        "age_group": random.choice(["18-25", "26-35", "36-45", "46-55", "55+"]),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_order(customer_id: str | None = None, product_id: str | None = None) -> dict:
    qty = random.randint(1, 5)
    unit_price = round(random.uniform(5.99, 499.99), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id or str(uuid.uuid4()),
        "product_id": product_id or str(uuid.uuid4()),
        "quantity": qty,
        "unit_price": unit_price,
        "total_amount": round(qty * unit_price, 2),
        "status": random.choice(STATUSES),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "crypto"]),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "order_created",
        "source": "ecommerce",
    }


def generate_event() -> dict:
    """Randomly produces a product, customer, or order event."""
    roll = random.random()
    if roll < 0.05:
        return {"event_type": "product_created", "source": "ecommerce", **generate_product()}
    elif roll < 0.10:
        return {"event_type": "customer_registered", "source": "ecommerce", **generate_customer()}
    else:
        return generate_order()
