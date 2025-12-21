import os
import json
import random
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
from faker import Faker
import yaml

# --------------------------------------------------
# Load configuration
# --------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(DATA_RAW_DIR, exist_ok=True)

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

fake = Faker()
random.seed(42)
Faker.seed(42)

# --------------------------------------------------
# Helper functions
# --------------------------------------------------
def generate_id(prefix, number, pad):
    return f"{prefix}{str(number).zfill(pad)}"


def random_date(start_date, end_date):
    delta = end_date - start_date
    return start_date + timedelta(days=random.randint(0, delta.days))


# --------------------------------------------------
# Generate Customers
# --------------------------------------------------
def generate_customers():
    customers = []
    used_emails = set()

    age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]

    for i in range(1, config["data_generation"]["customers"] + 1):
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        customers.append({
            "customer_id": generate_id("CUST", i, 4),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(age_groups)
        })

    return pd.DataFrame(customers)


# --------------------------------------------------
# Generate Products
# --------------------------------------------------
def generate_products():
    categories = {
        "Electronics": (500, 50000),
        "Clothing": (500, 5000),
        "Home & Kitchen": (800, 15000),
        "Books": (200, 2000),
        "Sports": (700, 12000),
        "Beauty": (300, 8000)
    }

    products = []

    for i in range(1, config["data_generation"]["products"] + 1):
        category = random.choice(list(categories.keys()))
        min_price, max_price = categories[category]

        price = round(random.uniform(min_price, max_price), 2)
        cost = round(price * random.uniform(0.5, 0.85), 2)

        products.append({
            "product_id": generate_id("PROD", i, 4),
            "product_name": fake.word().title(),
            "category": category,
            "sub_category": fake.word().title(),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 1000),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })

    return pd.DataFrame(products)


# --------------------------------------------------
# Generate Transactions & Transaction Items
# --------------------------------------------------
def generate_transactions(customers_df, products_df):
    transactions = []
    transaction_items = []

    payment_methods = [
        "Credit Card", "Debit Card", "UPI",
        "Cash on Delivery", "Net Banking"
    ]

    start_date = datetime.strptime(config["data_generation"]["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(config["data_generation"]["end_date"], "%Y-%m-%d")

    item_counter = 1
    txn_counter = 1

    for _ in range(config["data_generation"]["transactions"]):
        customer = customers_df.sample(1).iloc[0]
        txn_date = random_date(start_date, end_date)
        txn_id = generate_id("TXN", txn_counter, 5)

        num_items = random.randint(
            config["data_generation"]["min_items_per_txn"],
            config["data_generation"]["max_items_per_txn"]
        )

        total_amount = 0.0

        for _ in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 5)
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                quantity * product["price"] * (1 - discount / 100), 2
            )

            transaction_items.append({
                "item_id": generate_id("ITEM", item_counter, 5),
                "transaction_id": txn_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            total_amount += line_total
            item_counter += 1

        transactions.append({
            "transaction_id": txn_id,
            "customer_id": customer["customer_id"],
            "transaction_date": txn_date.date(),
            "transaction_time": txn_date.time(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": round(total_amount, 2)
        })

        txn_counter += 1

    return (
        pd.DataFrame(transactions),
        pd.DataFrame(transaction_items)
    )


# --------------------------------------------------
# Validation
# --------------------------------------------------
def validate_referential_integrity(customers, products, transactions, items):
    orphan_txn_customers = ~transactions["customer_id"].isin(customers["customer_id"])
    orphan_item_txn = ~items["transaction_id"].isin(transactions["transaction_id"])
    orphan_item_product = ~items["product_id"].isin(products["product_id"])

    violations = (
        orphan_txn_customers.sum()
        + orphan_item_txn.sum()
        + orphan_item_product.sum()
    )

    score = 100 if violations == 0 else max(0, 100 - violations)

    return {
        "orphan_customer_refs": int(orphan_txn_customers.sum()),
        "orphan_transaction_refs": int(orphan_item_txn.sum()),
        "orphan_product_refs": int(orphan_item_product.sum()),
        "data_quality_score": score
    }


# --------------------------------------------------
# Main Execution
# --------------------------------------------------
def main():
    customers_df = generate_customers()
    products_df = generate_products()
    transactions_df, items_df = generate_transactions(customers_df, products_df)

    customers_df.to_csv(os.path.join(DATA_RAW_DIR, "customers.csv"), index=False)
    products_df.to_csv(os.path.join(DATA_RAW_DIR, "products.csv"), index=False)
    transactions_df.to_csv(os.path.join(DATA_RAW_DIR, "transactions.csv"), index=False)
    items_df.to_csv(os.path.join(DATA_RAW_DIR, "transaction_items.csv"), index=False)

    validation = validate_referential_integrity(
        customers_df, products_df, transactions_df, items_df
    )

    metadata = {
        "generated_at": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers_df),
            "products": len(products_df),
            "transactions": len(transactions_df),
            "transaction_items": len(items_df)
        },
        "date_range": {
            "start": config["data_generation"]["start_date"],
            "end": config["data_generation"]["end_date"]
        },
        "validation": validation
    }

    with open(os.path.join(DATA_RAW_DIR, "generation_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=4)

    print("✅ Data generation completed successfully")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
