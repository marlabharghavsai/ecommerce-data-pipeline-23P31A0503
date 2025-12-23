import os
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

def test_csv_files_exist():
    files = [
        "customers.csv",
        "products.csv",
        "transactions.csv",
        "transaction_items.csv"
    ]
    for f in files:
        assert os.path.exists(os.path.join(RAW_DIR, f)), f"{f} not found"

def test_customers_columns():
    df = pd.read_csv(os.path.join(RAW_DIR, "customers.csv"))
    required = {"customer_id", "email", "first_name"}
    assert required.issubset(df.columns)

def test_line_total_calculation():
    df = pd.read_csv(os.path.join(RAW_DIR, "transaction_items.csv"))
    sample = df.iloc[0]
    expected = sample["quantity"] * sample["unit_price"] * (1 - sample["discount_percentage"] / 100)
    assert round(expected, 2) == round(sample["line_total"], 2)
