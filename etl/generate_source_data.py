"""
generate_source_data.py

Generates a raw CSV file simulating daily sales exports from a retail
source system. Includes realistic messiness (duplicates, missing values,
inconsistent formatting, invalid rows) that the ETL pipeline is designed
to detect, clean, and load into PostgreSQL.
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "raw" / "sales_export.csv"

PRODUCTS = [
    ("SKU-1001", "Wireless Mouse", 19.99),
    ("SKU-1002", "Mechanical Keyboard", 74.50),
    ("SKU-1003", "USB-C Hub", 29.90),
    ("SKU-1004", "Laptop Stand", 39.00),
    ("SKU-1005", "Webcam 1080p", 45.99),
    ("SKU-1006", "Noise Cancelling Headset", 129.00),
    ("SKU-1007", "Monitor Arm", 89.90),
    ("SKU-1008", "Desk Lamp LED", 24.50),
]

STORES = ["STORE-LAU", "STORE-GEN", "STORE-ZUR", "STORE-BER", "STORE-LUG"]

N_ROWS = 3000
start_date = date(2026, 1, 1)


def random_date():
    offset = random.randint(0, 180)
    return start_date + timedelta(days=offset)


rows = []
for i in range(1, N_ROWS + 1):
    sku, name, unit_price = random.choice(PRODUCTS)
    qty = random.randint(1, 12)
    row = {
        "transaction_id": f"TXN-{i:06d}",
        "sale_date": random_date().isoformat(),
        "store_id": random.choice(STORES),
        "sku": sku,
        "product_name": name,
        "unit_price": unit_price,
        "quantity": qty,
        "total_amount": round(unit_price * qty, 2),
    }
    rows.append(row)

# --- Inject realistic messiness ---

# 1. Exact duplicate transactions (~2%)
dup_sample = random.sample(rows, k=int(N_ROWS * 0.02))
rows.extend(dup_sample)

# 2. Missing values on non-critical fields (~4%)
for row in random.sample(rows, k=int(len(rows) * 0.04)):
    row["product_name"] = ""

for row in random.sample(rows, k=int(len(rows) * 0.03)):
    row["store_id"] = ""

# 3. Negative or zero quantities (data entry errors, ~1%)
for row in random.sample(rows, k=int(len(rows) * 0.01)):
    row["quantity"] = random.choice([-1, 0, -5])
    row["total_amount"] = round(row["unit_price"] * row["quantity"], 2)

# 4. Inconsistent date formats (~2%) — some rows use DD/MM/YYYY instead of ISO
for row in random.sample(rows, k=int(len(rows) * 0.02)):
    d = date.fromisoformat(row["sale_date"])
    row["sale_date"] = d.strftime("%d/%m/%Y")

# 5. Price mismatch: total_amount not matching unit_price * quantity (~1.5%)
for row in random.sample(rows, k=int(len(rows) * 0.015)):
    row["total_amount"] = round(row["total_amount"] * random.uniform(1.5, 3.0), 2)

# 6. Unknown SKU (product discontinued / referential issue, ~0.5%)
for row in random.sample(rows, k=int(len(rows) * 0.005)):
    row["sku"] = "SKU-9999"
    row["product_name"] = "Unknown Product"

random.shuffle(rows)

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} rows -> {OUTPUT_PATH}")
