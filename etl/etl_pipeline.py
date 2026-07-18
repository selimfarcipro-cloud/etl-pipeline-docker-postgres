"""
etl_pipeline.py

Extract-Transform-Load pipeline for daily sales data.

Extract : read the raw CSV export.
Transform: parse and validate types, normalize date formats, deduplicate,
           recompute/validate totals, flag and quarantine invalid rows.
Load    : write staging + cleaned dimensional model into PostgreSQL.

Runs inside the Docker container defined in this project (see Dockerfile),
connecting to the `postgres` service defined in docker-compose.yml.
Can also be run locally against any PostgreSQL instance by setting the
standard PG* environment variables (see README).
"""

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("etl")

RAW_CSV_PATH = Path(__file__).parent.parent / "data" / "raw" / "sales_export.csv"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": os.environ.get("PGPORT", "5432"),
    "dbname": os.environ.get("PGDATABASE", "sales_dw"),
    "user": os.environ.get("PGUSER", "etl_user"),
    "password": os.environ.get("PGPASSWORD", "etl_password"),
}


# --------------------------------------------------------------------------
# EXTRACT
# --------------------------------------------------------------------------
def extract(path: Path) -> list[dict]:
    log.info("Extracting raw data from %s", path)
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log.info("Extracted %d raw rows", len(rows))
    return rows


# --------------------------------------------------------------------------
# TRANSFORM
# --------------------------------------------------------------------------
def parse_date(raw_value: str):
    """Accepts both ISO (YYYY-MM-DD) and DD/MM/YYYY formats."""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw_value, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def transform(raw_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Returns (clean_rows, rejected_rows).
    Each rejected row keeps the original payload plus a rejection reason,
    so nothing is silently discarded — everything is auditable.
    """
    seen_transaction_ids = set()
    clean_rows = []
    rejected_rows = []

    for raw in raw_rows:
        reasons = []

        transaction_id = (raw.get("transaction_id") or "").strip()
        if not transaction_id:
            reasons.append("missing transaction_id")
        elif transaction_id in seen_transaction_ids:
            reasons.append("duplicate transaction_id")

        sale_date = parse_date((raw.get("sale_date") or "").strip())
        if sale_date is None:
            reasons.append("unparseable sale_date")

        store_id = (raw.get("store_id") or "").strip()
        if not store_id:
            reasons.append("missing store_id")

        sku = (raw.get("sku") or "").strip()
        product_name = (raw.get("product_name") or "").strip()
        if not product_name:
            product_name = "Unknown Product"
        if sku == "SKU-9999":
            reasons.append("unknown/discontinued SKU")

        try:
            unit_price = round(float(raw.get("unit_price", "")), 2)
            if unit_price <= 0:
                reasons.append("non-positive unit_price")
        except (ValueError, TypeError):
            unit_price = None
            reasons.append("unparseable unit_price")

        try:
            quantity = int(float(raw.get("quantity", "")))
            if quantity <= 0:
                reasons.append("non-positive quantity")
        except (ValueError, TypeError):
            quantity = None
            reasons.append("unparseable quantity")

        try:
            total_amount = round(float(raw.get("total_amount", "")), 2)
        except (ValueError, TypeError):
            total_amount = None
            reasons.append("unparseable total_amount")

        # Recompute expected total and flag mismatches beyond rounding tolerance
        if unit_price is not None and quantity is not None and total_amount is not None:
            expected_total = round(unit_price * quantity, 2)
            if abs(expected_total - total_amount) > 0.05:
                reasons.append(
                    f"total_amount mismatch (expected {expected_total}, got {total_amount})"
                )

        if reasons:
            rejected_rows.append({
                "transaction_id": transaction_id or None,
                "raw_payload": raw,
                "rejection_reason": "; ".join(reasons),
            })
            continue

        seen_transaction_ids.add(transaction_id)
        clean_rows.append({
            "transaction_id": transaction_id,
            "sale_date": sale_date,
            "store_id": store_id,
            "sku": sku,
            "product_name": product_name,
            "unit_price": unit_price,
            "quantity": quantity,
            "total_amount": total_amount,
        })

    log.info(
        "Transform complete: %d clean rows, %d rejected rows",
        len(clean_rows), len(rejected_rows),
    )
    return clean_rows, rejected_rows


# --------------------------------------------------------------------------
# LOAD
# --------------------------------------------------------------------------
def load(raw_rows: list[dict], clean_rows: list[dict], rejected_rows: list[dict]):
    log.info("Connecting to PostgreSQL at %s:%s/%s",
              DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # 1. Staging: load raw data as-is for auditability
            staging_values = [
                (
                    r.get("transaction_id"), r.get("sale_date"), r.get("store_id"),
                    r.get("sku"), r.get("product_name"), r.get("unit_price"),
                    r.get("quantity"), r.get("total_amount"),
                )
                for r in raw_rows
            ]
            execute_values(
                cur,
                """INSERT INTO staging.sales_raw
                   (transaction_id, sale_date, store_id, sku, product_name,
                    unit_price, quantity, total_amount)
                   VALUES %s""",
                staging_values,
            )
            log.info("Loaded %d rows into staging.sales_raw", len(staging_values))

            # 2. Dimensions: upsert stores and products from clean rows
            stores = {(r["store_id"],) for r in clean_rows}
            execute_values(
                cur,
                """INSERT INTO warehouse.dim_store (store_id, store_name)
                   VALUES %s
                   ON CONFLICT (store_id) DO NOTHING""",
                [(s[0], s[0]) for s in stores],
            )

            products = {(r["sku"], r["product_name"], r["unit_price"]) for r in clean_rows}
            execute_values(
                cur,
                """INSERT INTO warehouse.dim_product (sku, product_name, unit_price)
                   VALUES %s
                   ON CONFLICT (sku) DO NOTHING""",
                list(products),
            )
            log.info("Upserted %d stores and %d products", len(stores), len(products))

            # 3. Fact table: cleaned, validated transactions
            fact_values = [
                (
                    r["transaction_id"], r["sale_date"], r["store_id"], r["sku"],
                    r["quantity"], r["unit_price"], r["total_amount"],
                )
                for r in clean_rows
            ]
            execute_values(
                cur,
                """INSERT INTO warehouse.fact_sales
                   (transaction_id, sale_date, store_id, sku, quantity,
                    unit_price, total_amount)
                   VALUES %s
                   ON CONFLICT (transaction_id) DO NOTHING""",
                fact_values,
            )
            log.info("Loaded %d rows into warehouse.fact_sales", len(fact_values))

            # 4. Rejected rows: kept for audit rather than silently dropped
            rejected_values = [
                (r["transaction_id"], json.dumps(r["raw_payload"]), r["rejection_reason"])
                for r in rejected_rows
            ]
            if rejected_values:
                execute_values(
                    cur,
                    """INSERT INTO warehouse.rejected_rows
                       (transaction_id, raw_payload, rejection_reason)
                       VALUES %s""",
                    rejected_values,
                )
            log.info("Logged %d rows into warehouse.rejected_rows", len(rejected_values))

        conn.commit()
        log.info("Transaction committed successfully")
    except Exception:
        conn.rollback()
        log.exception("Load failed, transaction rolled back")
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------
# EXPORT (local artifacts, useful for inspection without a DB connection)
# --------------------------------------------------------------------------
def export_local_artifacts(clean_rows: list[dict], rejected_rows: list[dict]):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    clean_path = PROCESSED_DIR / "sales_clean.csv"
    with open(clean_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(clean_rows[0].keys()))
        writer.writeheader()
        writer.writerows(clean_rows)

    rejected_path = PROCESSED_DIR / "sales_rejected.csv"
    with open(rejected_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["transaction_id", "rejection_reason", "raw_payload"])
        writer.writeheader()
        for r in rejected_rows:
            writer.writerow({
                "transaction_id": r["transaction_id"],
                "rejection_reason": r["rejection_reason"],
                "raw_payload": json.dumps(r["raw_payload"]),
            })

    log.info("Exported local artifacts: %s, %s", clean_path, rejected_path)


def main():
    raw_rows = extract(RAW_CSV_PATH)
    clean_rows, rejected_rows = transform(raw_rows)
    export_local_artifacts(clean_rows, rejected_rows)

    skip_db = os.environ.get("ETL_SKIP_DB", "false").lower() == "true"
    if skip_db:
        log.info("ETL_SKIP_DB=true — skipping PostgreSQL load step")
        return

    load(raw_rows, clean_rows, rejected_rows)
    log.info("ETL pipeline completed successfully")


if __name__ == "__main__":
    main()
