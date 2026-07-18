"""
verify_load.py

Connects to the PostgreSQL warehouse and prints row counts and a few
sanity checks, to confirm the ETL load completed correctly.

Usage (after `docker-compose up --build`):
    docker-compose run --rm etl python etl/verify_load.py
"""

import os

import psycopg2

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": os.environ.get("PGPORT", "5432"),
    "dbname": os.environ.get("PGDATABASE", "sales_dw"),
    "user": os.environ.get("PGUSER", "etl_user"),
    "password": os.environ.get("PGPASSWORD", "etl_password"),
}


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    checks = [
        ("staging.sales_raw", "SELECT COUNT(*) FROM staging.sales_raw"),
        ("warehouse.dim_store", "SELECT COUNT(*) FROM warehouse.dim_store"),
        ("warehouse.dim_product", "SELECT COUNT(*) FROM warehouse.dim_product"),
        ("warehouse.fact_sales", "SELECT COUNT(*) FROM warehouse.fact_sales"),
        ("warehouse.rejected_rows", "SELECT COUNT(*) FROM warehouse.rejected_rows"),
    ]

    print(f"{'Table':<30}{'Row count':>12}")
    print("-" * 42)
    for label, query in checks:
        cur.execute(query)
        count = cur.fetchone()[0]
        print(f"{label:<30}{count:>12,}")

    print("\nTop rejection reasons:")
    cur.execute("""
        SELECT rejection_reason, COUNT(*) AS n
        FROM warehouse.rejected_rows
        GROUP BY rejection_reason
        ORDER BY n DESC
        LIMIT 10
    """)
    for reason, n in cur.fetchall():
        print(f"  {n:>5}  {reason}")

    print("\nTotal revenue by store (fact_sales):")
    cur.execute("""
        SELECT store_id, ROUND(SUM(total_amount)::numeric, 2) AS revenue
        FROM warehouse.fact_sales
        GROUP BY store_id
        ORDER BY revenue DESC
    """)
    for store_id, revenue in cur.fetchall():
        print(f"  {store_id:<12} {revenue:>12,.2f}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
