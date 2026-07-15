-- init_schema.sql
-- Database schema for the sales ETL pipeline.
-- Executed automatically on first container startup via
-- docker-entrypoint-initdb.d (see docker-compose.yml).

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Staging table: raw data loaded as-is (minimal typing), used for
-- auditability and re-processing without going back to the source file.
CREATE TABLE IF NOT EXISTS staging.sales_raw (
    transaction_id   TEXT,
    sale_date        TEXT,
    store_id         TEXT,
    sku              TEXT,
    product_name     TEXT,
    unit_price       TEXT,
    quantity         TEXT,
    total_amount     TEXT,
    loaded_at        TIMESTAMP DEFAULT NOW()
);

-- Dimension: store
CREATE TABLE IF NOT EXISTS warehouse.dim_store (
    store_id    TEXT PRIMARY KEY,
    store_name  TEXT
);

-- Dimension: product
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    sku           TEXT PRIMARY KEY,
    product_name  TEXT,
    unit_price    NUMERIC(10, 2)
);

-- Fact table: cleaned, validated sales transactions
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    transaction_id   TEXT PRIMARY KEY,
    sale_date        DATE NOT NULL,
    store_id         TEXT REFERENCES warehouse.dim_store(store_id),
    sku              TEXT REFERENCES warehouse.dim_product(sku),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price       NUMERIC(10, 2) NOT NULL,
    total_amount     NUMERIC(10, 2) NOT NULL,
    loaded_at        TIMESTAMP DEFAULT NOW()
);

-- Quarantine table: rows rejected by validation rules, kept for audit
-- and manual review rather than silently dropped.
CREATE TABLE IF NOT EXISTS warehouse.rejected_rows (
    transaction_id   TEXT,
    raw_payload      JSONB,
    rejection_reason TEXT,
    rejected_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON warehouse.fact_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store ON warehouse.fact_sales(store_id);
