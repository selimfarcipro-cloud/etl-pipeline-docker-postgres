# ETL Pipeline — Docker + PostgreSQL

A containerized **Extract-Transform-Load** pipeline that ingests a raw daily
sales export (CSV), validates and cleans it, and loads it into a
**PostgreSQL** data warehouse using a simple star-schema model — all
orchestrated with **Docker Compose**.

This project illustrates a core DataOps skill set: building a reproducible,
containerized ETL/ELT flow with proper data validation, auditability of
rejected records, and a dimensional warehouse model, rather than a one-off
script.

## Architecture

```
                ┌─────────────────────┐
  CSV export -> │   ETL container     │ -> staging.sales_raw (raw, unvalidated)
 (data/raw/)    │   (Python)          │ -> warehouse.dim_store / dim_product
                │  extract/transform/ │ -> warehouse.fact_sales (clean, validated)
                │  load               │ -> warehouse.rejected_rows (audit trail)
                └─────────┬───────────┘
                          │
                          v
                ┌─────────────────────┐
                │  PostgreSQL         │
                │  container          │
                │  (data warehouse)   │
                └─────────────────────┘
```

Two Docker services, defined in `docker-compose.yml`:

- **`postgres`** — PostgreSQL 16, schema auto-initialized on first boot via
  `sql/init_schema.sql`.
- **`etl`** — Python container that waits for `postgres` to be healthy, then
  runs the pipeline once (extract → transform → load) and exits.

## Project structure

```
etl-pipeline-docker-postgres/
├── Dockerfile
├── docker-compose.yml
├── etl/
│   ├── generate_source_data.py   # Generates a realistic messy CSV export
│   ├── etl_pipeline.py           # Main ETL: extract, transform, load
│   ├── verify_load.py            # Post-load sanity checks (row counts, aggregates)
│   └── requirements.txt
├── sql/
│   └── init_schema.sql           # Star-schema DDL (staging + warehouse)
├── data/
│   ├── raw/sales_export.csv      # Raw input (generated)
│   └── processed/                # Local clean/rejected CSV exports
└── README.md
```

## Data model

- `staging.sales_raw` — raw rows loaded as-is (text columns), kept for
  auditability and reprocessing without re-reading the source file.
- `warehouse.dim_store`, `warehouse.dim_product` — dimension tables.
- `warehouse.fact_sales` — cleaned, validated transactions (foreign keys to
  dimensions, `CHECK (quantity > 0)`).
- `warehouse.rejected_rows` — every row that failed validation, with the
  original payload (as JSONB) and the exact rejection reason. Nothing is
  silently dropped.

## Data quality rules applied in the Transform step

| Rule | Action |
|---|---|
| Missing / duplicate `transaction_id` | Rejected |
| Unparseable date (handles both `YYYY-MM-DD` and `DD/MM/YYYY`) | Rejected if neither format matches |
| Missing `store_id` | Rejected |
| Non-positive `quantity` or `unit_price` | Rejected |
| `total_amount` inconsistent with `unit_price × quantity` (>0.05 tolerance) | Rejected |
| Unknown/discontinued SKU (`SKU-9999`) | Rejected |

## Usage

### 1. Generate the raw source file

```bash
python3 etl/generate_source_data.py
```

### 2. Run the full pipeline (Postgres + ETL)

```bash
docker-compose up --build
```

This starts PostgreSQL, waits for it to be healthy, then runs the ETL
container which extracts, transforms, and loads the data. The `etl`
container exits after completion (job-style, not a long-running service).

### 3. Verify the load

```bash
docker-compose run --rm etl python etl/verify_load.py
```

Prints row counts per table, top rejection reasons, and total revenue by
store.

### 4. Run the ETL logic locally without Docker/Postgres (dry run)

Useful for quickly iterating on transform logic:

```bash
ETL_SKIP_DB=true python3 etl/etl_pipeline.py
```

This still runs extract + transform and writes
`data/processed/sales_clean.csv` and `data/processed/sales_rejected.csv`
locally, but skips the PostgreSQL load step.

## Results on the generated dataset

On a raw export of ~3,060 rows:

- **2,822 rows** passed validation and were loaded into `warehouse.fact_sales`
- **238 rows** were rejected and logged with a specific reason, including:
  - ~96 rows with missing `store_id`
  - ~51 duplicate `transaction_id`
  - ~46 `total_amount` mismatches
  - ~32 non-positive quantities
  - ~15 unknown/discontinued SKUs

## Design choices & possible extensions

- **Reject, don't silently fix**: ambiguous rows (e.g. a price mismatch) are
  quarantined with a reason rather than auto-corrected, since guessing the
  "right" value risks introducing incorrect data into the warehouse.
- **Idempotent loads**: `ON CONFLICT DO NOTHING` on the fact and dimension
  tables means re-running the pipeline on the same file will not create
  duplicate warehouse rows.
- **Next step — orchestration**: this pipeline currently runs as a single
  on-demand container. The natural next step is to schedule and orchestrate
  it (e.g. as an Argo Workflows template or an Airflow DAG) for daily runs —
  see the companion project
  [`data-quality-dashboard-r`](https://github.com/SelimFarci/data-quality-dashboard-r)
  for a complementary R/Shiny data quality monitoring angle on the same kind
  of problem.
