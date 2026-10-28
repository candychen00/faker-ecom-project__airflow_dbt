# dbt – Fruit E-commerce Data Warehouse

This dbt project (`ecom_dbt_pj`) transforms the raw e-commerce data (customers, products, orders) that Airflow loads into BigQuery into analysis-ready dimension tables, fact tables, and reporting marts. It includes built-in data quality checks, unit tests, and CI/CD.

- **Warehouse**: BigQuery
    - `dbt-bigquery`
    - OAuth authentication
    - location: `us-east1`
- **dbt packages**:
    - `dbt_utils` 1.4.1
    - `dbt_expectations` 0.10.10
- **Python environment**: uv (Python ≥ 3.12)

## Overall Flow

```
Faker generates data ──► Airflow DAG ──► BigQuery raw_data ──► dbt ──► dev / ci dataset
(airflow/)               (load_*.py)     raw_customers            │
                                         raw_products             ├─ src          (view)
                                         raw_orders               ├─ intermediate (deduplication)
                                                                  ├─ dim / fct    (dimensions, facts)
                                                                  ├─ mart         (aggregated reports)
                                                                  └─ data_quality (duplicate checks)
```

## Model Layers

```
raw_data.* (source: ecom_raw)
   │
   ▼
src_*            view   adds surrogate key (dbt_utils.generate_surrogate_key)
   │
   ▼
int_*            table  removes duplicate rows by surrogate key (row_number = 1)
   │
   ▼
dim_customers    table  splits first_name / last_name
dim_products     table  builds product = fruit + category
fct_orders       incremental (merge)  joins orders to product prices, computes order_amt
   │
   ▼
mart_daily_by_{prod,cat,fruit}      table  daily quantity / sales
mart_monthly_by_{prod,cat,fruit}    table  monthly quantity / sales
```

| Layer | Directory | Materialization | Description |
|------|------|-----------------|------|
| Source | `models/sources.yml` | – | `ecom_raw` maps to BigQuery `raw_data` tables `raw_customers` / `raw_products` / `raw_orders`; the project is set via the `GCP_PROJECT_ID` environment variable |
| Staging | `models/src/` | view | Generates a `*_sk` surrogate key for each record and keeps the original columns |
| Intermediate | `models/intermediate/` | table | Deduplicates by `*_sk` (the raw layer may contain duplicates from repeated loads) |
| Dimension | `models/dim/` | table | `dim_customers`, `dim_products`, with `created_at` / `updated_at` |
| Fact | `models/fct/` | incremental | `fct_orders`: `merge` strategy, `unique_key = order_id`, `on_schema_change = fail`; incremental runs only process rows whose `updated_at` is within 3 hours before the current maximum, to cover late-arriving data |
| Mart | `models/mart/` | table | Aggregates `ord_quantity` and `order_amt` by product / category / fruit, daily or monthly |
| Data quality | `models/data_quality/` | table | Duplicate monitoring (see below) |

### Marts and Macros

`mart_daily_by_prod` / `mart_monthly_by_prod` are the finest grain (product level). The other marts roll up from them via macros, avoiding repeated SQL:

- `daily_agg(group_col)`: `mart_daily_by_cat`, `mart_daily_by_fruit`
- `monthly_agg(group_col)`: `mart_monthly_by_cat`, `mart_monthly_by_fruit`

### Data Quality Monitoring

- `dq_dup_count`: compares `COUNT(*) - COUNT(DISTINCT key)` across raw → intermediate → dim/fct to confirm at which layer duplicates are removed, and includes the run time in Taipei time zone.
- `dq_dup_details`: lists the ids that appear more than once in the three raw tables, along with their counts, to help trace the source.

### Snapshots

`snapshots/` records historical changes (SCD Type 2) for `dim_customers` and `dim_products` using the `timestamp` strategy (`updated_at`).

## Testing

| Type | Location | Content |
|------|------|------|
| Contract | `models/schema.yml` | `contract.enforced` is enabled on `dim_products`, `dim_customers`, and `fct_orders`, fixing column types |
| Generic test | `models/schema.yml` | `not_null`, `unique`, `accepted_values` (category, fruit), `relationships` (`fct_orders` → the two dims) |
| dbt_expectations | `models/schema.yml` | Value ranges (`product_id` 0–39, `ord_quantity` / `order_amt` > 0), regex requiring capitalized names / state names |
| Custom generic test | `tests/generic/` | `concat_two_strings`: verifies derived columns (`full_name`, `product`) equal the concatenation of their source columns |
| Unit test | `models/unit_tests.yml` | Verifies the `int_*` deduplication logic, `dim_products` product construction, and `dim_customers` name splitting |

## Project Structure

```
dbt/
├── pyproject.toml / uv.lock     # Python dependencies (dbt-bigquery, python-dotenv)
├── .env                         # Local environment variables (do not commit)
├── secret/                      # GCP keys (do not commit)
└── ecom_dbt_pj/
    ├── dbt_project.yml
    ├── profiles.yml             # targets: dev / ci
    ├── packages.yml
    ├── models/                  # src, intermediate, dim, fct, mart, data_quality
    ├── macros/                  # daily_agg, monthly_agg
    ├── snapshots/
    └── tests/generic/
```

## Running Locally

```bash
cd dbt
uv sync

# Required environment variable
export GCP_PROJECT_ID=<your-gcp-project>
gcloud auth application-default login      # profiles.yml uses oauth

cd ecom_dbt_pj
uv run dbt deps
uv run dbt build                  # run + test + snapshot (default target: dev)
uv run dbt build --target ci      # writes to the ci dataset
uv run dbt build --select tag:ord # only run order-related models (tags: cust / prod / ord / data_quality)
uv run dbt test --select test_type:unit
uv run dbt docs generate && uv run dbt docs serve
```

> `threads: 1`; BigQuery datasets: `dev` (default) and `ci`.

## CI/CD

`.github/workflows/dbt-ci.yml` (triggered by pushes to `main` that change `dbt/**`, or manually):

1. **dbt-ci**: `dbt deps` → `dbt parse` → `dbt compile --target ci` → `dbt build --target ci` (including tests) → delete the `ci` dataset regardless of success or failure.
2. **dbt-cd** (after CI succeeds): build an image from the root `Dockerfile` (default command `dbt build`) → push to Google Artifact Registry (`<sha>` and `latest` tags) → update the Cloud Run Job `ecom-pj` to use the new image.

Required GitHub Secrets: `GCP_SA_KEY`, `GCP_PROJECT_ID`, `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`.
