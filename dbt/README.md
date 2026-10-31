# E-commerce Project - DBT Pipeline

This dbt project (`ecom_dbt_pj`) transforms the raw e-commerce data (customers, products, orders) that Airflow loads into BigQuery into analysis-ready dimension tables, fact tables, and reporting marts. It includes built-in dbt data quality checks, unit tests, and CI/CD.

- **Warehouse**: BigQuery
    - `dbt-bigquery`
    - OAuth authentication
    - location: `us-east1`
- **dbt packages**:
    - `dbt_utils` 1.4.1
    - `dbt_expectations` 0.10.10
- **Python environment**: uv **(Python ≥ 3.12)**
- **dbt Docs**: [https://candychen00.github.io/faker-ecom-project__airflow_dbt/](https://candychen00.github.io/faker-ecom-project__airflow_dbt/) (hosted on GitHub Pages)

<br><br>

## dbt Docs (GitHub Pages)

The dbt documentation site (model descriptions, columns, tests, and the lineage graph) is published to GitHub Pages:

👉 **[https://candychen00.github.io/faker-ecom-project__airflow_dbt/](https://candychen00.github.io/faker-ecom-project__airflow_dbt/)**

> It is rebuilt automatically by the **dbt-ci** job (`.github/workflows/dbt-ci.yml`): after `dbt build --target ci` passes, the job runs `dbt docs generate` and deploys `dbt/ecom_dbt_pj/target/` to GitHub Pages.


<br><br>

## Overall Flow

```
Faker api generates data ──► Airflow DAG ──► BigQuery raw_data ──► dbt models ──► dev / ci bigquery datasets
(airflow/)                  (load_*.py)      raw_customers           │
                                             raw_products            |─ src          (view)
                                             raw_orders              ├─ intermediate (deduplication)
                                                                     ├─ dim / fct    (dimensions, facts)
                                                                     ├─ mart         (aggregated reports)
                                                                     └─ data_quality (duplicate checks)
```

<br><br>

## Model Layers

| Step | Model | Materialization | Description |
|------|-------|-----------------|-------------|
| 1 | `raw_data.*` | – | Source data in the BigQuery `raw_data` dataset |
| 2 | `src_*` | view | Adds surrogate key (method: `dbt_utils.generate_surrogate_key`) |
| 3 | `int_*` | table | Removes duplicate rows by surrogate key |
| 4 | `dim_customers` | table | – |
| 4 | `dim_products` | table | – |
| 4 | `fct_orders` | incremental (merge) | Joins orders with `dim_products`, computes `order_amt` |
| 5 | `mart_daily_by_{prod,cat,fruit}` | table | Daily quantity / sales report |
| 5 | `mart_monthly_by_{prod,cat,fruit}` | table | Monthly quantity / sales report |

<br><br>

## Directory Layers

| Layer | Directory | Materialization | Description |
|------|------|-----------------|------|
| Raw | `models/sources.yml` | – | `ecom_raw` maps to BigQuery `raw_data` dataset tables: `raw_customers` / `raw_products` / `raw_orders` |
| Source | `models/src/` | view | Generates a `*_sk` surrogate key for each record and keeps the original columns |
| Intermediate | `models/intermediate/` | table | Deduplicates by `*_sk` (the raw and source layers may contain duplicates) |
| Dimension | `models/dim/` | table | `dim_customers` and `dim_products`, with cleaned data |
| Fact | `models/fct/` | incremental | `fct_orders` table with `merge` strategy, `unique_key = order_id`, `on_schema_change = fail`; incremental runs only process rows whose `updated_at` is within 3 hours before the current maximum, to cover late-arriving data |
| Mart | `models/mart/` | table | Aggregates `ord_quantity` and `order_amt` by product / category / fruit, daily or monthly |
| Data quality | `models/data_quality/` | table | Duplicate monitoring (see below) |

<br>

### Marts and Macros

`mart_daily_by_prod` / `mart_monthly_by_prod` are the finest grain (product level). The other marts roll up from them via macros, avoiding repeated SQL:

| Macro | Generated marts |
| --- | --- |
| `daily_agg(group_col)` | `mart_daily_by_cat`, `mart_daily_by_fruit` |
| `monthly_agg(group_col)` | `mart_monthly_by_cat`, `mart_monthly_by_fruit` |

<br>

### Data Quality Monitoring

| Model | Purpose | Tables checked |
| --- | --- | --- |
| `dq_dup_count` | Counts duplicate records in each table | 9 tables (see below) |
| `dq_dup_details` | Lists the ids that appear more than once in the three raw tables, along with their counts, to help trace the source | `raw_orders`, `raw_customers`, `raw_products` |

<br>

Tables checked by `dq_dup_count`:

| Entity | Key | Raw | Intermediate | Final |
| --- | --- | --- | --- | --- |
| Orders | `order_id` | `raw_orders` | `int_orders` | `fct_orders` |
| Products | `product_id` | `raw_products` | `int_products` | `dim_products` |
| Customers | `customer_id` | `raw_customers` | `int_customers` | `dim_customers` |

<br>

### Snapshots

`snapshots/` records historical changes (SCD Type 2) for `dim_customers` and `dim_products` using the `timestamp` strategy (with `updated_at` field).

<br><br>

## Testing

| Type | Location | Content |
|------|------|------|
| Contract | `models/schema.yml` | • `contract.enforced` is enabled on `dim_products`, `dim_customers`, and `fct_orders`<br>• Assign column types |
| Generic test | `models/schema.yml` | • `not_null`<br>• `unique`<br>• `accepted_values` (for category, fruit fields)<br>• `relationships` (`fct_orders` <> `dim_products` `dim_customers`) |
| dbt_expectations | `models/schema.yml` | • Value ranges: `product_id` 0–39<br>• `ord_quantity` / `order_amt` > 0<br>• Regex requiring capitalized customer names and state names |
| Custom generic test | `tests/generic/` | • `concat_two_strings`  verifies `product` is the concatenation of their source columns |
| Unit test | `models/unit_tests.yml` | • Verifies the `int_*` deduplication logic<br>• Verifies `dim_products` product construction<br>• Verifies `dim_customers` name splitting |

<br><br>

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

<br><br>

## Running Locally

> - `threads: 1`
> - BigQuery datasets: `dev` (default) and `ci`.


```bash
cd dbt
uv sync


# Required environment variable
GCP_PROJECT_ID=<your-gcp-project>
GCP_SECRET_PATH="../secret/gcp-key.json"


# in profiles.yml
# ↓ Use this when runing on github action
method: oauth
# ↓ Use this when runing on local
method: service-account
keyfile: "{{ env_var('GCP_SECRET_PATH') }}"


cd ecom_dbt_pj
uv run dbt deps
uv run dbt build                   # run + test + snapshot (default target: dev)
uv run dbt build --target ci       # writes to the ci dataset
uv run dbt build --select tag:ord  # only run order-related models (tags: cust / prod / ord / data_quality)
uv run dbt test --select test_type:unit
uv run dbt docs generate && uv run dbt docs serve
```


<br><br>

## CI/CD

`.github/workflows/dbt-ci.yml`: 

(triggered by pushes to `main` that change `dbt/**`, or manually)

a. **dbt-ci**:
  1. `dbt deps`
  2. `dbt parse`
  3. `dbt compile --target ci`
  4. `dbt build --target ci` (including tests)
  5. Delete the `ci` dataset regardless of success or failure
  6. `dbt docs generate`
  7. Deploy dbt docs to [GitHub Pages](https://candychen00.github.io/faker-ecom-project__airflow_dbt/)
  
b. **dbt-cd** (after CI succeeds):
  1. Build an image from the root `Dockerfile` (default command `dbt build`)
  2. Push to Google Artifact Registry (`<sha>` and `latest` tags)
  3. Update the Cloud Run Job `ecom-pj` to use the new image

> Required GitHub Secrets: `GCP_SA_KEY`, `GCP_PROJECT_ID`, `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`

<br><br>
