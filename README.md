# Faker E-commerce Project — Airflow + dbt

An end-to-end ELT pipeline for a fake e-commerce shop. **Airflow** loads raw products, customers and orders into **BigQuery**. **dbt** then turns that raw data into dimension tables, fact tables and reporting marts.

| Folder | Role | Runs on | Details |
|---|---|---|---|
| [`airflow/`](airflow/) | **Extract & Load**: pulls data, checks it, lands it in GCS and BigQuery, and starts dbt | Docker Compose (Airflow 3, `LocalExecutor`) | 👉 [airflow/README.md](airflow/README.md) |
| [`dbt/`](dbt/) | **Transform**: builds the `src` → `int` → `dim` / `fct` → `mart` models, plus tests and data-quality checks | Cloud Run job (Docker image), GitHub Actions for CI | 👉 [dbt/README.md](dbt/README.md) |

**Stack:** Airflow · dbt (`dbt-bigquery`) · BigQuery · Google Cloud Storage · Cloud Run · Artifact Registry · Neon Postgres · GitHub Actions · uv · Docker

**dbt Docs (lineage graph):** [candychen00.github.io/faker-ecom-project__airflow_dbt](https://candychen00.github.io/faker-ecom-project__airflow_dbt/)

<br><br>

## Table of Contents

1. [Architecture overview](#architecture-overview)
2. [What each folder contains](#what-each-folder-contains)
3. [How Airflow and dbt work together](#how-airflow-and-dbt-work-together)
4. [BigQuery datasets](#bigquery-datasets)
5. [Repository layout](#repository-layout)
6. [Getting started](#getting-started)

<br><br>

## Architecture overview

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}}}%%
flowchart LR
    subgraph AF["airflow/  (Extract & Load)"]
        direction TB
        API(Ecom fake-data API)
        PG[(Neon Postgres)]
        GCS[(GCS bucket)]
    end

    subgraph BQ["BigQuery"]
        direction TB
        RAW[(raw_data.raw_*)]
        DEV[(dev.src_* → int_* → dim_* / fct_* → mart_*)]
    end

    subgraph DBT["dbt/  (Transform)"]
        direction TB
        GH[[GitHub Actions<br>dbt-ci]]
        GAR[(Artifact Registry<br>dbt image)]
        CR[[Cloud Run job<br>ecom-pj]]
    end

    API --> PG --> GCS
    API --> GCS
    GCS -- "load_products / load_customers" --> RAW
    DEV -- "load_orders reads dim_*" --> RAW

    GH -- "build & push image" --> GAR --> CR
    CR -- "triggered by load_dbt<br>dbt build --target dev" --> DEV
    RAW --> DEV
```

In one sentence: **Airflow fills `raw_data`; dbt reads `raw_data` and writes the modelled tables to `dev`.** Airflow is also the orchestrator, so it starts the dbt run when the raw data is ready.

<br><br>

## What each folder contains

### `airflow/` — ingestion and orchestration

- **DAGs** (`dags/`):
  - `load_products`: fetches the product catalogue from the API, stores it in Neon Postgres, then copies it to BigQuery.
  - `load_customers`: fetches customers from the API every day and appends them to BigQuery.
  - `load_orders`: generates random orders from existing customers and products every day.
  - `load_dbt`: starts the dbt Cloud Run job.
- **Plugins** (`plugins/`): shared helpers such as API calls and Pydantic schema checks.
- **Tests** (`pytests/`): pytest unit tests for the DAG logic.
- **Runtime**: `Dockerfile` and `docker-compose.yaml` to run Airflow locally.

➡️ DAG diagrams, task tables, `.env` setup and troubleshooting: **[airflow/README.md](airflow/README.md)**

### `dbt/` — transformation and data quality

- **dbt project** (`dbt/ecom_dbt_pj/`):
  - `models/`: the `src`, `intermediate`, `dim`, `fct`, `mart` and `data_quality` layers.
  - `macros/`: reusable daily and monthly aggregations used by the marts.
  - `snapshots/`: SCD history for `dim_customers` and `dim_products`.
  - Tests: generic tests, `dbt_expectations`, and unit tests.
- **Python environment**: `pyproject.toml` and `uv.lock`, managed by uv.

➡️ Model layers, tests, snapshots, local runs and CI/CD details: **[dbt/README.md](dbt/README.md)**

### Root-level files — the bridge between the two

| File | Purpose |
|---|---|
| [`Dockerfile`](Dockerfile) | Packages the `dbt/` folder into an image whose default command is `dbt build`. Cloud Run runs this image. |
| [`.dockerignore`](.dockerignore) | Keeps `airflow/`, secrets and dbt build outputs out of the dbt image. |
| [`.github/workflows/dbt-ci.yml`](.github/workflows/dbt-ci.yml) | CI/CD for dbt: tests the models, publishes dbt Docs, then deploys a new image to the Cloud Run job. |

<br><br>

## How Airflow and dbt work together

Airflow and dbt never import each other's code. They are linked in two ways:

1. **Through BigQuery tables.** Airflow writes the tables that dbt reads, and the other way round.
2. **Through the Cloud Run job `ecom-pj`.** Airflow triggers it, and GitHub Actions keeps its image up to date.

### 1. Runtime flow (data)

| Step | Who | What happens | Reads | Writes |
|---|---|---|---|---|
| 1 | Airflow `load_products` | Loads the product catalogue (API → Postgres → GCS → BigQuery) | Ecom API | `raw_data.raw_products` |
| 2 | Airflow `load_customers` | Loads new customers (API → GCS → BigQuery) | Ecom API | `raw_data.raw_customers` |
| 3 | Airflow `load_dbt` → dbt | Runs `dbt build --target dev` on Cloud Run | `raw_data.*` | `dev.src_*`, `dev.int_*`, `dev.dim_*`, `dev.fct_*`, `dev.mart_*` |
| 4 | Airflow `load_orders` | Generates orders from random existing customers and products | `dev.dim_customers`, `dev.dim_products` | `raw_data.raw_orders` |
| 5 | Airflow `load_dbt` → dbt | Runs dbt again so the new orders reach `fct_orders` and the marts | `raw_data.*` | `dev.*` |

> **Two-way dependency:** `load_orders` reads the dbt-built `dim_*` tables, so dbt must run once **before** the first `load_orders`. On a fresh environment, run:
> `load_products` → `load_customers` → `load_dbt` → `load_orders` → `load_dbt`.
> See [airflow/README.md › How the DAGs fit together](airflow/README.md#how-the-dags-fit-together).

### 2. Deployment flow (code)

```mermaid
flowchart LR
    PUSH["push to main<br>(changes in dbt/**)"] --> CI["dbt-ci job<br>dbt build --target ci"]
    CI -- pass --> DOCS["dbt docs → GitHub Pages"]
    CI -- pass --> CD["dbt-cd job<br>docker build (root Dockerfile)"]
    CD --> GAR[(Artifact Registry)]
    GAR --> JOB[["Cloud Run job ecom-pj<br>updated to new image"]]
    JOB -. "next load_dbt run" .-> AIR["Airflow"]
```

- A change under `dbt/` starts the workflow. It first runs `dbt build` against a temporary `ci` dataset, which is deleted afterwards.
- If CI passes, the workflow publishes the dbt Docs and builds a new Docker image from the root `Dockerfile`. It then points the Cloud Run job `ecom-pj` at that image.
- Airflow doesn't need a redeploy. The next `load_dbt` run uses the latest dbt code.

<br><br>

## BigQuery datasets

| Dataset | Written by | Contents |
|---|---|---|
| `raw_data` | Airflow | `raw_products` (overwrite), `raw_customers` (append), `raw_orders` (append) |
| `dev` | dbt (`--target dev`, via Cloud Run) | Modelled tables: `src_*`, `int_*`, `dim_*`, `fct_orders`, `mart_*`, `dq_*`, snapshots |
| `ci` | dbt (`--target ci`, via GitHub Actions) | Temporary copy used for CI testing; deleted after each run |

<br><br>

## Repository layout

```text
faker-ecom-project__airflow_dbt/
├── airflow/                    # Extract & Load + orchestration   → airflow/README.md
│   ├── dags/                   #   load_products, load_customers, load_orders, load_dbt
│   ├── plugins/                #   shared helpers & Pydantic schemas
│   ├── pytests/                #   unit tests
│   ├── Dockerfile
│   └── docker-compose.yaml
├── dbt/                        # Transform                        → dbt/README.md
│   ├── ecom_dbt_pj/            #   dbt project (models, macros, snapshots, tests)
│   ├── pyproject.toml
│   └── uv.lock
├── Dockerfile                  # dbt image for the Cloud Run job
├── .dockerignore
└── .github/workflows/
    └── dbt-ci.yml              # dbt CI (test + docs) and CD (image → Cloud Run)
```

<br><br>

## Getting started

Each side is set up on its own:

1. **dbt**: install with uv, configure `profiles.yml`, and run `dbt build` locally. Make sure the Cloud Run job `ecom-pj` exists.
   → [dbt/README.md › Running Locally](dbt/README.md#running-locally) and [CI/CD](dbt/README.md#cicd)
2. **Airflow**: add the GCP key and `.env`, run `docker compose up`, then trigger the DAGs in the first-run order.
   → [airflow/README.md › Local setup](airflow/README.md#local-setup)

**You'll need:**

- A GCP project with BigQuery, Cloud Storage, Cloud Run and Artifact Registry
- A service account key for that project
- A Neon Postgres database
- The Ecom fake-data API URL and key
- A Discord webhook for notifications
