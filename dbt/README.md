# dbt – Fruit E-commerce Data Warehouse

這個 dbt project（`ecom_dbt_pj`）負責把 Airflow 載入 BigQuery 的原始電商資料（客戶、商品、訂單），轉換成可分析的維度表、事實表與報表 mart，並內建資料品質檢查、單元測試與 CI/CD。

- **Warehouse**：BigQuery
    - `dbt-bigquery`
    - OAuth 驗證
    - location: `us-east1`
- **Dbt 套件**：
    - `dbt_utils` 1.4.1
    - `dbt_expectations` 0.10.10
- **Python 環境**：uv（Python ≥ 3.12）

## 整體流程

```
Faker 產生資料 ──► Airflow DAG ──► BigQuery raw_data ──► dbt ──► dev / ci dataset
(airflow/)        (load_*.py)     raw_customers            │
                                  raw_products             ├─ src          (view)
                                  raw_orders               ├─ intermediate (去重)
                                                           ├─ dim / fct    (維度、事實)
                                                           ├─ mart         (彙總報表)
                                                           └─ data_quality (重複檢查)
```

## Model 分層

```
raw_data.* (source: ecom_raw)
   │
   ▼
src_*            view   加上 surrogate key (dbt_utils.generate_surrogate_key)
   │
   ▼
int_*            table  依 surrogate key 去除重複列 (row_number = 1)
   │
   ▼
dim_customers    table  拆 first_name / last_name
dim_products     table  組合 product = fruit + category
fct_orders       incremental (merge)  訂單 JOIN 商品價格，計算 order_amt
   │
   ▼
mart_daily_by_{prod,cat,fruit}      table  每日銷量/銷售額
mart_monthly_by_{prod,cat,fruit}    table  每月銷量/銷售額
```

| 層級 | 目錄 | Materialization | 說明 |
|------|------|-----------------|------|
| Source | `models/sources.yml` | – | `ecom_raw` 對應 BigQuery `raw_data` 的 `raw_customers` / `raw_products` / `raw_orders`；project 由環境變數 `GCP_PROJECT_ID` 指定 |
| Staging | `models/src/` | view | 為每筆資料產生 `*_sk` surrogate key，保留原始欄位 |
| Intermediate | `models/intermediate/` | table | 以 `*_sk` 去除重複資料（raw 層可能因重複載入產生 duplicate） |
| Dimension | `models/dim/` | table | `dim_customers`、`dim_products`，含 `created_at` / `updated_at` |
| Fact | `models/fct/` | incremental | `fct_orders`：`merge` 策略、`unique_key = order_id`、`on_schema_change = fail`；增量時只處理 `updated_at` 在目前最大值前 3 小時內的資料，以涵蓋延遲到達的資料 |
| Mart | `models/mart/` | table | 依商品 / 類別 / 水果，按日或月彙總 `ord_quantity`、`order_amt` |
| Data quality | `models/data_quality/` | table | 重複資料監控（見下方） |

### Mart 與 Macro

`mart_daily_by_prod` / `mart_monthly_by_prod` 是最細粒度（商品層級）。其餘 mart 透過 macro 從它們再往上彙總，避免重複 SQL：

- `daily_agg(group_col)`：`mart_daily_by_cat`、`mart_daily_by_fruit`
- `monthly_agg(group_col)`：`mart_monthly_by_cat`、`mart_monthly_by_fruit`

### Data Quality 監控

- `dq_dup_count`：比較 raw → intermediate → dim/fct 各層 `COUNT(*) - COUNT(DISTINCT key)`，確認重複資料在哪一層被清掉，並附上台北時區的執行時間。
- `dq_dup_details`：列出 raw 三張表中重複出現的 id 與筆數，方便追查來源。

### Snapshots

`snapshots/` 針對 `dim_customers`、`dim_products` 以 `timestamp` 策略（`updated_at`）記錄歷史變化（SCD Type 2）。

## 測試

| 類型 | 位置 | 內容 |
|------|------|------|
| Contract | `models/schema.yml` | `dim_products`、`dim_customers`、`fct_orders` 啟用 `contract.enforced`，欄位型別固定 |
| Generic test | `models/schema.yml` | `not_null`、`unique`、`accepted_values`（類別、水果）、`relationships`（`fct_orders` → 兩張 dim） |
| dbt_expectations | `models/schema.yml` | 數值範圍（`product_id` 0–39、`ord_quantity` / `order_amt` > 0）、姓名 / 州名需首字大寫的 regex |
| 自訂 generic test | `tests/generic/` | `concat_two_strings`：驗證衍生欄位（`full_name`、`product`）等於來源欄位串接結果 |
| Unit test | `models/unit_tests.yml` | 驗證 `int_*` 去重邏輯、`dim_products` 組合 product、`dim_customers` 拆姓名 |

## 專案結構

```
dbt/
├── pyproject.toml / uv.lock     # Python 依賴 (dbt-bigquery, python-dotenv)
├── .env                         # 本機環境變數（不要 commit）
├── secret/                      # GCP 金鑰（不要 commit）
└── ecom_dbt_pj/
    ├── dbt_project.yml
    ├── profiles.yml             # targets: dev / ci
    ├── packages.yml
    ├── models/                  # src, intermediate, dim, fct, mart, data_quality
    ├── macros/                  # daily_agg, monthly_agg
    ├── snapshots/
    └── tests/generic/
```

## 本機執行

```bash
cd dbt
uv sync

# 需要的環境變數
export GCP_PROJECT_ID=<your-gcp-project>
gcloud auth application-default login      # profiles.yml 使用 oauth

cd ecom_dbt_pj
uv run dbt deps
uv run dbt build                  # run + test + snapshot（預設 target: dev）
uv run dbt build --target ci      # 寫入 ci dataset
uv run dbt build --select tag:ord # 只跑訂單相關 model（tags: cust / prod / ord / data_quality）
uv run dbt test --select test_type:unit
uv run dbt docs generate && uv run dbt docs serve
```

> `threads: 1`，BigQuery dataset：`dev`（預設）與 `ci`。

## CI/CD

`.github/workflows/dbt-ci.yml`（`dbt/**` 有變動並 push 到 `main`，或手動觸發）：

1. **dbt-ci**：`dbt deps` → `dbt parse` → `dbt compile --target ci` → `dbt build --target ci`（含測試）→ 無論成功與否，刪除 `ci` dataset。
2. **dbt-cd**（CI 成功後）：用根目錄 `Dockerfile` build image（預設指令 `dbt build`）→ push 到 Google Artifact Registry（`<sha>` 與 `latest` tag）→ 更新 Cloud Run Job `ecom-pj` 使用新 image。

所需 GitHub Secrets：`GCP_SA_KEY`、`GCP_PROJECT_ID`、`DOCKERHUB_USERNAME`、`DOCKERHUB_TOKEN`。
