from airflow.sdk import dag, task, task_group, get_current_context
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator      #type:ignore
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator      #type:ignore

from plugins.fn_api_get import get_api_data         #type:ignore
from plugins.fn_api_verification import verify_api      #type:ignore
from plugins.api_schemas.products import ProductBase        #type:ignore

from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.dialects.postgresql import SMALLINT, VARCHAR, NUMERIC

import pandas as pd     #type:ignore
import pyarrow as pa        #type:ignore
import pyarrow.parquet as pq        #type:ignore
from pandas.testing import assert_frame_equal       #type:ignore
from pytests.test_load_products.complete_prod_data import prod_data       #type:ignore

import requests
import os
from dotenv import load_dotenv
from pprint import pprint
from pathlib import Path
import json
from decimal import Decimal



# load env variables
load_dotenv()

ECOM_API_URL = os.getenv("ECOM_API_URL")
ECOM_HEADERS = { "X-API-Key": os.getenv("ECOM_API_KEY")}
POSTGRES_URL = os.getenv("NEON_POSTGRES_URL")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

GCP_CLOUD_STORAGE_BUCKET_NAME = os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")


# Postgres DB Connection
engine = create_engine(POSTGRES_URL, echo=True)     #type:ignore
metadata = MetaData()




@dag(
    schedule=None
)
def load_products():

    @task
    def fetch_products_api():

        result = get_api_data(
            input_url= f"{ECOM_API_URL}/products/generate_all_products",
            input_headers=ECOM_HEADERS,
            input_timeout= 60,
        )
        if result["status"] == "success":
            return result["data"]


    @task
    def create_table_if_not_exists():

        ecom_products = Table(
            "ecom_products",
            metadata,
            Column("product_id", SMALLINT, primary_key=True ),
            Column("fruit", VARCHAR(30) ),
            Column("category", VARCHAR(30) ),
            Column("price", NUMERIC(8, 2) )
        )

        metadata.create_all(engine)


    @task.branch
    def check_prod_table_len():

        with engine.connect() as conn:
            result = conn.execute( text("select count(*) from ecom_products"))
            count = result.scalar()
            print(count)
        if count == 40:
            return "verify_prod_tbl"
        elif count == 0:
            return "save_data_to_pg_group"


    @task_group
    def save_data_to_pg_group(data):

        @task
        def verify_api_data(data):
            result = verify_api(data, ProductBase)
            pprint(result)
            return result
        
        @task
        def insert_data_to_pg(data):

            if data["valid_count"] > 0:
                products = Table("ecom_products", metadata, autoload_with=engine)
                with engine.begin() as conn:
                    stmt = products.insert()
                    conn.execute(stmt, data["valid_list"])
            else:
                print("valid_count length is zero. Add no data to ecom_products postgresDB.")


        verified_data = verify_api_data(data)
        insert_data_to_pg(verified_data) 


    @task(trigger_rule="none_failed_min_one_success")
    def verify_prod_tbl() -> list[dict]:
        df_actual = pd.read_sql(
            con= engine,
            sql= "select * from ecom_products;"
        )
        df_expect = pd.DataFrame(prod_data)

        # conpare if df_actual and df_expect is the same
        assert_frame_equal(df_actual, df_expect)

        return df_actual.to_dict(orient="records")


    @task
    def save_to_tmp(data: list[dict]):
        # create /tmp folder if not exists
        output_path = Path("/tmp")
        output_path.mkdir(parents=True, exist_ok=True)

        # convert dict -> df
        df = pd.DataFrame(data)
        # tz-naive so parquet writes DATETIME-compatible timestamps
        now = pd.Timestamp.now(tz="Asia/Taipei").tz_localize(None)
        df["updated_at"] = now
        df["created_at"] = now
        df[["created_at", "updated_at"]] = df[["created_at", "updated_at"]].astype("datetime64[us]")
        # XCom JSON turns Decimal into float; pyarrow needs Decimal objects for decimal128
        df["price"] = df["price"].apply(lambda x: Decimal(str(x)).quantize(Decimal("0.01")))

        # convert df -> parquet
        # save parquet to tmp
        output_file = output_path / "products.parquet"
        
        # price must be a parquet DECIMAL (not DOUBLE) to load into a BQ NUMERIC column
        schema = pa.schema([
            ("product_id", pa.int64()),
            ("fruit", pa.string()),
            ("category", pa.string()),
            ("price", pa.decimal128(8, 2)),      # → NUMERIC
            ("created_at", pa.timestamp("us")),  # → DATETIME
            ("updated_at", pa.timestamp("us")),  # → DATETIME
        ])
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        pq.write_table(table, output_file, compression="snappy")


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id= "upload_to_gcs",
        gcp_conn_id="google_cloud_default",

        # local
        src= "/tmp/products.parquet",

        # gsc
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        dst= "products/products.parquet",
    )


    upload_to_bq = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        gcp_conn_id="google_cloud_default",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        source_objects = "products/products.parquet",
        source_format= "PARQUET",

        # bq
        destination_project_dataset_table= "raw_data.raw_products",
        create_disposition= "CREATE_IF_NEEDED",
        write_disposition= "WRITE_TRUNCATE",
        location= "us-east1",
        schema_fields=[
            { "name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
            { "name": "fruit", "type": "STRING", "mode": "REQUIRED"},
            { "name": "category", "type": "STRING", "mode": "REQUIRED"},
            { "name": "price", "type": "NUMERIC", "mode": "REQUIRED"},
            { "name": "updated_at", "type": "DATETIME", "mode": "REQUIRED"},
            { "name": "created_at", "type": "DATETIME", "mode": "REQUIRED"},
        ],
        autodetect= False,
    )
        

    @task
    def send_success_msg():
        ctx = get_current_context()
        dag_name = ctx["dag"].dag_id
        run_time = ctx["logical_date"].in_timezone("Asia/Taipei").strftime("%Y-%m-%d %H:%M")

        webhook_url = DISCORD_WEBHOOK
        requests.post(webhook_url, json={
            "content": 
            f"Airflow Dag: {dag_name}\n"
            f"Datetime: {run_time}\n"
            "Finished run."
        })




    api_data = fetch_products_api()
    create_tbl = create_table_if_not_exists()
    branch = check_prod_table_len()
    group = save_data_to_pg_group(api_data)
    verified_data = verify_prod_tbl()
    to_tmp = save_to_tmp(verified_data)
    to_gcs = upload_to_gcs
    to_bq = upload_to_bq
    success = send_success_msg()

    api_data >> create_tbl >> branch >> group >> verified_data >> to_tmp >> to_gcs >> to_bq >> success
    branch >> verified_data >> to_tmp >> to_gcs >> to_bq >> success


load_products()