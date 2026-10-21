from airflow.sdk import dag, task, task_group, get_current_context

from plugins.fn_api_get import get_api_data         #type:ignore
from plugins.fn_api_verification import verify_api      #type:ignore
from plugins.api_schemas.products import ProductBase        #type:ignore

from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.dialects.postgresql import SMALLINT, VARCHAR, NUMERIC

import pandas as pd     #type:ignore
from pandas.testing import assert_frame_equal       #type:ignore
from pytests.test_load_prod_to_pg.complete_prod_data import prod_data       #type:ignore

import requests
import os
from dotenv import load_dotenv
from pprint import pprint



load_dotenv()
ECOM_API_URL = os.getenv("ECOM_API_URL")
ECOM_HEADERS = { "X-API-Key": os.getenv("ECOM_API_KEY")}
POSTGRES_URL = os.getenv("NEON_POSTGRES_URL")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")


# Postgres DB Connection
engine = create_engine(POSTGRES_URL, echo=True)     #type:ignore
metadata = MetaData()



@dag(
    schedule=None
)
def load_products_to_pg():

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
    def verify_prod_tbl():
        df_actual = pd.read_sql(
            con= engine,
            sql= "select * from ecom_products;"
        )
        df_expect = pd.DataFrame(prod_data)
        assert_frame_equal(df_actual, df_expect)



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
    check_prod_tbl = verify_prod_tbl()
    success = send_success_msg()

    api_data >> create_tbl >> branch >> group >> check_prod_tbl >> success
    branch >> check_prod_tbl >> success


load_products_to_pg()