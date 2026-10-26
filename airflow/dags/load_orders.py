from airflow.sdk import dag, task, get_current_context
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator      # type: ignore
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator      #type:ignore

from plugins.fn_fetch_random import fetch_random_rows       # type: ignore

import pendulum
import os
from dotenv import load_dotenv
import pandas as pd     #type:ignore
from pprint import pprint
import random
from uuid import uuid4
from faker import Faker
from zoneinfo import ZoneInfo
from pathlib import Path
import requests



load_dotenv()
GCP_CLOUD_STORAGE_PROJECT_ID = os.getenv("GCP_CLOUD_STORAGE_PROJECT_ID")
GCP_CLOUD_STORAGE_BUCKET_NAME = os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")



@dag(
        schedule= "@daily",
        start_date= pendulum.datetime( 2026, 8, 1, tz="Asia/Taipei"),
        catchup= False
)
def load_orders():

    @task
    def random_cust():
        result = fetch_random_rows("dim_customers", 30)
        pprint(result)
        return result


    @task
    def random_prod():
        result = fetch_random_rows("dim_products", 30)
        pprint(result)
        return result


    @task
    def generate_orders() -> list[dict]:
        ctx = get_current_context()
        rand_cust = ctx["ti"].xcom_pull(task_ids="random_cust")
        rand_prod = ctx["ti"].xcom_pull(task_ids="random_prod")
        run_date_start = ctx["logical_date"].in_timezone("Asia/Taipei").start_of("day")
        run_date_end = ctx["logical_date"].in_timezone("Asia/Taipei").end_of("day")

        print(f"run_date_start: {run_date_start}")
        print(f"run_date_end: {run_date_end}")

        fake = Faker("en_US")

        orders = []
        for i in range( random.randint(3,12) ):
            one_cust_id = random.choice(rand_cust)["customer_id"]
            one_prod_id = random.choice(rand_prod)["product_id"]

            orders.append({
                "order_id": uuid4(),
                "customer_id": one_cust_id,
                "product_id": one_prod_id,
                "quantity": random.randint(1,20),
                "order_at": fake.date_time_between_dates(
                        datetime_start= run_date_start, 
                        datetime_end= run_date_end, 
                        tzinfo=ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%dT%H:%M:%S"),
            })

        pprint(orders)
        return orders


    @task
    def save_to_tmp(data: list[dict]):
        # create /tmp if not exists
        output_path = Path("/tmp")
        output_path.mkdir(parents=True, exist_ok=True)

        # convert list[dict] -> dataframe
        df = pd.DataFrame(data)

        # convert to csv, and save to /tmp
        output_file = output_path / "orders.csv"
        df.to_csv(output_file, index=False)


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id= "upload_to_gcs",
        gcp_conn_id="google_cloud_default",

        # local
        src= "/tmp/orders.csv",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        dst= 'orders/date={{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%d") }}/orders_{{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%d") }}.csv'
    )


    upload_to_bq = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        gcp_conn_id= "google_cloud_default",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME, 
        source_objects= 'orders/date={{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%d") }}/orders_{{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%d") }}.csv',
        source_format= "CSV",

        # bq
        destination_project_dataset_table= "raw_data.raw_orders",
        create_disposition= "CREATE_IF_NEEDED",
        write_disposition= "WRITE_APPEND",
        location= "us-east1",
        autodetect= True,
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



    get_rand = [random_cust(), random_prod()]
    orders = generate_orders()
    
    get_rand >> orders >> save_to_tmp(orders) >> upload_to_gcs >> upload_to_bq >> send_success_msg()

load_orders()