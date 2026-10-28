from airflow.sdk import dag, task, get_current_context
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator      #type:ignore

import requests
import os
from dotenv import load_dotenv


load_dotenv()
GCP_CLOUD_STORAGE_BUCKET_NAME = os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")



@dag(
    schedule=None
)
def load_gcs_to_bq():

    upload_products = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        gcp_conn_id="google_cloud_default",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        source_objects = "products/products.csv",
        source_format= "CSV",

        # bq
        destination_project_dataset_table= "raw_data.raw_products",
        create_disposition= "CREATE_IF_NEEDED",
        write_disposition= "WRITE_TRUNCATE",
        location= "us-east1",
        autodetect= True,
    )


    upload_customers = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        gcp_conn_id="google_cloud_default",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        source_objects = "customers/customers.csv",
        source_format= "CSV",

        # bq
        destination_project_dataset_table= "raw_data.raw_customers",
        create_disposition= "CREATE_IF_NEEDED",
        write_disposition= "WRITE_TRUNCATE",
        location= "us-east1",
        autodetect= True,
    )


    upload_orders = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        gcp_conn_id="google_cloud_default",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        source_objects = "orders/orders.csv",
        source_format= "CSV",

        # bq
        destination_project_dataset_table= "raw_data.raw_orders",
        create_disposition= "CREATE_IF_NEEDED",
        write_disposition= "WRITE_TRUNCATE",
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


    upload_cust

load_gcs_to_bq()