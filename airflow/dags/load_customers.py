from airflow.sdk import task, dag, get_current_context
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  # type: ignore
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # type: ignore
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator        # type: ignore

import os
from dotenv import load_dotenv
from pprint import pprint
from pathlib import Path
import pandas as pd  # type: ignore
import requests

from plugins.fn_api_get import get_api_data     # type: ignore
from plugins.fn_api_verification import verify_api      # type: ignore
from plugins.api_schemas.customers import CustomersBase         # type: ignore



load_dotenv()
ECOM_API_URL=os.getenv("ECOM_API_URL")
ECOM_API_HEADERS = {
    "X-API-Key": os.getenv("ECOM_API_KEY")
}
GCP_CLOUD_STORAGE_PROJECT_ID= os.getenv("GCP_CLOUD_STORAGE_PROJECT_ID")
GCP_CLOUD_STORAGE_BUCKET_NAME= os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")



@dag(
    schedule=None
)
def load_customers():

    @task
    def fetch_api_data():
        cust_num = 50
        result = get_api_data(
            input_url = f"{ECOM_API_URL}/customers/fake_customers?num={cust_num}", 
            input_headers = ECOM_API_HEADERS, 
            input_timeout = 60
        )
        if result["status"] == "success":
            return result["data"]


    @task
    def verify_api_data(list_customers):
        result = verify_api(list_customers, CustomersBase)
        pprint(result)
        return result


    @task
    def save_to_tmp_folder(cust_list):
        if len(cust_list["valid_list"]) > 0:
            # create /tmp if not exists
            tmp_dir = Path("/tmp")
            tmp_dir.mkdir(parents=True, exist_ok=True)

            # convert dict -> df
            df = pd.DataFrame(cust_list["valid_list"])

            # convert dict -> csv
            # save to tmp
            output_file = tmp_dir / "customers.csv"
            df.to_csv(output_file, index=False, header=True)
            print("Saved data to tmp folder.")
        else:
            print("Customers valid list is empty. Save nothing to temp folder")


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id= "upload_to_gcs",
        gcp_conn_id= "google_cloud_default",

        # local
        src= "/tmp/customers.csv",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        dst= "customers/date={{ logical_date.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}/customers_{{ logical_date.in_timezone('Asia/Taipei').strftime('%Y%m%dT%H%M%S') }}.csv",  
    )

    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        gcp_conn_id= "google_cloud_default",

        project_id=GCP_CLOUD_STORAGE_PROJECT_ID,
        dataset_id="raw_data",
        location= "us-east1",
        exists_ok=True, # Dataset 已存在時，不會因為重複建立而失敗
    )


    upload_to_bq = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        gcp_conn_id= "google_cloud_default",

        # gcs
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        source_objects= "customers/date={{ logical_date.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}/*.csv",
        source_format="CSV",

        # bq
        destination_project_dataset_table= "raw_data.raw_customers",
        location= "us-east1",
        autodetect= True,
        write_disposition="WRITE_APPEND",
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


    data = fetch_api_data()
    verified_data = verify_api_data(data)
    save_to_tmp_folder(verified_data) >> upload_to_gcs >> create_bq_dataset >> upload_to_bq >> send_success_msg()


load_customers()