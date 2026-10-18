from airflow.sdk import task, dag, get_current_context
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  # type: ignore
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # type: ignore

import os
from dotenv import load_dotenv
from pprint import pprint
from pathlib import Path
import pandas as pd  # type: ignore

from plugins.fn_get_customers import fetch_api_data, verify_customers   # type: ignore


load_dotenv()
CUSTOMERS_API_URL=os.getenv("CUSTOMERS_API_URL")
CUSTOMERS_API_HEADERS = {
    "X-API-Key": os.getenv("CUSTOMERS_API_KEY")
}
GCP_CLOUD_STORAGE_PROJECT_ID= os.getenv("GCP_CLOUD_STORAGE_PROJECT_ID")
GCP_CLOUD_STORAGE_BUCKET_NAME= os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")



@dag(
    schedule=None
)
def get_customers():

    @task
    def get_api_data():
        cust_num = 2
        result = fetch_api_data(
            input_url = f"{CUSTOMERS_API_URL}/customers/fake_customers?num={cust_num}", 
            input_headers = CUSTOMERS_API_HEADERS, 
            input_timeout = 60
        )
        if result["status"] == "success":
            return result["data"]


    @task
    def verify_api_data(list_customers):
        result = verify_customers(list_customers)
        pprint(result)
        return result


    @task
    def save_to_tmp_folder(cust_list):
        if len(cust_list["valid_list"]) > 0:
            tmp_dir = Path("/tmp")
            tmp_dir.mkdir(parents=True, exist_ok=True)

            df = pd.DataFrame(cust_list["valid_list"])

            output_file = tmp_dir / "customers.csv"
            df.to_csv(output_file, index=False, header=True)
            print("Saved data to tmp folder.")
        else:
            print("Customers valid list is empty. Save nothing to temp folder")


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id= "upload_to_gcs",
        src= "/tmp/customers.csv",
        dst= "date={{ logical_date.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}/customers_{{ logical_date.in_timezone('Asia/Taipei').strftime('%Y%m%dT%H%M%S') }}.csv",
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        gcp_conn_id= "google_cloud_default",
    )


    upload_to_bq = GCSToBigQueryOperator(
        task_id= "upload_to_bq",
        bucket= GCP_CLOUD_STORAGE_BUCKET_NAME,
        source_objects= "date={{ logical_date.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}/*.csv",
        source_format="CSV",
        destination_project_dataset_table= "raw_customers.customers",
        autodetect= True,
        write_disposition="WRITE_APPEND",
    )


    data = get_api_data()
    verified_data = verify_api_data(data)
    save_to_tmp_folder(verified_data) >> upload_to_gcs >> upload_to_bq


get_customers()