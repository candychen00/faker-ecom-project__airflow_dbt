from airflow.sdk import dag, task, get_current_context
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator      #type:ignore


import os
from dotenv import load_dotenv


load_dotenv()
GCP_CLOUD_STORAGE_BUCKET_NAME = os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")



