
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook      #type:ignore

import os
from dotenv import load_dotenv
import pandas as pd     #type:ignore
from pprint import pprint



load_dotenv()
GCP_CLOUD_STORAGE_PROJECT_ID = os.getenv("GCP_CLOUD_STORAGE_PROJECT_ID")



def fetch_random_rows(bq_table: str, num_records: int) -> list[dict]:
    hook = BigQueryHook(
        gcp_conn_id= "google_cloud_default",
        use_legacy_sql= False,
        location= "us-east1"
    )
    df = hook.get_df(
        sql= f"select * from `{GCP_CLOUD_STORAGE_PROJECT_ID}.dev.{bq_table}`;"
    )

    # fetch random records
    df_rand = df.sample(num_records)

    # XCom can't serialize pandas.Timestamp, so cast datetime columns to str
    datetime_cols = df_rand.select_dtypes(include=["datetime", "datetimetz"]).columns
    df_rand[datetime_cols] = df_rand[datetime_cols].astype(str)

    # convert df -> dict
    return df_rand.to_dict(orient="records")