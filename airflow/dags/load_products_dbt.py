from airflow.sdk import dag, task, get_current_context
from airflow.providers.google.cloud.operators.cloud_run import CloudRunListJobsOperator, CloudRunExecuteJobOperator    #type:ignore

import os
from dotenv import load_dotenv



load_dotenv()
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_CLOUD_STORAGE_BUCKET_NAME = os.getenv("GCP_CLOUD_STORAGE_BUCKET_NAME")
GCP_REGION = os.getenv("GCP_REGION")

GCP_CLOUD_RUN_JOB_NAME = "ecom-pj"



@dag(
    schedule=None
)
def load_products_dbt():

    list_jobs = CloudRunListJobsOperator(
        task_id = "list_jobs",
        gcp_conn_id = "google_cloud_default",

        project_id= GCP_PROJECT_ID,
        region= GCP_REGION,
    )


    @task
    def show_jobs():
        ctx = get_current_context()

        j_output = ctx["ti"].xcom_pull(task_ids="list_jobs")

        j_list = []
        for j in j_output:
            j_list.append(j["name"].split("/")[-1])

        print(f"j_list= {j_list}")
        return j_list


    @task.branch
    def check_job_exists():
        ctx = get_current_context()
        j_list = ctx["ti"].xcom_pull(task_ids="show_jobs")

        print(f"received: {j_list}")

        if GCP_CLOUD_RUN_JOB_NAME in j_list:
            return ["job_exists"]
        else:
            return ["job_not_exists"]


    @task
    def job_not_exists():
        print(f"{GCP_CLOUD_RUN_JOB_NAME} does not exists.")


    @task
    def job_exists():
        print(f"{GCP_CLOUD_RUN_JOB_NAME} exists.")


    run_job = CloudRunExecuteJobOperator(
        task_id = "run_job",
        gcp_conn_id = "google_cloud_default",

        project_id= GCP_PROJECT_ID,
        region= GCP_REGION,

        job_name= GCP_CLOUD_RUN_JOB_NAME,
        timeout_seconds= 180,
        verbose= True,
        overrides={
            "container_overrides": [
                {
                    "args": ["dbt", "build", "-s", "tag:prod", "--target", "ci"],
                }
            ]
        }
    )


    list_j = list_jobs
    show_j = show_jobs()
    branch = check_job_exists()
    exist = job_exists()
    not_exist = job_not_exists()
    run_j = run_job

    list_j >> show_j >> branch >> [exist, not_exist]
    exist >> run_j


load_products_dbt()

