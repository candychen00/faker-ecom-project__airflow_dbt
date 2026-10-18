from airflow.sdk import task, dag
import requests
import os
from dotenv import load_dotenv
from pprint import pprint

from pydantic import ValidationError
from plugins.api_validation import CustomersBase # type: ignore[import-not-found]

from plugins.fn_get_customers import fetch_api_data, verify_customers # type: ignore[import-not-found]


load_dotenv()
CUSTOMERS_API_URL=os.getenv("CUSTOMERS_API_URL")
CUSTOMERS_API_HEADERS = {
    "X-API-Key": os.getenv("CUSTOMERS_API_KEY")
}

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
        wrong = [
            {
                "customer_id": "aad14268-1087-4756-bff4-bb8cf703c9ff",
                "name": "Julie", #wrong
                "email": "robertsonbrandon@example.net",
                "state": "California",
                "signup_date": "2026-13-27" #wrong
            },
            {
                "customer_id": "44365862-5af0-43e0-99a5-4a0d7b25331f",
                "name": "Timothy", #wrong
                "email": "davidchad@example.com",
                "state": "Georgia",
                "signup_date": "2026-05-08"
            },
        ]
        result = verify_customers(wrong)
        pprint(result)
        return result



    data = get_api_data()
    verify_api_data(data)

get_customers()