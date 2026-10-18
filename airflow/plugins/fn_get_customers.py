import requests
from pprint import pprint

from pydantic import ValidationError
from plugins.api_validation import CustomersBase # type: ignore[import-not-found]


def fetch_api_data(input_url: str, input_headers: dict, input_timeout: int = 30):
    try:
        print("Connect to API.")
        res = requests.get(url=input_url, headers=input_headers , timeout=input_timeout)
        res.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        raise
    else:
        print("Fetched data from API.")
        pprint(res.json())
        return {
            "status": "success",
            "data": res.json()
        }
    finally:
        print("Finish procesing API.")


def verify_customers(cust_list):
    valid, invalid = [], []
    for i in cust_list:
        try:
            print(f"Start varifying {i}")
            cust = CustomersBase.model_validate(i)
        except ValidationError as e:
            invalid.append({
                "Error message": e,
                "Error item": i,
            })
        else: 
            valid.append(i)
        finally:
            print(f"Finish varifying {i}")
    return {
        "valid_count": len(valid),
        "valid_list": valid,
        "invalid_count": len(invalid),
        "invalid_list": invalid,
    }


