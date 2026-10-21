import requests
from pprint import pprint


def get_api_data(input_url: str, input_headers: dict, input_timeout: int = 30):
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