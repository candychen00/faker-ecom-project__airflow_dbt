import pytest

from plugins.fn_api_verification import verify_api      # type: ignore[import-not-found]
from plugins.api_schemas.customers import CustomersBase      # type: ignore[import-not-found]

from pytests.test_get_customers.test_fetch_api_data import fixture_correct_cust_data      # type: ignore[import-not-found]


@pytest.fixture
def fixture_wrong_cust_data():
    return [
        {
            "customer_id": 1, #wrong
            "name": "Julie Wang",
            "email": "robertsonbrandon@example.net",
            "state": "California",
            "signup_date": "2026-01-27"
        },
        {
            "customer_id": "44365862-5af0-43e0-99a5-4a0d7b25331f",
            "name": "Julie", #wrong
            "email": "robertsonbrandon@example.net",
            "state": "California",
            "signup_date": "2026-01-27"
        },
        {
            "customer_id": "44365862-5af0-43e0-99a5-4a0d7b25331f",
            "name": "Julie Wang", 
            "email": "robertsonbrandon", #wrong
            "state": "California",
            "signup_date": "2026-01-27"
        },
        {
            "customer_id": "44365862-5af0-43e0-99a5-4a0d7b25331f",
            "name": "Julie Wang", 
            "email": "robertsonbrandon@example.net", 
            "state": 123, #wrong
            "signup_date": "2026-01-27"
        },
        {
            "customer_id": "44365862-5af0-43e0-99a5-4a0d7b25331f",
            "name": "Julie Wang", 
            "email": "robertsonbrandon@example.net", 
            "state": "California",
            "signup_date": "2026-13-27" #wrong
        },
    ]



def test_correct_cust_api_data(fixture_correct_cust_data):
    result = verify_api(fixture_correct_cust_data, CustomersBase)
    assert result == {
        "valid_count": 2,
        "valid_list": fixture_correct_cust_data,
        "invalid_count": 0,
        "invalid_list": [],
    }


def test_wrong_cust_api_data(fixture_wrong_cust_data):
    result = verify_api(fixture_wrong_cust_data, CustomersBase)
    error0 = result["invalid_list"][0]["Error message"]
    error1 = result["invalid_list"][1]["Error message"]
    error2 = result["invalid_list"][2]["Error message"]
    error3 = result["invalid_list"][3]["Error message"]
    error4 = result["invalid_list"][4]["Error message"]

    assert result["valid_count"] == 0
    assert result["valid_list"] == []
    assert result["invalid_count"] == 5
    assert isinstance(error0, str) and "validation error" in error0 # check if the error message is Validation error
    assert isinstance(error1, str) and "validation error" in error1
    assert isinstance(error2, str) and "validation error" in error2
    assert isinstance(error3, str) and "validation error" in error3
    assert isinstance(error4, str) and "validation error" in error4