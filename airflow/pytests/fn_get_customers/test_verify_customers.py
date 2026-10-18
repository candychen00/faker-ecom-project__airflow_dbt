import pytest
from pydantic import ValidationError

from plugins.fn_get_customers import verify_customers # type: ignore[import-not-found]
from pytests.fn_get_customers.test_fetch_api_data import fixture_correct_cust_data # type: ignore[import-not-found]


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
    result = verify_customers(fixture_correct_cust_data)
    assert result == {
        "valid_count": 2,
        "valid_list": fixture_correct_cust_data,
        "invalid_count": 0,
        "invalid_list": [],
    }


def test_wrong_cust_api_data(fixture_wrong_cust_data):
    result = verify_customers(fixture_wrong_cust_data)
    error0 = result["invalid_list"][0]["Error message"]
    error1 = result["invalid_list"][1]["Error message"]
    error2 = result["invalid_list"][2]["Error message"]
    error3 = result["invalid_list"][3]["Error message"]
    error4 = result["invalid_list"][4]["Error message"]

    assert result["valid_count"] == 0
    assert result["valid_list"] == []
    assert result["invalid_count"] == 5
    assert isinstance(error0, ValidationError) # check if the error message is Validation error
    assert isinstance(error1, ValidationError)
    assert isinstance(error2, ValidationError)
    assert isinstance(error3, ValidationError)
    assert isinstance(error4, ValidationError)