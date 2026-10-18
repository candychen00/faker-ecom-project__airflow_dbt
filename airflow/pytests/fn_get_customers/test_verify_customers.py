import pytest
from pydantic import ValidationError

from plugins.fn_get_customers import verify_customers # type: ignore[import-not-found]
from pytests.fn_get_customers.test_fetch_api_data import fixture_correct_cust_data # type: ignore[import-not-found]


@pytest.fixture
def fixture_wrong_cust_data():
    return [
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

    assert result["valid_count"] == 0
    assert result["valid_list"] == []
    assert result["invalid_count"] == 2
    assert isinstance(error0, ValidationError) # check if the error message is Validation error
    assert isinstance(error1, ValidationError)