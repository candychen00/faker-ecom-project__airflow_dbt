import pytest
import requests
import unittest.mock as mock

from plugins.fn_api_get import get_api_data      # type: ignore


@pytest.fixture
def fixture_correct_cust_data():
    return [
        {
            "customer_id": "aad14268-1087-4756-bff4-bb8cf703c9ff",
            "name": "Julie Guzman",
            "email": "robertsonbrandon@example.net",
            "state": "California",
            "signup_date": "2026-04-27"
        },
        {
            "customer_id": "44365862-5af0-43e0-99a5-4a0d7b25331f",
            "name": "Timothy Chavez",
            "email": "davidchad@example.com",
            "state": "Georgia",
            "signup_date": "2026-05-08"
        },
    ]


@mock.patch("plugins.fn_api_get.requests.get")
def test_fetch_api_data_correct(mock_get, fixture_correct_cust_data):
    mock_res = mock.Mock()
    mock_res.status_code = 200
    mock_res.raise_for_status.return_value = None
    mock_res.json.return_value = fixture_correct_cust_data

    mock_get.return_value = mock_res
    api_result = get_api_data("api.com", {"X-API-Key": 123}, 10)

    assert api_result["status"] == "success"
    assert api_result["data"] == fixture_correct_cust_data


@mock.patch("plugins.fn_api_get.requests.get")
def test_fetch_api_data_wrong(mock_get):
    mock_res = mock.Mock()
    mock_res.status_code = 400
    mock_res.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Error")

    mock_get.return_value = mock_res
    with pytest.raises(requests.exceptions.HTTPError):
        get_api_data("api.com", {"X-API-Key": 123}, 10)