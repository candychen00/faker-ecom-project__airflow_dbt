import pytest
from pydantic import ValidationError

from pytests.test_load_prod_to_pg.complete_prod_data import prod_data      #type:ignore

from plugins.fn_api_verification import verify_api  #type:ignore
from plugins.api_schemas.products import ProductBase    #type:ignore



@pytest.fixture
def correct_prod_api_data():
    return prod_data


@pytest.fixture
def wrong_prod_api_data():
    return [
        {
            "product_id": 41,   #wrong
            "fruit": "Pineapple",
            "category": "Gummies",
            "price": 16.75
        },
        {
            "product_id": 39,
            "fruit": "Fish",    #wrong
            "category": "Gummies",
            "price": 16.75
        },
        {
            "product_id": 39,
            "fruit": "Pineapple",
            "category": "Furniture",    #wrong
            "price": 16.75
        },
        {
            "product_id": 39,
            "fruit": "Pineapple",
            "category": "Gummies",
            "price": "a"      #wrong
        }
    ]




def test_correct_prod_api_data(correct_prod_api_data):
    result = verify_api(correct_prod_api_data, ProductBase)

    assert result["valid_count"] == 40
    assert result["valid_list"] == correct_prod_api_data
    assert result["invalid_count"] == 0
    assert result["invalid_list"] == []



def test_wrong_prod_api_data(wrong_prod_api_data):
    result = verify_api(wrong_prod_api_data, ProductBase)

    error0 = result["invalid_list"][0]["Error message"]
    error1 = result["invalid_list"][1]["Error message"]
    error2 = result["invalid_list"][2]["Error message"]
    error3 = result["invalid_list"][3]["Error message"]

    assert isinstance(error0, ValidationError) # check if the error message is Validation error
    assert isinstance(error1, ValidationError)
    assert isinstance(error2, ValidationError)
    assert isinstance(error3, ValidationError)