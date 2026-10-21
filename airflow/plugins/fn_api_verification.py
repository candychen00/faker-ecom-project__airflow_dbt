from pydantic import ValidationError



def verify_api(cust_list, api_schema):
    valid, invalid = [], []
    for i in cust_list:
        try:
            print(f"Start varifying {i}")
            cust = api_schema.model_validate(i)
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



# def verify_customers(cust_list):
#     valid, invalid = [], []
#     for i in cust_list:
#         try:
#             print(f"Start varifying {i}")
#             cust = CustomersBase.model_validate(i)
#         except ValidationError as e:
#             invalid.append({
#                 "Error message": e,
#                 "Error item": i,
#             })
#         else: 
#             valid.append(i)
#         finally:
#             print(f"Finish varifying {i}")
#     return {
#         "valid_count": len(valid),
#         "valid_list": valid,
#         "invalid_count": len(invalid),
#         "invalid_list": invalid,
#     }

