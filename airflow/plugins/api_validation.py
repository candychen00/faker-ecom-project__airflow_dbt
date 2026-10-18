from pydantic import BaseModel, EmailStr, field_validator
from uuid import UUID
from datetime import date


class CustomersBase(BaseModel):
    customer_id: UUID
    name: str
    email: EmailStr
    state: str
    signup_date: date

    @field_validator("name")
    @classmethod
    def velidate_name(cls, v: str) -> str:
        if len(v.split()) != 2:
            raise ValueError("name must contain exactly 2 words (first and last name).")
        return v

