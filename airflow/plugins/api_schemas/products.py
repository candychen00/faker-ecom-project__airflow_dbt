from pydantic import BaseModel, Field
from typing import Literal


class ProductBase(BaseModel):
    product_id: int = Field(ge=0, le=39)
    fruit: Literal['Apple', 'Banana', 'Orange', 'Strawberry', 'Mango', 'Blueberry', 'Peach', 'Pineapple']
    category: Literal['Juice', 'Jam', 'Dried Snacks', 'Smoothie', 'Gummies']
    price: float

