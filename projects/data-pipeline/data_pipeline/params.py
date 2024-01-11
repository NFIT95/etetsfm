"""Params module to store data pipeline parameters objects"""

from dataclasses import dataclass, field
from typing import List, Optional

from pydantic import BaseModel, Field


class SalesSchema(BaseModel):
    """Sales JSON file schema"""

    SaleId: int = Field(strict=True)
    OrderId: int = Field(strict=True)
    ProductId: int = Field(strict=True)
    Quantity: int = Field(strict=True)


class ProductsSchema(BaseModel):
    """Products JSON file schema"""

    ProductId: int = Field(strict=True)
    Name: str = Field(strict=True)
    ManufacturedCountry: str = Field(strict=True)
    WeightGrams: int = Field(strict=True)


class OrdersSchema(BaseModel):
    """Orders JSON file schema"""

    OrderId: int = Field(strict=True)
    CustomerId: int = Field(strict=True)
    Date: str = Field(strict=True)


class CustomersSchema(BaseModel):
    """Customers JSON file schema"""

    CustomerId: int = Field(strict=True)
    Active: bool = Field(strict=True)
    Name: str = Field(strict=True)
    Address: str = Field(strict=True)
    City: str = Field(strict=True)
    Country: str = Field(strict=True)
    Email: str = Field(strict=True)


class CountriesSchema(BaseModel):
    """Countries JSON file schema"""

    Country: str = Field(strict=True)
    Currency: str = Field(strict=True)
    Name: str = Field(strict=True)
    Region: str = Field(strict=True)
    Population: int = Field(strict=True)
    AreaSqMi: Optional[float]
    PopDensityPerSqMi: Optional[float]
    CoastlineCoastPerAreaRatio: Optional[float]
    NetMigration: Optional[float]
    InfantMortalityPer1000Births: Optional[float]
    GDPPerCapita: Optional[float]
    Literacy: Optional[float]
    PhonesPer1000: Optional[float]
    Arable: Optional[float]
    Crops: Optional[float]
    Other: Optional[float]
    Climate: Optional[float]
    Birthrate: Optional[float]
    Deathrate: Optional[float]
    Agriculture: Optional[float]
    Industry: Optional[float]
    Service: Optional[float]


@dataclass
class JsonLinesStorage:
    """General data storage for validated and broken JSON lines"""

    json_lines: List[dict] = field(default_factory=list)
    json_lines_broken: List[dict] = field(default_factory=list)


json_files_validators = {
    "sales": SalesSchema,
    "products": ProductsSchema,
    "orders": OrdersSchema,
    "customers": CustomersSchema,
    "countries": CountriesSchema,
}
