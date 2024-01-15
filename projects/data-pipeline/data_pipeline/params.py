"""Params module to store data pipeline parameters objects"""

from dataclasses import dataclass, field
from typing import List, Optional

from pydantic import BaseModel, Field

DATA_ROOT_FOLDER = "data"

expectation_suites_names = {
    "curated":"curated_flat_structure_suite",
    "consumable": "consumable_flat_structure_suite",
}


data_source_names = {
    "curated":"curated",
    "consumable":"consumable"
}


curated_flat_structures = {
    "sales": None,
    "products": None,
    "orders": None,
    "customers": None,
    "countries": None,
}


columns_to_select = [
    "SaleId",
    "SaleOrderId",
    "SaleProductId",
    "ProductName",
    "ProductManufacturedCountry",
    "CustomerName",
    "CustomerAddress",
    "CustomerCity",
    "CustomerCountry",
    "CustomerEmail",
    "CountryName",
    "CountryCurrency",
    "CountryRegion",
    "OrderDate",
    "CustomerActive",
    "CountryPopulation",
    "CountryAreaSqMi",
    "CountryPopDensityPerSqMi",
    "CountryCoastlineCoastPerAreaRatio",
    "CountryNetMigration",
    "CountryInfantMortalityPer1000Births",
    "CountryGDPPerCapita",
    "CountryLiteracy",
    "CountryPhonesPer1000",
    "CountryArable",
    "CountryCrops",
    "CountryClimate",
    "CountryBirthrate",
    "CountryDeathrate",
    "CountryAgriculture",
    "CountryIndustry",
    "CountryService",
    "ProductWeightGrams",
    "SaleQuantity",
    "ProductWeightGramsPerSaleQuantity",
    "CountryQuantityOverTotalQuantityPercentage",
    "QuantityOverMainCountriesQuantityPercentage",
    "QuantityOverTotalCountryQuantityPercentage",
]


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

    Country: str
    Currency: str
    Name: str
    Region: str
    Population: int
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
    """General data storage for JSON files lines"""

    extracted_json_lines: List[dict] = field(default_factory=lambda: [])
    valid_json_lines: List[dict] = field(default_factory=lambda: [])
    broken_json_lines: List[dict] = field(default_factory=lambda: [])


json_files_validators = {
    "sales": SalesSchema,
    "products": ProductsSchema,
    "orders": OrdersSchema,
    "customers": CustomersSchema,
    "countries": CountriesSchema,
}


@dataclass
class SalesExpectationsStorage:
    """Storage for sales data expectations"""

    columns_to_exist_and_be_not_null: List[str] = field(
        default_factory=lambda: [
            "SaleId",
            "OrderId",
            "ProductId",
            "Quantity",
            "CreatedTimeStamp",
        ]
    )
    columns_to_be_unique: List[str] = field(default_factory=lambda: ["SaleId"])


@dataclass
class ProductsExpectationsStorage:
    """Storage for products data expectations"""

    columns_to_exist_and_be_not_null: List[str] = field(
        default_factory=lambda: [
            "ProductId",
            "Name",
            "ManufacturedCountry",
            "WeightGrams",
            "CreatedTimeStamp",
        ]
    )
    columns_to_be_unique: List[str] = field(
        default_factory=lambda: ["ProductId", "Name"]
    )
    columns_with_length_equal_to: List[str] = field(
        default_factory=lambda: ["ManufacturedCountry"]
    )


@dataclass
class OrdersExpectationsStorage:
    """Storage for orders data expectations"""

    columns_to_exist_and_be_not_null: List[str] = field(
        default_factory=lambda: ["OrderId", "CustomerId", "Date", "CreatedTimeStamp"]
    )
    columns_to_be_unique: List[str] = field(default_factory=lambda: ["OrderId"])


@dataclass
class CustomersExpectationsStorage:
    """Storage for customers data expectations"""

    columns_to_exist_and_be_not_null: List[str] = field(
        default_factory=lambda: [
            "CustomerId",
            "Active",
            "Name",
            "Address",
            "City",
            "Country",
            "Email",
            "CreatedTimeStamp",
        ]
    )
    columns_to_be_unique: List[str] = field(default_factory=lambda: ["CustomerId"])
    columns_with_length_equal_to: List[str] = field(default_factory=lambda: ["Country"])


@dataclass
class CountriesExpectationsStorage:
    """Storage for countries data expectations"""

    columns_to_exist_and_be_not_null: List[str] = field(
        default_factory=lambda: [
            "Country",
            "Currency",
            "Name",
            "Region",
            "Population",
            "AreaSqMi",
            "PopDensityPerSqMi",
            "CoastlineCoastPerAreaRatio",
            "NetMigration",
            "InfantMortalityPer1000Births",
            "GDPPerCapita",
            "Literacy",
            "PhonesPer1000",
            "Arable",
            "Crops",
            "Other",
            "Climate",
            "Birthrate",
            "Deathrate",
            "Agriculture",
            "Industry",
            "Service",
            "CreatedTimeStamp",
        ]
    )
    columns_to_be_unique: List[str] = field(default_factory=lambda: ["Country"])
    columns_with_length_equal_to: List[str] = field(
        default_factory=lambda: ["Country", "Currency"]
    )
    lengths_checks: List[int] = field(default_factory=lambda: [2, 3])


@dataclass
class AnalyticsBaseTableExpectationsStorage:
    """Storage for countries data expectations"""

    columns_to_exist_and_be_not_null: List[str] = field(
        default_factory=lambda: [
            "SaleId",
            "SaleOrderId",
            "SaleProductId",
            "ProductName",
            "ProductManufacturedCountry",
            "CustomerName",
            "CustomerAddress",
            "CustomerCity",
            "CustomerCountry",
            "CustomerEmail",
            "CountryCurrency",
            "CountryName",
            "CountryRegion",
            "OrderDate",
            "CustomerActive",
            "CountryPopulation",
            "CountryAreaSqMi",
            "CountryPopDensityPerSqMi",
            "CountryCoastlineCoastPerAreaRatio",
            "CountryNetMigration",
            "CountryInfantMortalityPer1000Births",
            "CountryGDPPerCapita",
            "CountryLiteracy",
            "CountryPhonesPer1000",
            "CountryArable",
            "CountryCrops",
            "CountryClimate",
            "CountryBirthrate",
            "CountryDeathrate",
            "CountryAgriculture",
            "CountryIndustry",
            "CountryService",
            "ProductWeightGrams",
            "SaleQuantity",
            "ProductWeightGramsPerSaleQuantity",
            "CountryQuantityOverTotalQuantityPercentage",
            "QuantityOverMainCountriesQuantityPercentage",
            "QuantityOverTotalCountryQuantityPercentage",
        ]
    )
    columns_to_be_unique: List[str] = field(default_factory=lambda: ["SaleId"])
    columns_with_length_equal_to: List[str] = field(
        default_factory=lambda: ["CountryCurrency", "ProductManufacturedCountry"]
    )
    lengths_checks: List[int] = field(default_factory=lambda: [3, 2])

currencies_to_select = ["USD", "GBP", "EUR"]
