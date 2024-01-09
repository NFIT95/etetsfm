"""Extractor module to extract data from raw json files"""

import json
from dataclasses import dataclass, field
from typing import List, Optional

from pydantic import BaseModel, ValidationError, Field


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
    Area__sq__mi__: Optional[float]
    Pop__Density__per_sq__mi__: Optional[float]
    Coastline__coast_per_area_ratio_: Optional[float]
    Net_migration: Optional[float]
    Infant_mortality__per_1000_births_: Optional[float]
    GDP____per_capita_: Optional[float]
    Literacy____: Optional[float]
    Phones__per_1000_: Optional[float]
    Arable____: Optional[float]
    Crops____: Optional[float]
    Other____: Optional[float]
    Climate: Optional[float]
    Birthrate: Optional[float]
    Deathrate: Optional[float]
    Agriculture: Optional[float]
    Industry: Optional[float]
    Service: Optional[float]


@dataclass
class Storage:
    """General data storage for validated and broken JSON lines"""

    json_lines: List[dict] = field(default_factory=list)
    json_lines_broken: List[dict] = field(default_factory=list)


def _remove_final_comma(json_line: dict) -> dict:
    """
    Removes final comma from JSON file line if that comma exists
    otherwise return the JSON file line as is

    Args:
        json_line (dict): input JSON file line with final comma

    Returns:
        json_line (dict): input JSON file line without final comma
    """
    if json_line[-2] == ",":
        json_line = json.loads(json_line[:-2])
    else:
        json_line = json.loads(json_line)

    return json_line


def _remove_undesired_characters_from_schema_attributes(json_line: dict) -> dict:
    """
    Removes undesired characters from schema attributes in an input JSON
    file line during extraction to allow for Pydantic validation

    Args:
        json_line (dict): input JSON file line with blank spaces in any attribute

    Returns:
        renamed_json_line (dict): input JSON file line without blank spaces in any attribute
    """
    undesired_characters = [" ", ".", "(", ")", "$", "%"]
    renamed_json_line = {}

    for key, value in json_line.items():
        for character in key:
            if character in undesired_characters:
                key = key.replace(character, "_")
        renamed_json_line[key] = value

    return renamed_json_line


def _validate_json_line_schema(
    json_file_name: str, json_line: dict, storage: Storage
) -> BaseModel:
    """
    Returns a given JSON line schema validator depending on the input JSON file

    Args:
        json_file_name (str): input JSON file name
        json_line (dict): input JSON file name line
        storage: dataclass object with lists to store validated and broken data

    Returns:
        BaseModel: input JSON file specific Pydantic validator
    """
    validators = {
        "sales": SalesSchema,
        "products": ProductsSchema,
        "orders": OrdersSchema,
        "customers": CustomersSchema,
        "countries": CountriesSchema,
    }

    try:
        validators[json_file_name](**json_line)
        storage.json_lines.append(json_line)
        return storage.json_lines
    except ValidationError:
        print("Incorrect schema in JSON line: ", ValidationError)
        storage.json_lines_broken.append(json_line)
        return storage.json_lines_broken


def extract_data_from_json_file(
    json_file_name: str, storage: Storage = Storage
) -> (list[dict], list[dict]):
    """
    Extract data from a JSON file with one JSON object per line

    Args:
        json_file_name (str): input JSON file name
        storage: dataclass object with lists to store validated and broken data

    Returns:
        json_lines (list[dict]): list of input JSON file lines with a validated schema
        json_lines_broken (list[dict]): list of input JSON file lines without a validated schema
    """
    storage = Storage()

    cleaning_functions = [
        _remove_final_comma,
        _remove_undesired_characters_from_schema_attributes,
    ]

    with open(
        "data" + "/" + "raw_data" + "/" + json_file_name + ".json", encoding="utf-8"
    ) as json_file:
        for json_line in json_file:
            for cleaning_function in cleaning_functions:
                json_line = cleaning_function(json_line)

            _validate_json_line_schema(json_file_name, json_line, storage)

    return storage.json_lines, storage.json_lines_broken
