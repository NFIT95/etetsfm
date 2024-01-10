"""Checker module to define data quality checks via in-memory gx"""

from dataclasses import dataclass, field
from typing import List

import great_expectations as gx
import pandas as pd
from great_expectations.data_context import FileDataContext

GX_DATA_CONTEXT_FOLDER = "tools/gx/data_context"


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
    columns_to_be_unique: List[str] = field(
        default_factory=lambda: ["SaleId"]
    )


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
    columns_to_be_unique: List[str] = field(
        default_factory=lambda: ["OrderId"]
    )


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
    columns_to_be_unique: List[str] = field(
        default_factory=lambda: ["CustomerId"]
    )
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
    columns_to_be_unique: List[str] = field(
        default_factory=lambda: ["Country"]
    )
    columns_with_length_equal_to: List[str] = field(
        default_factory=lambda: ["Country", "Currency"]
    )
    lengths_checks: List[int] = field(
        default_factory=lambda: [2, 3]
    )


def create_gx_filesystem_context() -> gx.DataContext:
    """
    Create great_expectations FileSystem DataContext
    
    Returns:
        context (gx.DataContext): great_expectations FileSystem DataContext
    
    """
    gx_data_context_folder_path = f"../{GX_DATA_CONTEXT_FOLDER}"
    context = FileDataContext.create(project_root_dir=gx_data_context_folder_path)
    return context


def create_gx_expectations_suites(
    context: gx.DataContext, expectation_suites_names: str
) -> None:
    """
    Create great_expectations suites for curated and clean data
    
    Args:
        context (gx.DataContext): great_expectations FileSystem DataContext
        expectation_suites_names (str): list of expectation suite names to create
    
    """
    for expectation_suite_name in expectation_suites_names:
        context.add_or_update_expectation_suite(expectation_suite_name)


def create_gx_datasources(
    context: gx.DataContext, expectation_data_sources_names: str
) -> None:
    """
    Create great_expectations datasources for curated and clean data
    
    Args:
        context (gx.DataContext): great_expectations FileSystem DataContext
        expectation_data_sources_names (str): list of data source names to create
    """
    for expectation_data_source_name in expectation_data_sources_names:
        context.sources.add_or_update_pandas(name=expectation_data_source_name)


def _create_gx_batch_request(
    datasource: gx, data_asset_name: str, flat_structure: pd.DataFrame
) -> gx:
    """
    Create a great_expectations batch request for an input data asset
    related to the input datasource
    
    Args:
        datasource (gx): great_expectations datasource
        data_asset_name (str): name of the data asset
        flat_structure (pd.DataFrame): input Pandas dataframe
    """
    data_asset = datasource.add_dataframe_asset(name=data_asset_name)
    batch_request = data_asset.build_batch_request(dataframe=flat_structure)
    return batch_request


def _create_gx_validator(
    context: gx.DataContext, batch_request: gx, expectation_suite_name: str
) -> gx:
    """
    Create a great_expectations validator for an input batch request and
    an input expectation suite
    
    Args:
        context (gx.DataContext): great_expectations FileSystem DataContext
        batch_request (gx): great_expectations batch request
        expectation_suite_name (str): name of the expectation suite
        
    Returns:
        validator (gx): great_expectations validator
    """
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    return validator


def _validate_gx_sales_curated_expectations(
    validator: gx, expectations_storage: SalesExpectationsStorage
) -> None:
    """
    Creates expectations for sales data against the input
    validator
    
    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage
    """
    expectations_storage = SalesExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)
        
    return validator.validate()["success"]


def _validate_gx_products_curated_expectations(
    validator: gx, expectations_storage: ProductsExpectationsStorage
) -> None:
    """
    Creates expectations for products data against the input
    validator
    
    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage
    """
    expectations_storage = ProductsExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    for column in expectations_storage.columns_with_length_equal_to:
        validator.expect_column_value_lengths_to_equal(column, 2)
        
    return validator.validate()["success"]


def _validate_gx_orders_curated_expectations(
    validator: gx, expectations_storage: OrdersExpectationsStorage
) -> None:
    """
    Creates expectations for orders data against the input
    validator
    
    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage
    """
    expectations_storage = OrdersExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)
        
    return validator.validate()["success"]


def _validate_gx_customers_curated_expectations(
    validator: gx, expectations_storage: CustomersExpectationsStorage
) -> None:
    """
    Creates expectations for customers data against the input
    validator
    
    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage
    """
    expectations_storage = CustomersExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    for column in expectations_storage.columns_with_length_equal_to:
        validator.expect_column_value_lengths_to_equal(column, 2)
        
    return validator.validate()["success"]


def _validate_gx_countries_curated_expectations(
    validator: gx, expectations_storage: CountriesExpectationsStorage
) -> None:
    """
    Creates expectations for countries data against the input
    validator
    
    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage
    """
    expectations_storage = CountriesExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)
        
    for column, length in zip(expectations_storage.columns_with_length_equal_to, expectations_storage.lengths_checks):
        validator.expect_column_value_lengths_to_equal(column, length)
        
    return validator.validate()["success"]


def validate_curated_flat_structure(
    flat_structure: pd.DataFrame,
    context: gx.DataContext,
    json_file_name: str,
    expectation_suite_name: str,
    data_source_name: str,
) -> gx:
    """
    Validates input expectation suite expectations against curated version of
    an input JSON file returning full validation results
    
    Args:
        flat_structure (pd.DataFrame): input Pandas dataframe with curated data
        context (gx.DataContext): great_expectations FileSystem DataContext
        json_file_name (str): input JSON file name
        expectation_suite_name (str): input expectation suite name
        data_source_name (str): input data source name

    Returns:
        gx: great_expectations validator results

    """

    batch_request = _create_gx_batch_request(
        context.get_datasource(data_source_name), json_file_name, flat_structure
    )
    
    validator = _create_gx_validator(context, batch_request, expectation_suite_name)

    expectations_storages = {
        "sales": SalesExpectationsStorage,
        "products": ProductsExpectationsStorage,
        "orders": OrdersExpectationsStorage,
        "customers": CustomersExpectationsStorage,
        "countries": CountriesExpectationsStorage,
    }

    validator_functions = {
        "sales": _validate_gx_sales_curated_expectations,
        "products": _validate_gx_products_curated_expectations,
        "orders": _validate_gx_orders_curated_expectations,
        "customers": _validate_gx_customers_curated_expectations,
        "countries": _validate_gx_countries_curated_expectations,
    }

    if (
        json_file_name in expectations_storages
        and json_file_name in validator_functions
    ):
        validation_results = validator_functions[json_file_name](
            validator, expectations_storages[json_file_name]
        )
        
    return validation_results
