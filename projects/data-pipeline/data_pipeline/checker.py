"""Checker module to define data quality checks via in-memory gx"""

import sys

import great_expectations as gx
import pandas as pd
import polars as pl
from great_expectations.data_context import FileDataContext
from pydantic import ValidationError

from data_pipeline.params import (
    CountriesExpectationsStorage,
    CustomersExpectationsStorage,
    JsonLinesStorage,
    OrdersExpectationsStorage,
    ProductsExpectationsStorage,
    SalesExpectationsStorage,
)

GX_DATA_CONTEXT_FOLDER = "tools/gx/data_context"


def check_json_lines(
    extracted_json_lines: list[dict],
    json_file_name: str,
    json_files_validators: dict,
) -> dict:
    """
    Returns a given JSON line schema validator depending on the input JSON file

    Args:
        extracted_json_lines (list[dict]): list of input JSON file lines
        json_file_name (str): input JSON file name
        json_files_validators (dict): pydantic validators for input JSON file lines

    Returns:
        valid_and_broken_json_lines (dict): dict with json lines that passed validation
        and json lines that did not, each is a polars Dataframe
    """
    json_lines_storage = JsonLinesStorage()

    for extracted_json_line in extracted_json_lines:
        try:
            # Store valid JSON line
            json_files_validators[json_file_name](**extracted_json_line)
            json_lines_storage.valid_json_lines.append(extracted_json_line)
        except ValidationError:
            # Store broken JSON line
            print("Incorrect schema in JSON line: ", ValidationError)
            json_lines_storage.broken_json_lines.append(extracted_json_line)

    valid_and_broken_json_lines = {
        "valid_json_lines": pl.DataFrame(json_lines_storage.valid_json_lines),
        "broken_json_lines": pl.DataFrame(json_lines_storage.broken_json_lines),
    }

    return valid_and_broken_json_lines


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

    Returns:
        batch_request (gx): great_expectations batch request
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
) -> gx:
    """
    Creates expectations for sales data against the input validator

    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage

    Returns:
        validator_results (gx): expectations validation results
    """
    expectations_storage = SalesExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    validator_results = validator.validate()["success"]

    return validator_results


def _validate_gx_products_curated_expectations(
    validator: gx, expectations_storage: ProductsExpectationsStorage
) -> gx:
    """
    Creates expectations for products data against the input
    validator

    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage

    Returns:
        validator_results (gx): expectations validation results
    """
    expectations_storage = ProductsExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    for column in expectations_storage.columns_with_length_equal_to:
        validator.expect_column_value_lengths_to_equal(column, 2)

    validation_results = validator.validate()["success"]

    return validation_results


def _validate_gx_orders_curated_expectations(
    validator: gx, expectations_storage: OrdersExpectationsStorage
) -> gx:
    """
    Creates expectations for orders data against the input
    validator

    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage

    Returns:
        validator_results (gx): expectations validation results
    """
    expectations_storage = OrdersExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    validator_results = validator.validate()["success"]

    return validator_results


def _validate_gx_customers_curated_expectations(
    validator: gx, expectations_storage: CustomersExpectationsStorage
) -> gx:
    """
    Creates expectations for customers data against the input
    validator

    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage

    Returns:
        validator_results (gx): expectations validation results
    """
    expectations_storage = CustomersExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    for column in expectations_storage.columns_with_length_equal_to:
        validator.expect_column_value_lengths_to_equal(column, 2)

    validator_results = validator.validate()["success"]

    return validator_results


def _validate_gx_countries_curated_expectations(
    validator: gx, expectations_storage: CountriesExpectationsStorage
) -> gx:
    """
    Creates expectations for countries data against the input
    validator

    Args:
        validator (gx): great_expectations validator
        expectations_storage (SalesExpectationsStorage): expectations storage

    Returns:
        validator_results (gx): expectations validation results
    """
    expectations_storage = CountriesExpectationsStorage()

    for column in expectations_storage.columns_to_exist_and_be_not_null:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column)

    for column in expectations_storage.columns_to_be_unique:
        validator.expect_column_values_to_be_unique(column)

    for column, length in zip(
        expectations_storage.columns_with_length_equal_to,
        expectations_storage.lengths_checks,
    ):
        validator.expect_column_value_lengths_to_equal(column, length)

    validator_results = validator.validate()["success"]

    return validator_results


def validate_curated_flat_structure(
    flat_structure: pd.DataFrame,
    context: gx.DataContext,
    json_file_name: str,
    expectation_suite_name: str,
    data_source_name: str,
) -> None:
    """
    Validates input expectation suite expectations against curated version of
    an input JSON file returning full validation results

    Args:
        flat_structure (pd.DataFrame): input Pandas dataframe with curated data
        context (gx.DataContext): great_expectations FileSystem DataContext
        json_file_name (str): input JSON file name
        expectation_suite_name (str): input expectation suite name
        data_source_name (str): input data source name
    """

    # Convert pl Dataframe to pd Dataframe for gx integration
    flat_structure = flat_structure.to_pandas()

    batch_request = _create_gx_batch_request(
        context.get_datasource(data_source_name), json_file_name, flat_structure
    )

    print(batch_request.dict())

    validator = _create_gx_validator(context, batch_request, expectation_suite_name)

    validation_specs = {
        "sales": {
            "expectations_storage": SalesExpectationsStorage,
            "validator_function": _validate_gx_sales_curated_expectations,
        },
        "products": {
            "expectations_storage": ProductsExpectationsStorage,
            "validator_function": _validate_gx_products_curated_expectations,
        },
        "orders": {
            "expectations_storage": OrdersExpectationsStorage,
            "validator_function": _validate_gx_orders_curated_expectations,
        },
        "customers": {
            "expectations_storage": CustomersExpectationsStorage,
            "validator_function": _validate_gx_customers_curated_expectations,
        },
        "countries": {
            "expectations_storage": CountriesExpectationsStorage,
            "validator_function": _validate_gx_countries_curated_expectations,
        },
    }

    if json_file_name in validation_specs:
        validation_results = validation_specs[json_file_name]["validator_function"](
            validator, validation_specs[json_file_name]["expectations_storage"]
        )

    if not validation_results:
        print(
            f"""Validation unsuccessful. Curated data related to {json_file_name}
            does not match expectations. Stopping execution now."""
        )
        sys.exit()
