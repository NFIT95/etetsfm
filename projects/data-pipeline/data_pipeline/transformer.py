"""Transformer module to create consumable analytics base table"""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def _create_column_prefix(json_file_name: str) -> str:
    """
    Create column prefix based on json file name for column renaming

    Args:
        json_file_name (str): json file name

    Returns:
        column_prefix (str): column prefix
    """
    column_prefix = json_file_name.capitalize()[:-1]
    match json_file_name:
        case "countries":
            column_prefix = "country".capitalize()

    return column_prefix


def _create_columns_renaming(
    column_prefix: str, curated_flat_structure: pl.DataFrame, columns_renaming: dict
) -> dict:
    """
    Create a mapping between old columns names and new columns names

    Args:
        column_prefix (str): column prefix
        curated_flat_structure (pl.DataFrame): curated flat structure
        columns_renaming (dict): empty mapping between old columns names and new columns names

    Returns:
        columns_renaming (dict): updated mapping between old columns names and
        new columns names
    """
    for column in curated_flat_structure.columns:
        columns_renaming[column] = column_prefix + column

    return columns_renaming


def _join_curated_flat_structures(
    curated_flat_structures: dict,
) -> pl.DataFrame:
    """
    Join all curated flat structures into a single flat structure

    Args:
        curated_flat_structures (dict): curated flat structures

    Returns:
        joined_flat_structure (pl.DataFrame): joined flat structure
    """
    joined_flat_structure = (
        curated_flat_structures["sales"]
        .join(
            other=curated_flat_structures["products"],
            left_on="SaleProductId",
            right_on="ProductProductId",
            how="left",
        )
        .join(
            other=curated_flat_structures["orders"],
            left_on="SaleOrderId",
            right_on="OrderOrderId",
            how="left",
        )
        .join(
            other=curated_flat_structures["customers"],
            left_on="OrderCustomerId",
            right_on="CustomerCustomerId",
            how="left",
        )
        .join(
            other=curated_flat_structures["countries"],
            left_on="CustomerCountry",
            right_on="CountryCountry",
            how="left",
        )
    )

    return joined_flat_structure


def _create_total_quantities_per_country_and_currency(
    filtered_flat_structure: pl.DataFrame,
) -> pl.DataFrame:
    """
    Create total quantities per country and currency

    Args:
        filtered_flat_structure (pl.DataFrame): filtered flat structure

    Returns:
        total_quantity_per_country_and_currency (pl.DataFrame): total quantities per country
        and currency
    """
    total_quantity_per_country_and_currency = filtered_flat_structure.group_by(
        ["CountryName", "CountryCurrency"]
    ).agg(
        pl.col("SaleQuantity")
        .alias("TotalSaleQuantityPerCountry")
        .sum()
        .cast(pl.Decimal(scale=6, precision=None))
    )

    return total_quantity_per_country_and_currency


def _add_feature_country_quantity_over_total_quantity_percentage(
    joined_flat_structure: pl.DataFrame,
    total_quantity_per_country_and_currency: pl.DataFrame,
) -> pl.DataFrame:
    """
    Add feature country quantity over total quantity percentage

    Args:
        joined_flat_structure (pl.DataFrame): joined flat structure without
        country quantity over total quantity percentage feature
        total_quantity_per_country_and_currency (pl.DataFrame): total quantities per country
        and currency

    Returns:
        joined_flat_structure (pl.DataFrame): joined flat structure with
        country quantity over total quantity percentage feature
    """

    total_quantity = joined_flat_structure.select(pl.sum("SaleQuantity")).item()
    total_quantity_per_country = total_quantity_per_country_and_currency.select(
        "CountryName", "TotalSaleQuantityPerCountry"
    )

    joined_flat_structure = joined_flat_structure.join(
        other=total_quantity_per_country,
        left_on="CountryName",
        right_on="CountryName",
        how="left",
    ).with_columns(
        (pl.col("TotalSaleQuantityPerCountry") / pl.lit(total_quantity))
        .cast(pl.Decimal(scale=6, precision=None))
        .alias("CountryQuantityOverTotalQuantityPercentage")
    )

    return joined_flat_structure


def _add_feature_quantity_over_total_country_quantity_percentage(
    joined_flat_structure: pl.DataFrame,
    total_quantity_per_country_and_currency: pl.DataFrame,
) -> pl.DataFrame:
    """
    Add feature quantity over total quantity percentage to joined_flat_structure

    Args:
        joined_flat_structure (pl.DataFrame): joined flat structure without
        quantity over total quantity percentage feature
        total_quantity_per_country_and_currency (pl.DataFrame): total quantities per country
        and currency

    Returns:
        joined_flat_structure (pl.DataFrame): joined flat structure with
        quantity over total quantity percentage feature
    """

    joined_flat_structure = joined_flat_structure.join(
        other=total_quantity_per_country_and_currency,
        left_on="CountryName",
        right_on="CountryName",
        how="left",
    ).with_columns(
        (pl.col("SaleQuantity") / pl.col("TotalSaleQuantityPerCountry"))
        .cast(pl.Decimal(scale=6, precision=None))
        .alias("QuantityOverTotalCountryQuantityPercentage")
    )

    return joined_flat_structure


def _add_feature_quantity_over_main_countries_quantity_percentage(
    joined_flat_structure: pl.DataFrame,
    total_quantity_per_country_and_currency: pl.DataFrame,
    currencies_to_select: list[str],
):
    """
    Add feature quantity over main countries quantity percentage to joined_flat_structure

    Args:
        joined_flat_structure (pl.DataFrame): joined flat structure without
        quantity over main countries quantity percentage feature
        total_quantity_per_country_and_currency (pl.DataFrame): total quantities per country
        and currency
        currencies_to_select (list[str]): currencies to filter for

    Returns:
        joined_flat_structure (pl.DataFrame): joined flat structure with
        quantity over main countries quantity percentage feature
    """

    total_main_countries_quantity = (
        total_quantity_per_country_and_currency.filter(
            pl.col("CountryCurrency").is_in(currencies_to_select)
        )
    ).select(pl.sum("TotalSaleQuantityPerCountry").alias("TotalSaleQuantity"))

    joined_flat_structure = joined_flat_structure.join(
        other=total_main_countries_quantity,
        how="cross",
    ).with_columns(
        (pl.col("SaleQuantity") / pl.col("TotalSaleQuantity"))
        .cast(pl.Decimal(scale=6, precision=None))
        .alias("QuantityOverMainCountriesQuantityPercentage")
    )

    return joined_flat_structure


def _add_feature_product_weight_grams_per_sale_quantity(
    joined_flat_structure: pl.DataFrame,
) -> pl.DataFrame:
    """
    Add feature product weight grams per sale quantity to joined_flat_structure

    Args:
        joined_flat_structure (pl.DataFrame): joined flat structure without
        product weight grams per sale quantity feature

    Returns:
        joined_flat_structure (pl.DataFrame): joined flat structure with
        product weight grams per sale quantity feature
    """
    joined_flat_structure = joined_flat_structure.with_columns(
        (pl.col("ProductWeightGrams") / pl.col("SaleQuantity"))
        .cast(pl.Decimal(scale=6, precision=None))
        .alias("ProductWeightGramsPerSaleQuantity")
    )

    return joined_flat_structure


def create_consumable_flat_structure(
    curated_flat_structures: dict,
    consumable_columns_to_select: list[str],
    currencies_to_select: list[str],
) -> pl.DataFrame:
    """
    Create consumable flat structure with all features

    Args:
        curated_flat_structures (dict): curated flat structures
        attributes_to_select (list[str]): attributes to select from joined flat structure
        currencies_to_select (list[str]): currencies to filter for

    Returns
        consumable_flat_structure (pl.DataFrame): consumable flat structure
    """
    columns_renaming = {}

    # Rename columns
    for json_file_name, curated_flat_structure in curated_flat_structures.items():
        column_prefix = _create_column_prefix(json_file_name=json_file_name)
        columns_renaming = _create_columns_renaming(
            column_prefix=column_prefix,
            curated_flat_structure=curated_flat_structure,
            columns_renaming=columns_renaming,
        )
        curated_flat_structures[json_file_name] = curated_flat_structure.rename(
            columns_renaming
        )
        columns_renaming = {}

    # Special case of renaming column
    curated_flat_structures["sales"] = curated_flat_structures["sales"].rename(
        {"SaleSaleId": "SaleId"}
    )

    # Join
    joined_flat_structure = _join_curated_flat_structures(curated_flat_structures)

    # Add features
    total_quantity_per_country_and_currency = (
        _create_total_quantities_per_country_and_currency(joined_flat_structure)
    )
    add_feature_functions = [
        _add_feature_country_quantity_over_total_quantity_percentage,
        _add_feature_quantity_over_total_country_quantity_percentage,
    ]

    for add_feature_function in add_feature_functions:
        joined_flat_structure = add_feature_function(
            joined_flat_structure, total_quantity_per_country_and_currency
        )

    joined_flat_structure = (
        _add_feature_quantity_over_main_countries_quantity_percentage(
            joined_flat_structure,
            total_quantity_per_country_and_currency,
            currencies_to_select,
        )
    )

    joined_flat_structure = _add_feature_product_weight_grams_per_sale_quantity(
        joined_flat_structure
    )

    # Select attributes
    consumable_flat_structure = joined_flat_structure.select(
        consumable_columns_to_select
    )

    logger.info("Consumable flat structure created.")

    return consumable_flat_structure
