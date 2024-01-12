"""Transformer module to create final master table"""

import polars as pl


def _rename_curated_flat_structures_columns(curated_flat_structures: dict) -> dict:
    """
    Rename columns in curated flat structures
    
    Args:
        curated_flat_structures (dict): curated flat structures with non renamed columns
        
    Returns:
        curated_flat_structures (dict): curated flat structures with renamed columns
    """
    renaming_dictionary = {}

    for json_file_name, curated_flat_structure in curated_flat_structures.items():
        # Create new column name
        column_prefix = json_file_name.capitalize()[:-1]
        match json_file_name:
            case "countries":
                column_prefix = "country".capitalize()
        # Create new dict with old and new column name per each dataframe
        for column in curated_flat_structure.columns:
            renaming_dictionary[column] = column_prefix + column

        # Rename columns
        curated_flat_structures[json_file_name] = curated_flat_structure.rename(
            renaming_dictionary
        )
        renaming_dictionary = {}

    # Special case of renaming column
    curated_flat_structures["sales"] = curated_flat_structures["sales"].rename(
        {"SaleSaleId": "SaleId"}
    )

    return curated_flat_structures


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


def _add_feature_country_percentage_of_total_quantity(
    joined_flat_structure: pl.DataFrame,
) -> pl.DataFrame:
    """
    Add feature country percentage of total quantity to joined_flat_structure
    
    Args:
        joined_flat_structure (pl.DataFrame): joined flat structure without
        country percentage of total quantity feature
    
    Returns:
        joined_flat_structure (pl.DataFrame): joined flat structure with 
        country percentage of total quantity feature
    """
    grouping_column, numerator, denominator = "CountryName", "SaleQuantity", "TotalSaleQuantityPerCountry"

    joined_flat_structure = (
        joined_flat_structure.group_by(grouping_column)
        .agg(pl.col(numerator).alias(denominator).sum())
        .join(
            other=joined_flat_structure,
            left_on=grouping_column,
            right_on=grouping_column,
            how="left",
        )
        .with_columns(
            (pl.col(numerator) / pl.col(denominator))
            .cast(pl.Float64)
            .alias("CountryPercentageOfTotalQuantity")
        )
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
        .cast(pl.Float64)
        .alias("ProductWeightGramsPerSaleQuantity")
    )

    return joined_flat_structure


def create_consumable_flat_structure(
    curated_flat_structures: dict, attributes_to_select: list[str]
) -> pl.DataFrame:
    """
    Create consumable flat structure with all features
    
    Args:
        curated_flat_structures (dict): curated flat structures
        attributes_to_select (list[str]): attributes to select from joined flat structure
        
    Returns
        consumable_flat_structure (pl.DataFrame): consumable flat structure
    """
    renamed_curated_flat_structures = _rename_curated_flat_structures_columns(
        curated_flat_structures
    )

    joined_flat_structure = _join_curated_flat_structures(
        renamed_curated_flat_structures
    )

    add_feature_functions = [
        _add_feature_country_percentage_of_total_quantity,
        _add_feature_product_weight_grams_per_sale_quantity,
    ]

    for add_feature_function in add_feature_functions:
        joined_flat_structure = add_feature_function(joined_flat_structure)

    consumable_flat_structure = joined_flat_structure.select(
        attributes_to_select
    )

    return consumable_flat_structure
