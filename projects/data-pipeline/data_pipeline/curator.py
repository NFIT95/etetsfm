"""Curator module to apply timestamp and data cleansing"""

import logging
from datetime import datetime

import polars as pl

logger = logging.getLogger(__name__)


def _add_timestamp_column(flat_structure_to_curate: pl.DataFrame) -> pl.DataFrame:
    """
    Add a timestamp column with the current date and time to the input
    Polars dataframe

    Args:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with raw data

    Returns:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with timestamp column
    """
    flat_structure_to_curate = flat_structure_to_curate.with_columns(
        CreatedTimeStamp=datetime.now()
    )
    return flat_structure_to_curate


def _fill_nulls_in_columns(
    flat_structure_to_curate: pl.DataFrame,
) -> pl.DataFrame:
    """
    Fills nulls in columns in the input Polars dataframe. Columns with a non
    relevant or unknown data type are avoided

    Args:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with raw data

    Returns:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with nulls filled
    """

    fill_values = {
        pl.Int64: 0,
        pl.Float64: 0,
        pl.Object: "MISSING",
        pl.Boolean: False,
    }

    for column in flat_structure_to_curate.columns:
        try:
            fill_value = fill_values.get(flat_structure_to_curate[column].dtype)
            flat_structure_to_curate = flat_structure_to_curate.with_columns(
                pl.col(column).fill_null(fill_value)
            )
        # Avoid columns with a not relevant or unknown data type
        except ValueError:
            continue

    return flat_structure_to_curate


def _round_float_columns(flat_structure_to_curate: pl.DataFrame) -> pl.DataFrame:
    """
    Round float columns in the input Polars dataframe to 2 decimal places.

    Args:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with raw data

    Returns:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with rounded float columns
    """
    for column in flat_structure_to_curate.columns:
        if flat_structure_to_curate[column].dtype == pl.Float64:
            flat_structure_to_curate = flat_structure_to_curate.with_columns(
                flat_structure_to_curate[column].round(2)
            )

    return flat_structure_to_curate


def create_curated_flat_structure(
    flat_structure_to_curate: pl.DataFrame,
) -> pl.DataFrame:
    """
    Applies curation functions to the input Polars dataframe

    Args:
        flat_structure_to_curate (pl.DataFrame): Polars dataframe with raw data

    Returns:
        curated_flat_structure (pl.DataFrame): Polars dataframe with curated data
    """

    curating_functions = [
        _add_timestamp_column,
        _fill_nulls_in_columns,
        _round_float_columns,
    ]

    for curating_function in curating_functions:
        flat_structure_to_curate = curating_function(flat_structure_to_curate)

    curated_flat_structure = getattr(flat_structure_to_curate, "unique")()
    logger.info("Curated flat structure created.")

    return curated_flat_structure
