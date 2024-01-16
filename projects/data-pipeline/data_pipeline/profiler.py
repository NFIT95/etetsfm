"""Profiler module to generate data profiles of curated and clean data"""

import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from ydata_profiling import ProfileReport

from data_pipeline.params import DATA_ROOT_FOLDER

logger = logging.getLogger(__name__)


def write_data_profile_report(flat_structure: pl.DataFrame, file_name: str) -> None:
    """
    Creates html data profiling report for an input polars dataframe

    Args:
        flat_structure (pl.DataFrame): input polars dataframe
        file_name (str): output file name
    """

    # Convert from polars to pandas dataframe for ydata-profiling integration
    pd_flat_structure = flat_structure.to_pandas()

    profile = ProfileReport(pd_flat_structure, title=f"Profile Report of {file_name}")
    output_path = Path(
        f"{DATA_ROOT_FOLDER}/data_profiles/{str(datetime.now())}_{file_name}.html"
    )
    profile.to_file(output_file=output_path)
    logger.info("Profiling completed.")
