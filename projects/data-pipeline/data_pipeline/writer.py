"""Writer module to write data to physical files"""

import logging
from datetime import datetime

import polars as pl

from data_pipeline.params import DATA_ROOT_FOLDER

logger = logging.getLogger(__name__)


def write_data_to_file(
    flat_structure: pl.DataFrame,
    folder_name: str,
    file_name: str,
    file_type: str,
    write_method: str,
) -> None:
    """
    Writes a polars dataframe to a physical file, with the file type
    being either csv or parquet

    Args:
        flat_structure (pd.DataFrame): pandas dataframe to be written to disk
        folder_name (str): name of the folder where the file will be written
        file_name (str): name of the file that will be written
        file_type (str): type of file that will be written, either csv or parquet
        write_method (str): method that will be used to write the data to a file
    """
    output_path = f"{DATA_ROOT_FOLDER}/{folder_name}/{str(datetime.now())}_{file_name}.{file_type}"
    logger.info("File writing complete.")
    return getattr(flat_structure, write_method)(output_path)
