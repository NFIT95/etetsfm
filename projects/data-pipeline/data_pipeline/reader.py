"""Reader module to read data to physical files"""

import logging
import os

import polars as pl

from data_pipeline.params import DATA_ROOT_FOLDER

logger = logging.getLogger(__name__)


def _get_files_to_sort(folder_name: str, file_name: str, file_type: str) -> list[str]:
    """
    Return a list of files names from a folder identified with a given file suffix
    and to be sorted according to a timestamp file prefix

    Args:
        folder_name (str): name of the folder from where files will be read
        file_name (str): name of file part of file suffix
        file_type (str): type of file part of file suffix

    Returns:
        files_to_be_sorted (list[str]): list of files to be sorted according to a timestamp
        file prefix
    """
    files_to_be_sorted = []
    input_files_path = os.listdir(f"{DATA_ROOT_FOLDER}/{folder_name}")
    # Creates file end to identify files to be fetched
    file_suffix = f"{file_name}.{file_type}"

    for file in input_files_path:
        if file.endswith(file_suffix):
            files_to_be_sorted.append(file)

    return files_to_be_sorted


def read_data_from_file(
    folder_name: str, file_name: str, file_type: str, read_method: str
) -> pl.DataFrame:
    """
    Read data from the latest input file of the input type in a polars Dataframe.
    The latest file is determined according to a prefix timestamp.

    Args:
        folder_name (str): name of the folder from where files will be read
        file_name (str): name of file part of file suffix
        file_type (str): type of file part of file suffix
        read_method (str): method that will be used to read the data from a file

    Returns:
        flat_structure (pl.DataFrame): polars dataframe with file data
    """

    files_to_sort = _get_files_to_sort(
        folder_name=folder_name, file_name=file_name, file_type=file_type
    )
    # Pick file with the latest timestamp from files_to_sort
    sorted_files = sorted(files_to_sort, reverse=True, key=lambda x: x.split("_")[0])
    input_file_path = f"{DATA_ROOT_FOLDER}/{folder_name}/{sorted_files[0]}"
    flat_structure = getattr(pl, read_method)(input_file_path)
    logger.info("File read completed.")

    return flat_structure
