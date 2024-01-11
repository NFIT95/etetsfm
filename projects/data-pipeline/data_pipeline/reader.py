"""Reader module to read data to physical files"""

import os

import polars as pl

from data_pipeline.params import DATA_ROOT_FOLDER


def read_data_from_file(
    folder_name: str, file_name: str, file_type: str
) -> pl.DataFrame:
    """
    Read data from the latest input file of the input type in a polars Dataframe.
    The latest file is determined according to a prefix timestamp.

    Args:
        folder_name (str): name of the folder where the file will be written
        file_name (str): name of the file that will be written
        file_type (str): type of file that will be written, either csv or parquet

    Returns:
        flat_structure (pl.DataFrame): polars dataframe with file data
    """
    files_to_sort = []

    input_files_path = os.listdir(f"{DATA_ROOT_FOLDER}/{folder_name}")
    file_end = f"{file_name}.{file_type}"

    # Pick only files that end with file_end
    for file in input_files_path:
        if file.endswith(file_end):
            files_to_sort.append(file)

    # Pick file with the latest timestamp from files_to_sort
    sorted_file = sorted(files_to_sort, reverse=True, key=lambda x: x.split("_")[0])
    flat_structure = pl.DataFrame(sorted_file[0])

    return flat_structure
