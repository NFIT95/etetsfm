"""Reader module to read data to physical files"""

import os

import polars as pl

from data_pipeline.params import DATA_ROOT_FOLDER


def _create_path_for_reader(folder_name: str, file_name: str, file_type: str) -> str:
    """
    Create a path for a file to be read

    Args:
        folder_name (str): name of the folder where the file will be written
        file_name (str): name of the file that will be written
        file_type (str): type of file that will be written, either csv or parquet

    Returns:
        path (str): path of the file to be read
    """
    dir_path = f"{DATA_ROOT_FOLDER}/{folder_name}"
    input_files_path = os.listdir(dir_path)
    file_end = f"{file_name}.{file_type}"

    return dir_path, input_files_path, file_end


def read_data_from_file(
    folder_name: str, file_name: str, file_type: str, read_method: str
) -> pl.DataFrame:
    """
    Read data from the latest input file of the input type in a polars Dataframe.
    The latest file is determined according to a prefix timestamp.

    Args:
        folder_name (str): name of the folder where the file will be written
        file_name (str): name of the file that will be written
        file_type (str): type of file that will be written, either csv or parquet
        read_method (str): method that will be used to read the data from a file

    Returns:
        flat_structure (pl.DataFrame): polars dataframe with file data
    """
    files_to_sort = []

    dir_path = f"{DATA_ROOT_FOLDER}/{folder_name}"
    input_files_path = os.listdir(dir_path)
    file_end = f"{file_name}.{file_type}"

    # Pick only files that end with file_end
    for file in input_files_path:
        if file.endswith(file_end):
            files_to_sort.append(file)

    # Pick file with the latest timestamp from files_to_sort
    sorted_files = sorted(files_to_sort, reverse=True, key=lambda x: x.split("_")[0])
    input_file_path = f"{dir_path}/{sorted_files[0]}"
    flat_structure = getattr(pl, read_method)(input_file_path)

    return flat_structure
