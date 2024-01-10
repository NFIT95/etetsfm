"""Writer module to write data to physical files"""

from datetime import datetime

import pandas as pd

STORAGE_ROOT_FOLDER = "data"


def write_data_to_file(
    flat_structure: pd.DataFrame, folder_name: str, file_name: str, file_type: str
) -> None:
    """
    Writes a pandas dataframe to a physical file, with the file type
    being either csv or parquet

    Args:
        flat_structure (pd.DataFrame): pandas dataframe to be written to disk
        folder_name (str): name of the folder where the file will be written
        file_name (str): name of the file that will be written
        file_type (str): type of file that will be written, either csv or parquet
    """
    output_path = f"{STORAGE_ROOT_FOLDER}/{folder_name}/{str(datetime.now())}_{file_name}.{file_type}"
    match file_type:
        case "csv":
            flat_structure.to_csv(output_path)
        case "parquet":
            flat_structure.to_parquet(output_path)
