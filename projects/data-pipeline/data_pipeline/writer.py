"""Writer module to write data to physical files"""

from datetime import datetime

import polars as pl


def write_data_to_file(
    flat_structure: pl.DataFrame, folder_name: str, file_name: str, file_type: str
) -> None:
    """PyDocs"""
    output_path = f"data/{folder_name}/{str(datetime.now())}_{file_name}.{file_type}"
    match file_type:
        case "csv":
            flat_structure.write_csv(output_path)
        case "parquet":
            flat_structure.write_parquet(output_path)
