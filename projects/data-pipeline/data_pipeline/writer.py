"""Writer module to write data to physical files"""

import polars as pl
from datetime import datetime

def write_data_to_parquet_file(flat_structure: pl.DataFrame, folder_name: str, parquet_file_name: str) -> None:
    """PyDocs"""
    timestamp = datetime.now()
    output_path = "data" + "/" + folder_name + "/" + str(timestamp) + parquet_file_name + ".parquet"
    flat_structure.write_parquet(output_path)