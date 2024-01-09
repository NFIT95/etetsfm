"""Curator module to apply timestamp and data checks"""

import polars as pl
import pandas as pd

from data_pipeline.extractor import extract_data_from_json_file

def create_flat_structure_for_data_quarantine(json_file_name: str) -> pl.DataFrame:
    """PyDocs"""
    quarantined_table = pl.DataFrame(extract_data_from_json_file(json_file_name)[1])
    
    return quarantined_table


def create_flat_structure_for_data_curation(json_file_name: str) -> pl.DataFrame:
    """PyDocs"""
    table = pl.DataFrame(extract_data_from_json_file(json_file_name)[0])
    
    return table




