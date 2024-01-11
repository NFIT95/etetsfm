"""Extractor module to extract data from raw json files"""

import json
import re

from data_pipeline.params import JsonLinesStorage


def _remove_final_comma(json_line: dict) -> dict:
    """
    Removes final comma from JSON file line if that comma exists
    otherwise return the JSON file line as is

    Args:
        json_line (dict): input JSON file line with final comma

    Returns:
        json_line (dict): input JSON file line without final comma
    """
    if json_line[-2] == ",":
        json_line = json.loads(json_line[:-2])
    else:
        json_line = json.loads(json_line)

    return json_line


def _rename_keys(json_line: dict) -> dict:
    """
    Removes undesired characters from schema attributes in an input JSON
    file line during extraction to allow for Pydantic validation

    Args:
        json_line (dict): input JSON file line with blank spaces in any attribute

    Returns:
        renamed_json_line (dict): input JSON file line without blank spaces in any attribute
    """
    undesired_characters = [" ", ".", "(", ")", "$", "%"]
    renamed_json_line = {}

    for key, value in json_line.items():
        for character in key:
            if character in undesired_characters:
                # Replace all undesired characters with _ to allow for regex
                key = key.replace(character, "_")
        # Apply CamelCase naming and remove all _
        new_key = re.sub(
            r"(?<=_)([^_])", lambda match: match.group(1).upper(), key
        ).replace("_", "")
        renamed_json_line[new_key] = value

    return renamed_json_line


def extract_json_lines_from_json_file(json_file_name: str) -> list[dict]:
    """
    Extract data from a JSON file with one JSON object per line

    Args:
        json_file_name (str): input JSON file name

    Returns:
        extracted_json_lines (list[dict]): list of input JSON file clean lines
    """
    json_lines_storage = JsonLinesStorage()

    cleaning_functions = [
        _remove_final_comma,
        _rename_keys,
    ]

    with open(
        "data" + "/" + "raw_data" + "/" + json_file_name + ".json", encoding="utf-8"
    ) as json_file:
        for json_line in json_file:
            for cleaning_function in cleaning_functions:
                json_line = cleaning_function(json_line)
            json_lines_storage.extracted_json_lines.append(json_line)

    return json_lines_storage.extracted_json_lines
