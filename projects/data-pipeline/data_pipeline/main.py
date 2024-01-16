"""Main entry point for data pipeline"""
import logging
import logging.config

import yaml

from data_pipeline.checker import (
    check_json_lines,
    create_gx_datasources,
    create_gx_expectations_suites,
    create_gx_filesystem_context,
    validate_consumable_flat_structure,
    validate_curated_flat_structure,
)
from data_pipeline.curator import create_curated_flat_structure
from data_pipeline.extractor import extract_json_lines_from_json_file
from data_pipeline.params import (
    consumable_columns_to_select,
    curated_flat_structures,
    currencies_to_select,
    data_source_names,
    expectation_suites_names,
    json_files_names,
    json_files_validators,
)
from data_pipeline.profiler import write_data_profile_report
from data_pipeline.reader import read_data_from_file
from data_pipeline.transformer import create_consumable_flat_structure
from data_pipeline.writer import write_data_to_file

LOGGING_CONF_PATH = "../../tools/conf/logging.yaml"

with open(LOGGING_CONF_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

logging.config.dictConfig(config)
logger = logging.getLogger(__name__)

logger.info("Running data pipeline.")


def main():
    """Main entry point"""

    # Initiate gx objects
    context = create_gx_filesystem_context()
    create_gx_expectations_suites(context, expectation_suites_names)
    create_gx_datasources(context, data_source_names)

    # ETL logic - one file cached in memory at a time
    for json_file_name in json_files_names:
        extracted_json_lines = extract_json_lines_from_json_file(
            json_file_name=json_file_name
        )
        valid_and_broken_json_lines = check_json_lines(
            extracted_json_lines=extracted_json_lines,
            json_file_name=json_file_name,
            json_files_validators=json_files_validators,
        )
        write_data_to_file(
            flat_structure=valid_and_broken_json_lines["broken_json_lines"],
            folder_name="quarantine_data",
            file_name=json_file_name,
            file_type="csv",
            write_method="write_csv",
        )
        curated_flat_structure = create_curated_flat_structure(
            valid_and_broken_json_lines["valid_json_lines"]
        )
        validate_curated_flat_structure(
            flat_structure=curated_flat_structure,
            context=context,
            json_file_name=json_file_name,
            expectation_suite_name=expectation_suites_names["curated"],
            data_source_name=data_source_names["curated"],
        )
        write_data_to_file(
            flat_structure=curated_flat_structure,
            folder_name="curated_data",
            file_name=json_file_name,
            file_type="parquet",
            write_method="write_parquet",
        )
        write_data_profile_report(
            flat_structure=curated_flat_structure, file_name=json_file_name
        )

    for json_file_name in json_files_names:
        curated_flat_structures[json_file_name] = read_data_from_file(
            folder_name="curated_data",
            file_name=json_file_name,
            file_type="parquet",
            read_method="read_parquet",
        )

    consumable_flat_structure = create_consumable_flat_structure(
        curated_flat_structures, consumable_columns_to_select, currencies_to_select
    )
    validate_consumable_flat_structure(
        flat_structure=consumable_flat_structure,
        context=context,
        file_name="analytics_base_table",
        expectation_suite_name=expectation_suites_names["consumable"],
        data_source_name=data_source_names["consumable"],
    )
    write_data_to_file(
        flat_structure=consumable_flat_structure,
        folder_name="consumable_data",
        file_name="analytics_base_table",
        file_type="parquet",
        write_method="write_parquet",
    )
    write_data_profile_report(
        flat_structure=consumable_flat_structure, file_name="analytics_base_table"
    )


if __name__ == "__main__":
    main()
