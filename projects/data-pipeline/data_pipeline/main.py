"""Main entry point for data pipeline"""
from data_pipeline import settings
from data_pipeline.checker import (
    check_json_lines,
    create_gx_filesystem_context,
    create_gx_expectations_suites,
    create_gx_datasources,
    validate_curated_flat_structure,
)
from data_pipeline.extractor import extract_json_lines_from_json_file
from data_pipeline.params import json_files_validators
from data_pipeline.curator import create_curated_flat_structure
from data_pipeline.writer import write_data_to_file


def main():
    """Main entry point"""
    # Initiate gx objects
    context = create_gx_filesystem_context()
    create_gx_expectations_suites(context, settings.expectation_suites_names)
    create_gx_datasources(context, settings.data_source_names)

    # ETL logic
    for json_file_name in settings.json_files_names:
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
            expectation_suite_name=settings.expectation_suites_names[0],
            data_source_name=settings.data_source_names[0],
        )
        write_data_to_file(
            flat_structure=curated_flat_structure,
            folder_name="curated_data",
            file_name=json_file_name,
            file_type="parquet",
            write_method="write_parquet",
        )

    print(f"FANTASTIC JOB {settings.sample_setting}")


if __name__ == "__main__":
    main()
