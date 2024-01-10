"""Main entry point for data pipeline"""
from data_pipeline import settings
from data_pipeline.checker import (
    create_gx_datasources,
    create_gx_expectations_suites,
    create_gx_filesystem_context,
)
from data_pipeline.curator import materialize_curated_flat_structures


def main():
    """Main entry point"""

    context = create_gx_filesystem_context()
    create_gx_expectations_suites(context, settings.expectation_suites_names)
    create_gx_datasources(context, settings.data_source_names)
    materialize_curated_flat_structures(
        context,
        settings.files_names,
        settings.expectation_suites_names[0],
        settings.data_source_names[0],
    )

    print(f"FANTASTIC JOB {settings.sample_setting}")


if __name__ == "__main__":
    main()
