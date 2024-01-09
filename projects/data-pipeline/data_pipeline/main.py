"""Main entry point for data pipeline"""
from data_pipeline import settings
from data_pipeline.curator import create_table_for_data_curation


def main():
    """Main entry point"""
    
    create_table_for_data_curation("sales")

    print(f"FANTASTIC JOB {settings.sample_setting}")


if __name__ == "__main__":
    main()
