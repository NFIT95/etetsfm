"""Settings for the Data Pipeline application - not yet in use"""
from pydantic_settings import BaseSettings


# pylint: disable=too-few-public-methods
class Settings(BaseSettings):
    """Base settings object"""

    sample_setting: str = "Nicola Filosi"
    json_files_names: list[str] = [
        "sales",
        "customers",
        "products",
        "orders",
        "countries",
    ]
    data_source_names: list[str] = ["curated", "clean"]

    class Config:
        """Loads the env vars from a .env file"""

        env_file = "../.env"
        case_sensitive = False
