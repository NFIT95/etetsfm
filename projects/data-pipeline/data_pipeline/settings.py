"""Settings for the Data Pipeline application"""
from pydantic_settings import BaseSettings


# pylint: disable=too-few-public-methods
class Settings(BaseSettings):
    """Base settings object"""

    log_level: str = "INFO"
    sample_setting: str = "Nicola Filosi"

    class Config:
        """Loads the env vars from a .env file"""

        env_file = "../.env"
        case_sensitive = False