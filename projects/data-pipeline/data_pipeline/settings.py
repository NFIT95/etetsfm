"""Settings for the Data Pipeline application"""
from pydantic-settings import BaseSettings

class Settings(BaseSettings):
    """Base settings object"""

    log_level: str = "INFO"
    sample_setting: str = "Nicola Filosi"

    class Config:
        """Loads the env vars from a .env file"""

        env_file = "../.env"
        case_sensitive = False