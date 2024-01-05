"""Settings for the Data Pipeline application"""
from pydantic import BaseSettings

class Settings(BaseSettings):
    """Base settings object"""

    log_level: str = "INFO"
    sample_setting: str = ""

    class Config:
        """Loads the env vars from a .env file"""

        env_file = ".env"
        case_sensitive = False