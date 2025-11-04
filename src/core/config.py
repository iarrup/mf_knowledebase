from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
class Settings(BaseSettings):
    """Loads environment variables from .env file."""
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    DATABASE_URL: str
    OPENAI_API_KEY: str
    GOOGLE_API_KEY: str
    LOG_LEVEL: str = "INFO"
    
    # Data source configuration
    # Set to "local" or "github"
    DATA_SOURCE_TYPE: str = "local"
    
    # Optional settings for "github" source type
    GITHUB_REPO_URL: Optional[str] = None
    GITHUB_REPO_PATH: Optional[str] = None # e.g., "path/to/cobol/files"
    GITHUB_ACCESS_TOKEN: Optional[str] = None # For private repos

# Create a single instance to be imported by other modules
settings = Settings()