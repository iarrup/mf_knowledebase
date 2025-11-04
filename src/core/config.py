from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Loads environment variables from .env file."""
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    DATABASE_URL: str
    OPENAI_API_KEY: str
    GOOGLE_API_KEY: str
    LOG_LEVEL: str = "INFO"

# Create a single instance to be imported by other modules
settings = Settings()