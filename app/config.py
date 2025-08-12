# app/config.py
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Application configuration loaded from environment."""

    # Use explicit env var names so your .env / compose work with UPPERCASE.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,   # allow UPLOAD_DIR or upload_dir
        extra="ignore",
    )

    upload_dir: Path = Field(default=Path("/uploads"), validation_alias="UPLOAD_DIR")
    db_path: Path    = Field(default=Path("/data/analytics.duckdb"), validation_alias="DB_PATH")
    redis_url: str   = Field(default="redis://redis:6379/0", validation_alias="REDIS_URL")
    api_key: str     = Field(default="changeme", validation_alias="API_KEY")

settings = Settings()
UPLOAD_DIR = settings.upload_dir
DB_PATH    = settings.db_path
REDIS_URL  = settings.redis_url
API_KEY    = settings.api_key

