from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration loaded from environment."""

    upload_dir: Path = Path("uploads")
    db_path: Path = Path("analytics.duckdb")
    redis_url: str = "redis://redis:6379/0"
    api_key: str = "changeme"

    class Config:
        env_file = ".env"


settings = Settings()
UPLOAD_DIR = settings.upload_dir
DB_PATH = settings.db_path
REDIS_URL = settings.redis_url
API_KEY = settings.api_key
