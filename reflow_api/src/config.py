from typing import List
import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    allow_origins: List[str] = os.environ.get("ORIGINS_CORS", ["*"])
    allow_methods: List[str] = os.environ.get("METHODS_CORS", ["*"])
    allow_headers: List[str] = os.environ.get("HEADERS_CORS", ["*"])
    allow_hosts: List[str] = os.environ.get("HOSTS_CORS", ["*"])
    prefix: str = os.getenv("PREFIX", "/api")
    db_user: str = os.getenv("DB_USER", "reflow")
    db_password: str = os.getenv("DB_PASSWORD", "reflow")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5432")
    db_name: str = os.getenv("DB_NAME", "reflow")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
