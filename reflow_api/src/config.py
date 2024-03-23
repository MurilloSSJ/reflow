from typing import List, Optional
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
    db_host: str = os.getenv("DB_HOST", "reflow-postgres")
    db_port: str = os.getenv("DB_PORT", "5432")
    db_name: str = os.getenv("DB_NAME", "reflow")
    keycloak_client_credentials: Optional[str] = os.getenv(
        "KEYCLOAK_CLIENT_CREDENTIALS", "secret"
    )
    max_active_runs_per_dag: int = os.getenv("MAX_ACTIVE_RUNS_PER_DAG", 16)
    max_active_tasks_per_dag: int = os.getenv("MAX_ACTIVE_TASKS_PER_DAG", 16)
    max_dagruns_to_create_per_loop: int = os.getenv(
        "MAX_DAGRUNS_TO_CREATE_PER_LOOP", 10
    )
    dag_default_view: str = os.getenv("DAG_DEFAULT_VIEW", "grid")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
