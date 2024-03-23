from pydantic_settings import BaseSettings


class BaseSettings(BaseSettings):
    dag_path: str = "/home/murillossj/Documentos/airflow/reflow/dags"


def get_settings():
    return BaseSettings()
