from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    username: str = "postgres"
    password: str = "postgres"
    db: str = "testdb"


class XNATSettings(BaseModel):
    url: str = "http://xnat-web:8080"
    username: str = "admin"
    password: str = "admin"


class ImagingSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="IMAGING_",
        env_nested_delimiter="__",
    )

    postgres: PostgresSettings = PostgresSettings()
    xnat: XNATSettings = XNATSettings()


def load_yaml_config(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f) or {}
