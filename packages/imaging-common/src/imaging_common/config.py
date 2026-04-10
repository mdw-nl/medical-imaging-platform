"""Pydantic settings models and YAML loading helpers for imaging pipeline services."""

from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    """Connection parameters for a PostgreSQL database."""

    host: str = "localhost"
    port: int = 5432
    username: str = "postgres"
    password: str
    db: str = "testdb"
    sslmode: str = "prefer"

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("postgres password must be set — do not deploy with an empty password")
        return v


class XNATSettings(BaseModel):
    """Connection parameters for an XNAT server."""

    url: str = "http://xnat-web:8080"
    username: str
    password: str


class SCPSettings(BaseModel):
    """DICOM SCP (Storage Class Provider) configuration."""

    ae_titles: list[str] = ["MY_SCP"]


class AnonymizationSettings(BaseModel):
    """Parameters controlling DICOM de-identification and UID remapping."""

    patient_name: str = "ANONYMOUS"
    profile_name: str = ""
    project_name: str = ""
    trial_name: str = ""
    site_name: str = ""
    site_id: str = ""
    uid_secret: str
    uid_prefix: str = "99999."

    @field_validator("uid_secret")
    @classmethod
    def uid_secret_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("uid_secret must be set — do not deploy with an empty secret")
        return v


class ImagingSettings(BaseSettings):
    """Top-level settings aggregating all subsystem configurations.

    Values are loaded from a YAML file and can be overridden by environment
    variables prefixed with ``IMAGING_`` (nested via double underscores).
    """

    model_config = SettingsConfigDict(
        env_prefix="IMAGING_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    postgres: PostgresSettings
    xnat: XNATSettings | None = None
    scp: SCPSettings | None = None
    anonymization: AnonymizationSettings | None = None


def load_yaml_config(path: Path) -> dict:
    """Parse a YAML file and return its contents as a dictionary."""
    with path.open() as f:
        return yaml.safe_load(f) or {}


def load_settings(config_path: Path) -> ImagingSettings:
    """Load and validate pipeline settings from a YAML configuration file."""
    yaml_data = load_yaml_config(config_path)
    return ImagingSettings(**yaml_data)
