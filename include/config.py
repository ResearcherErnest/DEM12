"""
include/config.py
=================
Single source of truth for all platform configuration.

Usage (anywhere in the project):
    from include.config import settings
    print(settings.postgres_host)

Environment variables are loaded from .env at startup.
Any missing required field raises a clear ValidationError
listing every problem before the process exits — fail-fast,
never silent.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated platform settings loaded from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",          # ignore unknown env vars
    )

    # === PostgreSQL ============================================================
    postgres_host: str = Field(..., description="PostgreSQL hostname")
    postgres_port: int = Field(5432, ge=1, le=65535)
    postgres_db: str = Field(..., description="Sales database name")
    postgres_user: str = Field(..., description="PostgreSQL user")
    postgres_password: str = Field(..., min_length=8, description="PostgreSQL password")

    # === Airflow =============================================================
    airflow_admin_username: str = Field("admin")
    airflow_admin_password: str = Field(..., min_length=8)
    airflow_admin_email: str = Field(...)

    # === MinIO ===============================================================
    minio_root_user: str = Field(..., min_length=3)
    minio_root_password: str = Field(..., min_length=8)
    minio_endpoint: str = Field(..., description="e.g. http://minio:9000")
    minio_raw_bucket: str = Field("raw-data")
    minio_processed_bucket: str = Field("processed-data")
    minio_invalid_bucket: str = Field("invalid-data")

    # === Data Generator ======================================================
    generator_num_customers:    int = Field(1000, ge=10, le=1_000_000)
    generator_num_transactions: int = Field(10000, ge=50, le=10_000_000)
    generator_min_rows: int = Field(200, ge=1, le=1_000_000)
    generator_max_rows: int = Field(1500, ge=1, le=1_000_000)
    generator_seed: int = Field(42)

    # === Derived helpers =====================================================
    @property
    def postgres_dsn(self) -> str:
        """Standard psycopg2 DSN string."""
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    @property
    def postgres_url(self) -> str:
        """SQLAlchemy URL."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # === Validators ==========================================================
    @field_validator("minio_endpoint")
    @classmethod
    def validate_minio_endpoint(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("MINIO_ENDPOINT must start with http:// or https://")
        return v.rstrip("/")

    @field_validator("airflow_admin_email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError("AIRFLOW_ADMIN_EMAIL must be a valid email address")
        return v

    @model_validator(mode="after")
    def warn_default_passwords(self) -> "Settings":
        """Warn loudly if placeholder passwords are still in use."""
        defaults = {"change_me_postgres", "change_me_minio", "change_me_admin"}
        bad = [
            name
            for name, val in {
                "POSTGRES_PASSWORD": self.postgres_password,
                "MINIO_ROOT_PASSWORD": self.minio_root_password,
                "AIRFLOW_ADMIN_PASSWORD": self.airflow_admin_password,
            }.items()
            if val in defaults
        ]
        if bad:
            import warnings
            warnings.warn(
                f"WARNING: Still using placeholder passwords for: {bad}. "
                "Update your .env before running in production.",
                stacklevel=2,
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the validated Settings singleton.
    Cached so .env is only read once per process.

    Raises pydantic.ValidationError (with all errors listed) if any
    required variable is missing or invalid.
    """
    return Settings()


# Module-level singleton — import this everywhere
settings: Settings = get_settings()
