"""Configuration via environment variables using pydantic-settings."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = {"env_prefix": "MC_", "env_file": ".env", "extra": "ignore"}

    # ── Database ──────────────────────────────────────────────
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "metadata_crawl"
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_pool_min: int = 2
    db_pool_max: int = 10

    # ── NCBI ──────────────────────────────────────────────────
    ncbi_api_key: Optional[str] = None
    ncbi_email: str = "user@example.com"
    ncbi_tool_name: str = "metadata_crawl"
    ncbi_requests_per_second: float = 3.0  # 10 with API key

    # ── FTP ───────────────────────────────────────────────────
    ftp_host: str = "ftp.ncbi.nlm.nih.gov"
    ftp_timeout: int = 60
    ftp_max_retries: int = 5

    # ── Paths ─────────────────────────────────────────────────
    download_dir: Path = Path("./data/downloads")
    cache_dir: Path = Path("./data/cache")
    log_dir: Path = Path("./data/logs")

    # ── Pipeline ──────────────────────────────────────────────
    batch_size: int = 500
    max_concurrent_downloads: int = 4
    resume_enabled: bool = True

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def rate_limit_delay(self) -> float:
        return 1.0 / self.ncbi_requests_per_second


@dataclass
class PipelineState:
    """Mutable runtime state for resume support."""

    last_processed_gse: Optional[str] = None
    last_processed_srp: Optional[str] = None
    total_parsed: int = 0
    total_errors: int = 0
    checkpoint_file: Path = field(default_factory=lambda: Path("./data/checkpoint.json"))


def get_settings() -> Settings:
    """Factory – call once at startup."""
    return Settings()
