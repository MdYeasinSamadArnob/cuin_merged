"""
CUIN v2 Control Plane - Configuration Settings

Centralized configuration using Pydantic Settings.
Loads from environment variables with .env file support.
"""

import secrets
from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    Environment variables can be set directly or via a .env file.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # ----------------------------------------
    # Application Settings
    # ----------------------------------------
    APP_NAME: str = "CUIN v2 Control Plane"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"
    
    # ----------------------------------------
    # API Server
    # ----------------------------------------
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    # ----------------------------------------
    # Security
    # ----------------------------------------
    SECRET_KEY: str = "change-this-to-a-secure-random-string-in-production"
    CORS_ORIGINS: str = "http://localhost:3000,http://127.0.0.1:3000"

    # Single global bearer token guarding the public Identity Recognition
    # API (/api/v1 -- see api/routes_public_identity_api.py). If
    # PUBLIC_API_BEARER_TOKEN is not set in .env, default_factory
    # auto-generates a random one at process startup -- good enough to
    # try the API immediately, but it changes on every restart until you
    # pin a real value in .env (the Getting Started panel on the /api
    # page always shows the CURRENT effective token either way).
    PUBLIC_API_BEARER_TOKEN: str = Field(default_factory=lambda: f"cuin_{secrets.token_urlsafe(32)}")
    PUBLIC_API_RATE_LIMIT_PER_MIN: int = 120

    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS origins from comma-separated string."""
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]
    
    # ----------------------------------------
    # Database
    # ----------------------------------------
    DATABASE_URL: str = "postgresql://cuin:cuin_secret@localhost:5432/cuin_db"
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10

    # Whether the pipeline orchestrator persists results (customers,
    # candidate pairs, scores, decisions, clusters) to Postgres. Off by
    # default so a fresh checkout without a reachable Postgres still runs
    # the pipeline against in-memory/file artifacts only.
    PERSIST_TO_POSTGRES: bool = True

    # ----------------------------------------
    # Doris (the pipeline's execution engine -- see pipeline.doris_orchestrator)
    # ----------------------------------------
    DORIS_HOST: str = "127.0.0.1"
    DORIS_MYSQL_PORT: int = 9130
    DORIS_HTTP_PORT: int = 8130
    DORIS_USER: str = "root"
    DORIS_PASSWORD: str = ""

    # ----------------------------------------
    # Lakehouse storage (engine.lake) -- local filesystem path now, an
    # S3/HDFS URI for multi-BE deployments later. Currently vestigial:
    # the pipeline reads its source dataset from the hardcoded
    # PARQUET_PATH constant (data_source/oracle_data.parquet), not
    # through this setting -- nothing in the live orchestrator actually
    # reads or writes through LAKE_ROOT today. Only api/routes_admin.py's
    # reset endpoint references it (clearing a directory nothing
    # populates). Kept as a forward-looking placeholder for when bulk
    # data genuinely needs a configurable root.
    # ----------------------------------------
    LAKE_ROOT: str = "./data/lake"

    # ----------------------------------------
    # Referee Agent (LLM)
    # ----------------------------------------
    LLM_ENABLED: bool = False
    LLM_MODEL_PATH: str = ""
    LLM_MAX_TOKENS: int = 512
    LLM_TEMPERATURE: float = 0.3
    
    # ----------------------------------------
    # Blocking Configuration
    # ----------------------------------------
    BLOCKING_MAX_BLOCK_SIZE: int = 200
    BLOCKING_MAX_KEYS_PER_RECORD: int = 50
    BLOCKING_SUPPRESS_FREQUENCY_PCT: float = 5.0
    
    # ----------------------------------------
    # Scoring Thresholds
    # ----------------------------------------
    THRESHOLD_AUTO_LINK: float = 0.92
    THRESHOLD_REVIEW_MIN: float = 0.55
    THRESHOLD_REFEREE_MIN: float = 0.45
    THRESHOLD_REFEREE_MAX: float = 0.70
    
    # ----------------------------------------
    # File Paths
    # ----------------------------------------
    SAMPLE_CSV_PATH: str = "/home/arnob/Downloads/challenging_er_200.csv"
    POLICY_DIR: str = "./policies"
    
    # ----------------------------------------
    # Run Configuration
    # ----------------------------------------
    MAX_CANDIDATES_PER_RUN: int = 1000000
    FULL_MODE_RECORD_THRESHOLD: int = 100000


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Using lru_cache ensures settings are only loaded once.
    """
    return Settings()


# Global settings instance
settings = get_settings()
