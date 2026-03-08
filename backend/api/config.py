"""
CUIN v2 Control Plane - Configuration Settings

Centralized configuration using Pydantic Settings.
Loads from environment variables with .env file support.
"""

from functools import lru_cache
from typing import List

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
    
    # ----------------------------------------
    # Neo4j
    # ----------------------------------------
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "neo4j_secret"
    
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
    # Writable on Vercel (/tmp) and local dev (data/).
    # Override via DATA_DIR env var if needed.
    DATA_DIR: str = "/tmp/cuin_data"

    
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
