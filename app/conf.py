import os
from pydantic_settings import BaseSettings
import logging
from enum import Enum


class OutputFormats(Enum):
    PARQUET: str = 'parquet'
    CSV: str = 'csv'
    JSONL: str = 'jsonl'


class Settings(BaseSettings):
    # Connection settings
    API_KEY: str = ''
    DATA_SOURCE_BASE_URL: str = 'http://localhost:8000/'
    CONNECTION_RETRIES: int = 10
    BACKOFF_FACTOR: int = 2

    LOG_LEVEL: int = logging.INFO

    # Output settings
    OUTPUT_FOLDER: str = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'output')
    OUTPUT_FORMAT: OutputFormats = OutputFormats.CSV.value


settings = Settings()

logging.basicConfig(level=settings.LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s')
