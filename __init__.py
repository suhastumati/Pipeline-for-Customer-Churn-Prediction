"""
Initialize the data ingestion package
"""
from .orchestrator import orchestrator
from .logger import logger
from .config import DATA_SOURCES
from .utils import DataIngestionUtils

__version__ = "1.0.0"
__author__ = "Data Science Team"

__all__ = [
    'orchestrator',
    'logger', 
    'DATA_SOURCES',
    'DataIngestionUtils'
]
