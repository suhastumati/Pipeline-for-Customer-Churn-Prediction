"""
Logging utility for data ingestion system
"""
import logging
import os
from datetime import datetime
from config import LOG_FILE, LOG_FORMAT, LOG_LEVEL, LOG_DIR

class IngestionLogger:
    def __init__(self, name="DataIngestion"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, LOG_LEVEL))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(LOG_FORMAT)
        
        # File handler
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def log_ingestion_start(self, source_name):
        self.info(f"Starting data ingestion for source: {source_name}")
    
    def log_ingestion_success(self, source_name, file_count, total_size_mb):
        self.info(f"Successfully ingested {file_count} files from {source_name}. Total size: {total_size_mb:.2f} MB")
    
    def log_ingestion_failure(self, source_name, error_message):
        self.error(f"Failed to ingest data from {source_name}: {error_message}")
    
    def log_file_processed(self, file_path, file_size_mb):
        self.info(f"Processed file: {file_path} (Size: {file_size_mb:.2f} MB)")
    
    def log_api_call(self, url, status_code, response_time):
        if status_code == 200:
            self.info(f"API call successful: {url} (Status: {status_code}, Time: {response_time:.2f}s)")
        else:
            self.warning(f"API call failed: {url} (Status: {status_code}, Time: {response_time:.2f}s)")

# Create a global logger instance
logger = IngestionLogger()
