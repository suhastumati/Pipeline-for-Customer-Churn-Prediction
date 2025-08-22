"""
Configuration file for data ingestion system
"""
import os
from datetime import datetime

# Base configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

# Data directories
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "raw_data")
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "processed_data")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging configuration
LOG_FILE = os.path.join(LOG_DIR, f"data_ingestion_{datetime.now().strftime('%Y%m%d')}.log")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"

# Data source configurations
DATA_SOURCES = {
    "churn_data": {
        "url": "https://example.com/api/churn_data",  # Replace with actual API endpoint
        "local_path": "/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection/churn_dataset/",
        "file_pattern": "*.csv",
        "frequency": "daily",  # daily, hourly, weekly
        "enabled": True
    },
    "image_data": {
        "url": "https://example.com/api/images",  # Replace with actual API endpoint
        "local_path": "/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection/Images/",
        "file_pattern": "*.jpg,*.png,*.jpeg",
        "frequency": "hourly",
        "enabled": True
    },
    "kaggle_datasets": {
        "dataset_ids": ["username/dataset-name"],  # Replace with actual Kaggle dataset IDs
        "local_path": "/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection/kaggle_data/",
        "frequency": "daily",
        "enabled": False  # Set to True when you have Kaggle API configured
    }
}

# API configuration
API_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# File size limits (in MB)
MAX_FILE_SIZE = 500

# Email notification settings (optional)
EMAIL_NOTIFICATIONS = {
    "enabled": False,
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": "your-email@gmail.com",
    "sender_password": "your-app-password",
    "recipient_emails": ["admin@company.com"]
}
