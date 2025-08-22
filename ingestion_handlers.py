"""
Specific handlers for different types of data ingestion
"""
import os
import shutil
import requests
import pandas as pd
import zipfile
from datetime import datetime
from typing import Dict, Any, List
from abc import ABC, abstractmethod

from logger import logger
from utils import DataIngestionUtils
from config import RAW_DATA_DIR

class BaseIngestionHandler(ABC):
    """Base class for all ingestion handlers"""
    
    @abstractmethod
    def ingest(self, source_name: str, source_config: Dict[str, Any]) -> bool:
        """Ingest data from source"""
        pass

class LocalFileIngestionHandler(BaseIngestionHandler):
    """Handler for ingesting data from local file sources"""
    
    def ingest(self, source_name: str, source_config: Dict[str, Any]) -> bool:
        """Ingest data from local files"""
        try:
            local_path = source_config.get('local_path')
            file_pattern = source_config.get('file_pattern', '*')
            
            if not local_path or not os.path.exists(local_path):
                logger.error(f"Local path does not exist: {local_path}")
                return False
            
            # Create destination directory
            dest_dir = os.path.join(RAW_DATA_DIR, source_name)
            os.makedirs(dest_dir, exist_ok=True)
            
            # Get files matching pattern
            files = DataIngestionUtils.get_files_by_pattern(local_path, file_pattern)
            
            if not files:
                logger.warning(f"No files found matching pattern {file_pattern} in {local_path}")
                return True  # Not an error, just no new files
            
            file_count = 0
            total_size = 0
            
            for file_path in files:
                filename = os.path.basename(file_path)
                dest_path = os.path.join(dest_dir, filename)
                
                # Add timestamp to filename to avoid conflicts
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name, ext = os.path.splitext(filename)
                dest_path = os.path.join(dest_dir, f"{name}_{timestamp}{ext}")
                
                if DataIngestionUtils.safe_copy_file(file_path, dest_path):
                    file_count += 1
                    total_size += DataIngestionUtils.get_file_size_mb(dest_path)
            
            logger.log_ingestion_success(source_name, file_count, total_size)
            return True
            
        except Exception as e:
            logger.error(f"Error in local file ingestion for {source_name}: {str(e)}")
            return False

class APIIngestionHandler(BaseIngestionHandler):
    """Handler for ingesting data from API sources"""
    
    def ingest(self, source_name: str, source_config: Dict[str, Any]) -> bool:
        """Ingest data from API"""
        try:
            url = source_config.get('url')
            if not url:
                logger.error(f"No URL specified for API source {source_name}")
                return False
            
            # Create destination directory
            dest_dir = os.path.join(RAW_DATA_DIR, source_name)
            os.makedirs(dest_dir, exist_ok=True)
            
            # Make API request
            response = DataIngestionUtils.make_api_request(url)
            if not response:
                return False
            
            # Determine content type and save accordingly
            content_type = response.headers.get('content-type', '').lower()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if 'json' in content_type:
                filename = f"{source_name}_data_{timestamp}.json"
                file_path = os.path.join(dest_dir, filename)
                
                with open(file_path, 'w') as f:
                    f.write(response.text)
                    
            elif 'csv' in content_type or 'text' in content_type:
                filename = f"{source_name}_data_{timestamp}.csv"
                file_path = os.path.join(dest_dir, filename)
                
                with open(file_path, 'w') as f:
                    f.write(response.text)
                    
            elif 'zip' in content_type:
                filename = f"{source_name}_data_{timestamp}.zip"
                file_path = os.path.join(dest_dir, filename)
                
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                # Extract zip file
                try:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        extract_dir = os.path.join(dest_dir, f"extracted_{timestamp}")
                        zip_ref.extractall(extract_dir)
                        logger.info(f"Extracted zip file to {extract_dir}")
                except Exception as e:
                    logger.warning(f"Failed to extract zip file: {str(e)}")
                    
            else:
                # Default to binary save
                filename = f"{source_name}_data_{timestamp}.bin"
                file_path = os.path.join(dest_dir, filename)
                
                with open(file_path, 'wb') as f:
                    f.write(response.content)
            
            file_size = DataIngestionUtils.get_file_size_mb(file_path)
            logger.log_ingestion_success(source_name, 1, file_size)
            return True
            
        except Exception as e:
            logger.error(f"Error in API ingestion for {source_name}: {str(e)}")
            return False

class KaggleIngestionHandler(BaseIngestionHandler):
    """Handler for ingesting data from Kaggle datasets"""
    
    def __init__(self):
        self.kaggle_api = None
        self._setup_kaggle_api()
    
    def _setup_kaggle_api(self):
        """Setup Kaggle API if available"""
        try:
            import kaggle
            from kaggle.api.kaggle_api_extended import KaggleApi
            
            self.kaggle_api = KaggleApi()
            self.kaggle_api.authenticate()
            logger.info("Kaggle API initialized successfully")
            
        except ImportError:
            logger.warning("Kaggle package not installed. Install with: pip install kaggle")
        except Exception as e:
            logger.warning(f"Failed to initialize Kaggle API: {str(e)}")
    
    def ingest(self, source_name: str, source_config: Dict[str, Any]) -> bool:
        """Ingest data from Kaggle datasets"""
        if not self.kaggle_api:
            logger.error("Kaggle API not available")
            return False
        
        try:
            dataset_ids = source_config.get('dataset_ids', [])
            if not dataset_ids:
                logger.error(f"No dataset IDs specified for Kaggle source {source_name}")
                return False
            
            # Create destination directory
            dest_dir = os.path.join(RAW_DATA_DIR, source_name)
            os.makedirs(dest_dir, exist_ok=True)
            
            file_count = 0
            total_size = 0
            
            for dataset_id in dataset_ids:
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    dataset_dir = os.path.join(dest_dir, f"{dataset_id.replace('/', '_')}_{timestamp}")
                    
                    logger.info(f"Downloading Kaggle dataset: {dataset_id}")
                    self.kaggle_api.dataset_download_files(dataset_id, path=dataset_dir, unzip=True)
                    
                    # Count files and calculate total size
                    for root, dirs, files in os.walk(dataset_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            file_count += 1
                            total_size += DataIngestionUtils.get_file_size_mb(file_path)
                    
                    logger.info(f"Successfully downloaded dataset {dataset_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to download dataset {dataset_id}: {str(e)}")
                    continue
            
            if file_count > 0:
                logger.log_ingestion_success(source_name, file_count, total_size)
                return True
            else:
                logger.warning(f"No files downloaded for {source_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error in Kaggle ingestion for {source_name}: {str(e)}")
            return False

class CSVIngestionHandler(BaseIngestionHandler):
    """Specialized handler for CSV data ingestion with validation"""
    
    def ingest(self, source_name: str, source_config: Dict[str, Any]) -> bool:
        """Ingest and validate CSV data"""
        try:
            # Use local file handler as base
            local_handler = LocalFileIngestionHandler()
            if not local_handler.ingest(source_name, source_config):
                return False
            
            # Additional CSV validation
            dest_dir = os.path.join(RAW_DATA_DIR, source_name)
            csv_files = DataIngestionUtils.get_files_by_pattern(dest_dir, "*.csv")
            
            for csv_file in csv_files:
                df = DataIngestionUtils.read_csv_safely(csv_file)
                if df is not None:
                    # Basic validation
                    if len(df) == 0:
                        logger.warning(f"CSV file {csv_file} is empty")
                    elif df.isnull().all().all():
                        logger.warning(f"CSV file {csv_file} contains only null values")
                    else:
                        logger.info(f"CSV validation passed for {csv_file}: {len(df)} rows, {len(df.columns)} columns")
                        
                        # Save basic statistics
                        stats_file = csv_file.replace('.csv', '_stats.json')
                        stats = {
                            'rows': len(df),
                            'columns': len(df.columns),
                            'column_names': df.columns.tolist(),
                            'null_counts': df.isnull().sum().to_dict(),
                            'data_types': df.dtypes.astype(str).to_dict(),
                            'ingestion_timestamp': datetime.now().isoformat()
                        }
                        
                        import json
                        with open(stats_file, 'w') as f:
                            json.dump(stats, f, indent=2)
                        
                        logger.info(f"Saved statistics to {stats_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in CSV ingestion for {source_name}: {str(e)}")
            return False
