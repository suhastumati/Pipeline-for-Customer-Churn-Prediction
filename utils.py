"""
Data ingestion utilities and helper functions
"""
import os
import shutil
import requests
import pandas as pd
import time
import hashlib
import glob
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

from logger import logger
from config import API_TIMEOUT, MAX_RETRIES, RETRY_DELAY, MAX_FILE_SIZE

class DataIngestionUtils:
    @staticmethod
    def calculate_file_hash(file_path: str) -> str:
        """Calculate MD5 hash of a file for integrity checking"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {str(e)}")
            return None
    
    @staticmethod
    def get_file_size_mb(file_path: str) -> float:
        """Get file size in MB"""
        try:
            size_bytes = os.path.getsize(file_path)
            return size_bytes / (1024 * 1024)
        except Exception as e:
            logger.error(f"Error getting file size for {file_path}: {str(e)}")
            return 0.0
    
    @staticmethod
    def validate_file_size(file_path: str, max_size_mb: float = MAX_FILE_SIZE) -> bool:
        """Validate if file size is within acceptable limits"""
        file_size = DataIngestionUtils.get_file_size_mb(file_path)
        if file_size > max_size_mb:
            logger.warning(f"File {file_path} exceeds maximum size limit ({file_size:.2f}MB > {max_size_mb}MB)")
            return False
        return True
    
    @staticmethod
    def create_backup(file_path: str) -> str:
        """Create a backup of existing file before overwriting"""
        if os.path.exists(file_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{file_path}.backup_{timestamp}"
            try:
                shutil.copy2(file_path, backup_path)
                logger.info(f"Created backup: {backup_path}")
                return backup_path
            except Exception as e:
                logger.error(f"Failed to create backup for {file_path}: {str(e)}")
        return None
    
    @staticmethod
    def safe_copy_file(src: str, dst: str, create_backup: bool = True) -> bool:
        """Safely copy file with backup and validation"""
        try:
            # Validate source file
            if not os.path.exists(src):
                logger.error(f"Source file does not exist: {src}")
                return False
            
            if not DataIngestionUtils.validate_file_size(src):
                return False
            
            # Create destination directory if it doesn't exist
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            
            # Create backup if file already exists
            if create_backup and os.path.exists(dst):
                DataIngestionUtils.create_backup(dst)
            
            # Copy file
            shutil.copy2(src, dst)
            
            # Verify copy integrity
            src_hash = DataIngestionUtils.calculate_file_hash(src)
            dst_hash = DataIngestionUtils.calculate_file_hash(dst)
            
            if src_hash and dst_hash and src_hash == dst_hash:
                file_size = DataIngestionUtils.get_file_size_mb(dst)
                logger.log_file_processed(dst, file_size)
                return True
            else:
                logger.error(f"File integrity check failed for {dst}")
                return False
                
        except Exception as e:
            logger.error(f"Error copying file from {src} to {dst}: {str(e)}")
            return False
    
    @staticmethod
    def make_api_request(url: str, method: str = "GET", **kwargs) -> Optional[requests.Response]:
        """Make API request with retry logic and logging"""
        for attempt in range(MAX_RETRIES):
            try:
                start_time = time.time()
                
                if method.upper() == "GET":
                    response = requests.get(url, timeout=API_TIMEOUT, **kwargs)
                elif method.upper() == "POST":
                    response = requests.post(url, timeout=API_TIMEOUT, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response_time = time.time() - start_time
                logger.log_api_call(url, response.status_code, response_time)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limit
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"API request failed with status {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout for {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error for {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            except Exception as e:
                logger.error(f"Unexpected error in API request: {str(e)}")
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        
        logger.error(f"Failed to make successful API request to {url} after {MAX_RETRIES} attempts")
        return None
    
    @staticmethod
    def read_csv_safely(file_path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Safely read CSV file with error handling"""
        try:
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Successfully read CSV: {file_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df
        except FileNotFoundError:
            logger.error(f"CSV file not found: {file_path}")
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file is empty: {file_path}")
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error reading CSV {file_path}: {str(e)}")
        return None
    
    @staticmethod
    def get_files_by_pattern(directory: str, pattern: str) -> List[str]:
        """Get list of files matching pattern in directory"""
        try:
            patterns = pattern.split(',')
            all_files = []
            for pat in patterns:
                files = glob.glob(os.path.join(directory, pat.strip()))
                all_files.extend(files)
            return sorted(list(set(all_files)))  # Remove duplicates and sort
        except Exception as e:
            logger.error(f"Error getting files with pattern {pattern} in {directory}: {str(e)}")
            return []
    
    @staticmethod
    def is_file_recently_modified(file_path: str, hours: int = 24) -> bool:
        """Check if file was modified within specified hours"""
        try:
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            threshold = datetime.now() - timedelta(hours=hours)
            return file_time > threshold
        except Exception as e:
            logger.error(f"Error checking modification time for {file_path}: {str(e)}")
            return False
