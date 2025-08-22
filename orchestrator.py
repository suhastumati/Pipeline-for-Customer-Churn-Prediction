"""
Main data ingestion orchestrator
"""
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
import schedule
import threading

from logger import logger
from config import DATA_SOURCES, RAW_DATA_DIR
from ingestion_handlers import (
    LocalFileIngestionHandler,
    APIIngestionHandler,
    KaggleIngestionHandler
)

class DataIngestionOrchestrator:
    def __init__(self):
        self.handlers = {
            'local': LocalFileIngestionHandler(),
            'api': APIIngestionHandler(),
            'kaggle': KaggleIngestionHandler()
        }
        self.ingestion_status = {}
        self.running = False
    
    def determine_handler_type(self, source_config: Dict[str, Any]) -> str:
        """Determine which handler to use based on source configuration"""
        if 'url' in source_config and source_config['url'].startswith('http'):
            return 'api'
        elif 'dataset_ids' in source_config:
            return 'kaggle'
        elif 'local_path' in source_config:
            return 'local'
        else:
            return 'local'  # default
    
    def ingest_single_source(self, source_name: str, source_config: Dict[str, Any]) -> bool:
        """Ingest data from a single source"""
        if not source_config.get('enabled', True):
            logger.info(f"Source {source_name} is disabled, skipping...")
            return True
        
        logger.log_ingestion_start(source_name)
        start_time = time.time()
        
        try:
            handler_type = self.determine_handler_type(source_config)
            handler = self.handlers[handler_type]
            
            success = handler.ingest(source_name, source_config)
            
            if success:
                elapsed_time = time.time() - start_time
                self.ingestion_status[source_name] = {
                    'last_run': datetime.now().isoformat(),
                    'status': 'success',
                    'elapsed_time': elapsed_time
                }
                logger.info(f"Successfully completed ingestion for {source_name} in {elapsed_time:.2f} seconds")
                return True
            else:
                self.ingestion_status[source_name] = {
                    'last_run': datetime.now().isoformat(),
                    'status': 'failed',
                    'elapsed_time': time.time() - start_time
                }
                logger.log_ingestion_failure(source_name, "Handler returned False")
                return False
                
        except Exception as e:
            self.ingestion_status[source_name] = {
                'last_run': datetime.now().isoformat(),
                'status': 'error',
                'error': str(e),
                'elapsed_time': time.time() - start_time
            }
            logger.log_ingestion_failure(source_name, str(e))
            return False
    
    def ingest_all_sources(self):
        """Ingest data from all configured sources"""
        logger.info("Starting scheduled data ingestion for all sources")
        success_count = 0
        total_count = len(DATA_SOURCES)
        
        for source_name, source_config in DATA_SOURCES.items():
            if self.ingest_single_source(source_name, source_config):
                success_count += 1
        
        logger.info(f"Completed ingestion batch: {success_count}/{total_count} sources successful")
        self.save_ingestion_status()
    
    def ingest_source_by_name(self, source_name: str):
        """Ingest data from a specific source by name"""
        if source_name not in DATA_SOURCES:
            logger.error(f"Source '{source_name}' not found in configuration")
            return False
        
        return self.ingest_single_source(source_name, DATA_SOURCES[source_name])
    
    def save_ingestion_status(self):
        """Save ingestion status to file for monitoring"""
        status_file = os.path.join(RAW_DATA_DIR, 'ingestion_status.json')
        try:
            with open(status_file, 'w') as f:
                json.dump(self.ingestion_status, f, indent=2)
            logger.info(f"Saved ingestion status to {status_file}")
        except Exception as e:
            logger.error(f"Failed to save ingestion status: {str(e)}")
    
    def load_ingestion_status(self):
        """Load previous ingestion status"""
        status_file = os.path.join(RAW_DATA_DIR, 'ingestion_status.json')
        try:
            if os.path.exists(status_file):
                with open(status_file, 'r') as f:
                    self.ingestion_status = json.load(f)
                logger.info(f"Loaded ingestion status from {status_file}")
        except Exception as e:
            logger.error(f"Failed to load ingestion status: {str(e)}")
    
    def setup_schedules(self):
        """Setup scheduled ingestion jobs based on configuration"""
        logger.info("Setting up ingestion schedules...")
        
        for source_name, source_config in DATA_SOURCES.items():
            if not source_config.get('enabled', True):
                continue
                
            frequency = source_config.get('frequency', 'daily')
            
            if frequency == 'hourly':
                schedule.every().hour.do(self.ingest_source_by_name, source_name)
                logger.info(f"Scheduled {source_name} for hourly ingestion")
            elif frequency == 'daily':
                schedule.every().day.at("02:00").do(self.ingest_source_by_name, source_name)
                logger.info(f"Scheduled {source_name} for daily ingestion at 02:00")
            elif frequency == 'weekly':
                schedule.every().monday.at("02:00").do(self.ingest_source_by_name, source_name)
                logger.info(f"Scheduled {source_name} for weekly ingestion (Mondays at 02:00)")
    
    def start_scheduler(self):
        """Start the background scheduler"""
        self.running = True
        logger.info("Starting ingestion scheduler...")
        
        def run_scheduler():
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("Scheduler started in background thread")
    
    def stop_scheduler(self):
        """Stop the background scheduler"""
        self.running = False
        logger.info("Scheduler stopped")
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get a summary of ingestion status"""
        summary = {
            'total_sources': len(DATA_SOURCES),
            'enabled_sources': len([s for s in DATA_SOURCES.values() if s.get('enabled', True)]),
            'last_run_summary': {},
            'next_scheduled_runs': []
        }
        
        for source_name, status in self.ingestion_status.items():
            summary['last_run_summary'][source_name] = {
                'status': status.get('status', 'unknown'),
                'last_run': status.get('last_run', 'never')
            }
        
        # Get next scheduled runs
        for job in schedule.jobs:
            summary['next_scheduled_runs'].append({
                'job': str(job.job_func),
                'next_run': job.next_run.isoformat() if job.next_run else None
            })
        
        return summary


# Global orchestrator instance
orchestrator = DataIngestionOrchestrator()
