"""
Main entry point for data ingestion system
Run this script to start automated data ingestion
"""
import argparse
import sys
import time
from datetime import datetime

from logger import logger
from orchestrator import orchestrator
from config import DATA_SOURCES

def run_single_ingestion(source_name: str = None):
    """Run ingestion for a single source or all sources"""
    orchestrator.load_ingestion_status()
    
    if source_name:
        if source_name not in DATA_SOURCES:
            logger.error(f"Unknown source: {source_name}")
            print(f"Available sources: {', '.join(DATA_SOURCES.keys())}")
            return False
        
        success = orchestrator.ingest_source_by_name(source_name)
        return success
    else:
        orchestrator.ingest_all_sources()
        return True

def run_scheduled_ingestion():
    """Run the scheduled ingestion service"""
    logger.info("Starting data ingestion service...")
    
    orchestrator.load_ingestion_status()
    orchestrator.setup_schedules()
    orchestrator.start_scheduler()
    
    try:
        logger.info("Data ingestion service is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(10)
            # Print status every 10 minutes
            if int(time.time()) % 600 == 0:
                status = orchestrator.get_status_summary()
                logger.info(f"Service status: {status['enabled_sources']} sources enabled")
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
        orchestrator.stop_scheduler()
        logger.info("Data ingestion service stopped")

def show_status():
    """Show current ingestion status"""
    orchestrator.load_ingestion_status()
    status = orchestrator.get_status_summary()
    
    print("\n=== Data Ingestion Status ===")
    print(f"Total sources: {status['total_sources']}")
    print(f"Enabled sources: {status['enabled_sources']}")
    
    print("\n=== Last Run Summary ===")
    for source, info in status['last_run_summary'].items():
        print(f"  {source}: {info['status']} (last run: {info['last_run']})")
    
    print("\n=== Next Scheduled Runs ===")
    for job in status['next_scheduled_runs']:
        print(f"  {job['job']}: {job['next_run']}")
    
    print("\n=== Source Configuration ===")
    for source_name, config in DATA_SOURCES.items():
        enabled = "✓" if config.get('enabled', True) else "✗"
        frequency = config.get('frequency', 'daily')
        print(f"  {enabled} {source_name}: {frequency}")

def list_sources():
    """List all configured data sources"""
    print("\n=== Configured Data Sources ===")
    for source_name, config in DATA_SOURCES.items():
        enabled = "✓" if config.get('enabled', True) else "✗"
        frequency = config.get('frequency', 'daily')
        source_type = "API" if 'url' in config else "Local" if 'local_path' in config else "Kaggle"
        print(f"  {enabled} {source_name} ({source_type}) - {frequency}")

def main():
    parser = argparse.ArgumentParser(description="Data Ingestion System")
    parser.add_argument('action', choices=['run', 'schedule', 'status', 'list'], 
                       help='Action to perform')
    parser.add_argument('--source', '-s', help='Specific source to ingest (for run action)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.action == 'run':
            success = run_single_ingestion(args.source)
            if success:
                print("✓ Ingestion completed successfully")
                sys.exit(0)
            else:
                print("✗ Ingestion failed")
                sys.exit(1)
        
        elif args.action == 'schedule':
            run_scheduled_ingestion()
        
        elif args.action == 'status':
            show_status()
        
        elif args.action == 'list':
            list_sources()
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
