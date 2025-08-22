"""
Example usage of the data ingestion system
"""
import time
from datetime import datetime
from data_ingestion import orchestrator, logger

def example_single_ingestion():
    """Example: Run ingestion for a single source"""
    print("=== Single Source Ingestion Example ===")
    
    # Load previous status
    orchestrator.load_ingestion_status()
    
    # Ingest from churn_data source
    success = orchestrator.ingest_source_by_name('churn_data')
    
    if success:
        print("✓ Churn data ingestion successful")
    else:
        print("✗ Churn data ingestion failed")
    
    # Save status
    orchestrator.save_ingestion_status()

def example_all_sources():
    """Example: Run ingestion for all sources"""
    print("=== All Sources Ingestion Example ===")
    
    orchestrator.load_ingestion_status()
    orchestrator.ingest_all_sources()
    
    # Print summary
    status = orchestrator.get_status_summary()
    print(f"Ingested from {status['enabled_sources']} sources")

def example_scheduled_ingestion():
    """Example: Set up and run scheduled ingestion"""
    print("=== Scheduled Ingestion Example ===")
    
    orchestrator.load_ingestion_status()
    orchestrator.setup_schedules()
    orchestrator.start_scheduler()
    
    print("Scheduler started. Running for 30 seconds...")
    try:
        time.sleep(30)
    except KeyboardInterrupt:
        pass
    finally:
        orchestrator.stop_scheduler()
        print("Scheduler stopped")

def example_custom_ingestion():
    """Example: Custom ingestion with specific configuration"""
    print("=== Custom Ingestion Example ===")
    
    # Custom source configuration
    custom_config = {
        'local_path': '/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection/churn_dataset/',
        'file_pattern': '*.csv',
        'frequency': 'manual',
        'enabled': True
    }
    
    success = orchestrator.ingest_single_source('custom_churn', custom_config)
    
    if success:
        print("✓ Custom ingestion successful")
    else:
        print("✗ Custom ingestion failed")

def example_monitoring():
    """Example: Monitor ingestion status"""
    print("=== Monitoring Example ===")
    
    orchestrator.load_ingestion_status()
    status = orchestrator.get_status_summary()
    
    print(f"Total sources: {status['total_sources']}")
    print(f"Enabled sources: {status['enabled_sources']}")
    
    print("\nLast run summary:")
    for source, info in status['last_run_summary'].items():
        print(f"  {source}: {info['status']} at {info['last_run']}")

if __name__ == "__main__":
    print("Data Ingestion System Examples")
    print("=" * 40)
    
    # Run examples
    example_single_ingestion()
    print()
    
    example_all_sources()
    print()
    
    example_custom_ingestion()
    print()
    
    example_monitoring()
    print()
    
    print("Examples completed. Check the logs directory for detailed logs.")
