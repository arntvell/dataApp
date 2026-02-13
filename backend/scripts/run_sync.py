#!/usr/bin/env python3
"""
Manual data synchronization script
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from pipelines.data_sync import DataSyncPipeline
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Run data synchronization"""
    print("Starting DataApp data synchronization...")
    
    try:
        # Get connector configurations
        config = settings.get_connector_configs()
        
        # Initialize pipeline
        pipeline = DataSyncPipeline(config)
        
        # Run synchronization
        pipeline.sync_all_data()
        
        print("✅ Data synchronization completed successfully!")
        
    except Exception as e:
        print(f"❌ Data synchronization failed: {e}")
        logging.error(f"Sync error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
