#!/usr/bin/env python3
"""
Script to export all Shopify pages containing 'fitguide'
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
import logging

# Add the parent directory to the path so we can import from backend
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.connectors.shopify_connector import ShopifyConnector
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from environment variables"""
    load_dotenv()
    
    config = {
        'base_url': os.getenv('SHOPIFY_BASE_URL'),
        'api_key': os.getenv('SHOPIFY_API_KEY')
    }
    
    # Validate config
    if not config['base_url'] or not config['api_key']:
        raise ValueError("Missing Shopify configuration. Please check your .env file.")
    
    return config

def export_to_json(pages, filename=None):
    """Export pages to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'fitguide_pages_{timestamp}.json'
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Exported {len(pages)} pages to {filename}")
    return filename

def export_to_csv(pages, filename=None):
    """Export pages to CSV file"""
    import csv
    
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'fitguide_pages_{timestamp}.csv'
    
    if not pages:
        logger.warning("No pages to export")
        return filename
    
    # Define CSV fields for REST API response
    fields = ['id', 'title', 'handle', 'published', 'created_at', 'updated_at', 'author']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        
        for page in pages:
            writer.writerow(page)
    
    logger.info(f"Exported {len(pages)} pages to {filename}")
    return filename

def main():
    """Main function"""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Initialize Shopify connector
        logger.info("Initializing Shopify connector...")
        shopify = ShopifyConnector(config)
        
        # Authenticate
        logger.info("Authenticating with Shopify...")
        if not shopify.authenticate():
            logger.error("Failed to authenticate with Shopify")
            return 1
        
        # Get pages containing 'fitguide'
        logger.info("Fetching pages containing 'fitguide'...")
        pages = shopify.get_pages(query_filter="fitguide")
        
        if not pages:
            logger.warning("No pages found containing 'fitguide'")
            return 0
        
        # Display results
        logger.info(f"\nFound {len(pages)} page(s) containing 'fitguide':\n")
        for i, page in enumerate(pages, 1):
            logger.info(f"{i}. {page['title']}")
            logger.info(f"   Handle: {page['handle']}")
            logger.info(f"   Published: {page.get('published', False)}")
            logger.info(f"   URL: {config['base_url']}/pages/{page['handle']}")
            logger.info("")
        
        # Export to files
        json_file = export_to_json(pages)
        csv_file = export_to_csv(pages)
        
        logger.info(f"\n✓ Export complete!")
        logger.info(f"  JSON: {json_file}")
        logger.info(f"  CSV:  {csv_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())

