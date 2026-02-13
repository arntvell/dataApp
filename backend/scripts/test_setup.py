#!/usr/bin/env python3
"""
Test script to verify DataApp setup
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_imports():
    """Test that all modules can be imported"""
    try:
        from database.config import engine, Base
        print("✓ Database configuration imported successfully")
        
        from database.models import Product, Customer, Order, Inventory
        print("✓ Database models imported successfully")
        
        from connectors.base_connector import BaseConnector
        print("✓ Base connector imported successfully")
        
        from connectors.sitoo_connector import SitooConnector
        print("✓ Sitoo connector imported successfully")
        
        from connectors.shopify_connector import ShopifyConnector
        print("✓ Shopify connector imported successfully")
        
        from pipelines.data_sync import DataSyncPipeline
        print("✓ Data sync pipeline imported successfully")
        
        print("\n🎉 All imports successful! DataApp is ready to use.")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_database_connection():
    """Test database connection"""
    try:
        from database.config import engine
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✓ Database connection successful")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing DataApp setup...\n")
    
    # Test imports
    imports_ok = test_imports()
    
    print("\n" + "="*50 + "\n")
    
    if imports_ok:
        print("Testing database connection...")
        db_ok = test_database_connection()
        
        if db_ok:
            print("\n🎉 DataApp setup is complete and working!")
            print("\nNext steps:")
            print("1. Configure your API keys in the .env file")
            print("2. Run 'docker-compose up -d' to start services")
            print("3. Visit http://localhost:8000/docs for API documentation")
        else:
            print("\n⚠️  Database connection failed. Make sure PostgreSQL is running.")
    else:
        print("\n❌ Setup incomplete. Please check the errors above.")
