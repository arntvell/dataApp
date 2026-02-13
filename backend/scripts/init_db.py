#!/usr/bin/env python3
"""
Database initialization script
Creates schemas and tables
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from database.config import engine, Base
from database.models import (
    Product, Customer, Order, Inventory,
    SalesOrder, SalesOrderItem, SalesRefund, SyncStatus,
    CategoryMapping, ParentSkuMapping, StaffMapping,
    SameSystemBudget, SameSystemWorktime,
    Cin7Stock, Cin7Sale, Cin7SaleItem, Cin7Purchase, Cin7PurchaseItem,
)

def init_database():
    """Initialize the database with schemas and tables"""
    # Create schemas first
    print("Creating database schemas...")
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts"))
    print("Schemas created: raw, staging, marts")

    # Create all tables
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully!")

if __name__ == "__main__":
    init_database()
