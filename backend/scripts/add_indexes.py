#!/usr/bin/env python3
"""
Add missing performance indexes to the database.
Safe to run multiple times (uses CREATE INDEX IF NOT EXISTS).
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from database.config import engine

INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_sales_orders_date_source ON sales_orders (order_date, source_system)",
    "CREATE INDEX IF NOT EXISTS ix_sales_orders_staff_name ON sales_orders (staff_name)",
    "CREATE INDEX IF NOT EXISTS ix_sales_orders_date_staff ON sales_orders (order_date, staff_name)",
    "CREATE INDEX IF NOT EXISTS ix_sales_order_items_order_sku ON sales_order_items (order_id, sku)",
]

def add_indexes():
    print("Adding performance indexes...")
    with engine.begin() as conn:
        for sql in INDEXES:
            name = sql.split("ix_")[1].split(" ")[0]
            print(f"  Creating ix_{name}...")
            conn.execute(text(sql))
    print("Done.")

if __name__ == "__main__":
    add_indexes()
