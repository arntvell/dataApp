#!/usr/bin/env python3
"""
Data Exploration Script
Loads all database tables into pandas DataFrames for analysis
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import create_engine, text
from database.config import DATABASE_URL

# Create engine
engine = create_engine(DATABASE_URL)

print("=" * 60)
print("DATAAPP - DATA EXPLORATION")
print("=" * 60)

# ============== CORE TABLES ==============

print("\n📦 Loading core tables...")

# Products
df_products = pd.read_sql("SELECT * FROM products", engine)
print(f"  products: {len(df_products):,} rows")

# Customers
df_customers = pd.read_sql("SELECT * FROM customers", engine)
print(f"  customers: {len(df_customers):,} rows")

# Orders (basic)
df_orders = pd.read_sql("SELECT * FROM orders", engine)
print(f"  orders: {len(df_orders):,} rows")

# Inventory
df_inventory = pd.read_sql("SELECT * FROM inventory", engine)
print(f"  inventory: {len(df_inventory):,} rows")

# ============== SALES DASHBOARD TABLES ==============

print("\n📊 Loading sales dashboard tables...")

# All Sales Orders
df_sales_orders = pd.read_sql("SELECT * FROM sales_orders ORDER BY order_date DESC", engine)
print(f"  sales_orders (all): {len(df_sales_orders):,} rows")

# Sales Orders - Sitoo (POS)
df_sales_orders_sitoo = pd.read_sql(
    "SELECT * FROM sales_orders WHERE source_system = 'sitoo' ORDER BY order_date DESC", 
    engine
)
print(f"  sales_orders (Sitoo/POS): {len(df_sales_orders_sitoo):,} rows")

# Sales Orders - Shopify (Online)
df_sales_orders_shopify = pd.read_sql(
    "SELECT * FROM sales_orders WHERE source_system = 'shopify' ORDER BY order_date DESC", 
    engine
)
print(f"  sales_orders (Shopify/Online): {len(df_sales_orders_shopify):,} rows")

# All Sales Order Items
df_sales_items = pd.read_sql("SELECT * FROM sales_order_items", engine)
print(f"  sales_order_items (all): {len(df_sales_items):,} rows")

# Sales Order Items - Sitoo
df_sales_items_sitoo = pd.read_sql("""
    SELECT soi.* 
    FROM sales_order_items soi
    JOIN sales_orders so ON soi.order_id = so.id
    WHERE so.source_system = 'sitoo'
""", engine)
print(f"  sales_order_items (Sitoo/POS): {len(df_sales_items_sitoo):,} rows")

# Sales Order Items - Shopify
df_sales_items_shopify = pd.read_sql("""
    SELECT soi.* 
    FROM sales_order_items soi
    JOIN sales_orders so ON soi.order_id = so.id
    WHERE so.source_system = 'shopify'
""", engine)
print(f"  sales_order_items (Shopify/Online): {len(df_sales_items_shopify):,} rows")

# Sync Status
df_sync_status = pd.read_sql("SELECT * FROM sync_status", engine)
print(f"  sync_status: {len(df_sync_status):,} rows")

# ============== SUMMARY ==============

print("\n" + "=" * 60)
print("DATAFRAMES AVAILABLE:")
print("=" * 60)
print("""
CORE TABLES:
  df_products          - All products from Sitoo & Shopify
  df_customers         - All customers
  df_orders            - Basic orders (legacy)
  df_inventory         - Inventory levels

SALES DASHBOARD:
  df_sales_orders           - All detailed sales orders
  df_sales_orders_sitoo     - POS orders (Sitoo)
  df_sales_orders_shopify   - Online orders (Shopify)
  
  df_sales_items            - All line items
  df_sales_items_sitoo      - POS line items
  df_sales_items_shopify    - Online line items
  
  df_sync_status            - Sync tracking
""")

# ============== QUICK STATS ==============

print("=" * 60)
print("QUICK STATS:")
print("=" * 60)

print(f"\n📈 Total Revenue (all time):")
print(f"   Sitoo (POS):     {df_sales_orders_sitoo['total_amount'].sum():,.0f} NOK")
print(f"   Shopify (Online): {df_sales_orders_shopify['total_amount'].sum():,.0f} NOK")
print(f"   TOTAL:           {df_sales_orders['total_amount'].sum():,.0f} NOK")

print(f"\n🏪 Orders by Location:")
location_counts = df_sales_orders.groupby('location').agg({
    'id': 'count',
    'total_amount': 'sum'
}).rename(columns={'id': 'orders', 'total_amount': 'revenue'})
location_counts['revenue'] = location_counts['revenue'].apply(lambda x: f"{x:,.0f} NOK")
print(location_counts.to_string())

print(f"\n📅 Date Range:")
print(f"   Sitoo:   {df_sales_orders_sitoo['order_date'].min()} to {df_sales_orders_sitoo['order_date'].max()}")
print(f"   Shopify: {df_sales_orders_shopify['order_date'].min()} to {df_sales_orders_shopify['order_date'].max()}")

print("\n" + "=" * 60)
print("To explore interactively, run: python -i scripts/explore_data.py")
print("=" * 60)
