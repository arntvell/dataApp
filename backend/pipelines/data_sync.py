import logging
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from database.config import get_db
from database.models import Product, Customer, Order, Inventory
from connectors.sitoo_connector import SitooConnector
from connectors.shopify_connector import ShopifyConnector

logger = logging.getLogger(__name__)

class DataSyncPipeline:
    """Main data synchronization pipeline"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connectors = {
            'sitoo': SitooConnector(config.get('sitoo', {})),
            'shopify': ShopifyConnector(config.get('shopify', {}))
        }
    
    def sync_all_data(self):
        """Sync data from all systems"""
        logger.info("Starting data synchronization...")
        
        try:
            # Sync products
            self.sync_products()
            
            # Sync customers
            self.sync_customers()
            
            # Sync orders
            self.sync_orders()
            
            # Sync inventory
            self.sync_inventory()
            
            logger.info("Data synchronization completed successfully")
            
        except Exception as e:
            logger.error(f"Data synchronization failed: {e}")
            raise
    
    def sync_products(self):
        """Sync products from all systems"""
        logger.info("Syncing products...")
        
        db = next(get_db())
        try:
            for system_name, connector in self.connectors.items():
                if connector.authenticate():
                    products = connector.get_products()
                    self._save_products(db, products, system_name)
                    logger.info(f"Synced {len(products)} products from {system_name}")
                else:
                    logger.warning(f"Failed to authenticate with {system_name}")
        finally:
            db.close()
    
    def sync_customers(self):
        """Sync customers from all systems"""
        logger.info("Syncing customers...")
        
        db = next(get_db())
        try:
            for system_name, connector in self.connectors.items():
                if connector.authenticate():
                    customers = connector.get_customers()
                    self._save_customers(db, customers, system_name)
                    logger.info(f"Synced {len(customers)} customers from {system_name}")
                else:
                    logger.warning(f"Failed to authenticate with {system_name}")
        finally:
            db.close()
    
    def sync_orders(self):
        """Sync orders from all systems"""
        logger.info("Syncing orders...")
        
        db = next(get_db())
        try:
            for system_name, connector in self.connectors.items():
                if connector.authenticate():
                    orders = connector.get_orders()
                    self._save_orders(db, orders, system_name)
                    logger.info(f"Synced {len(orders)} orders from {system_name}")
                else:
                    logger.warning(f"Failed to authenticate with {system_name}")
        finally:
            db.close()
    
    def sync_inventory(self):
        """Sync inventory from all systems"""
        logger.info("Syncing inventory...")
        
        db = next(get_db())
        try:
            for system_name, connector in self.connectors.items():
                if connector.authenticate():
                    inventory = connector.get_inventory()
                    self._save_inventory(db, inventory, system_name)
                    logger.info(f"Synced {len(inventory)} inventory items from {system_name}")
                else:
                    logger.warning(f"Failed to authenticate with {system_name}")
        finally:
            db.close()
    
    def _save_products(self, db: Session, products: List[Dict[str, Any]], system_name: str):
        """Save products to database"""
        for product_data in products:
            # Check if product already exists
            existing_product = db.query(Product).filter(
                Product.source_system == system_name,
                Product.source_id == product_data['source_id']
            ).first()
            
            if existing_product:
                # Update existing product
                for key, value in product_data.items():
                    if hasattr(existing_product, key):
                        setattr(existing_product, key, value)
            else:
                # Create new product
                new_product = Product(**product_data)
                db.add(new_product)
        
        db.commit()
    
    def _save_customers(self, db: Session, customers: List[Dict[str, Any]], system_name: str):
        """Save customers to database"""
        for customer_data in customers:
            # Check if customer already exists
            existing_customer = db.query(Customer).filter(
                Customer.source_system == system_name,
                Customer.source_id == customer_data['source_id']
            ).first()
            
            if existing_customer:
                # Update existing customer
                for key, value in customer_data.items():
                    if hasattr(existing_customer, key):
                        setattr(existing_customer, key, value)
            else:
                # Create new customer
                new_customer = Customer(**customer_data)
                db.add(new_customer)
        
        db.commit()
    
    def _save_orders(self, db: Session, orders: List[Dict[str, Any]], system_name: str):
        """Save orders to database"""
        for order_data in orders:
            # Rename customer_id to customer_source_id for database
            if 'customer_id' in order_data:
                order_data['customer_source_id'] = order_data.pop('customer_id')
            
            # Check if order already exists
            existing_order = db.query(Order).filter(
                Order.source_system == system_name,
                Order.source_id == order_data['source_id']
            ).first()
            
            if existing_order:
                # Update existing order
                for key, value in order_data.items():
                    if hasattr(existing_order, key):
                        setattr(existing_order, key, value)
            else:
                # Create new order
                new_order = Order(**order_data)
                db.add(new_order)
        
        db.commit()
    
    def _save_inventory(self, db: Session, inventory: List[Dict[str, Any]], system_name: str):
        """Save inventory to database"""
        for inventory_data in inventory:
            # Check if inventory already exists
            existing_inventory = db.query(Inventory).filter(
                Inventory.product_id == inventory_data['product_id'],
                Inventory.location == inventory_data['location']
            ).first()
            
            if existing_inventory:
                # Update existing inventory
                existing_inventory.quantity = inventory_data['quantity']
                existing_inventory.reserved_quantity = inventory_data['reserved_quantity']
            else:
                # Create new inventory
                new_inventory = Inventory(**inventory_data)
                db.add(new_inventory)
        
        db.commit()
