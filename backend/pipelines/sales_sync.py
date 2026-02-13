"""
Sales data synchronization pipeline for dashboard
Supports both full historical sync and incremental sync
Enriches data with category mappings, parent SKUs, staff names, and vendor standardization
"""

import logging
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, text
from database.config import get_db, SessionLocal
from database.models import SalesOrder, SalesOrderItem, SalesRefund, SyncStatus, CategoryMapping, StaffMapping, ParentSkuMapping
from connectors.sitoo_connector import SitooConnector
from connectors.shopify_connector import ShopifyConnector
from data.vendor_standardization import standardize_vendor

logger = logging.getLogger(__name__)


class SalesSyncPipeline:
    """Sync detailed sales data for dashboard"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sitoo = SitooConnector(config.get('sitoo', {}))
        self.shopify = ShopifyConnector(config.get('shopify', {}))
    
    def _get_sync_status(self, db: Session, source: str) -> SyncStatus:
        """Get or create sync status for a source"""
        status = db.query(SyncStatus).filter(SyncStatus.source_system == source).first()
        if not status:
            status = SyncStatus(source_system=source)
            db.add(status)
            db.commit()
            db.refresh(status)
        return status
    
    def _update_sync_status(self, db: Session, source: str, **kwargs):
        """Update sync status for a source"""
        status = self._get_sync_status(db, source)
        for key, value in kwargs.items():
            if hasattr(status, key):
                setattr(status, key, value)
        db.commit()
    
    # ============== INCREMENTAL SYNC (Daily Use) ==============
    
    def sync_incremental(self, progress_callback: Callable = None):
        """
        Smart incremental sync - only fetches orders since last sync.
        This is the recommended method for daily dashboard updates.
        """
        logger.info("Starting incremental sync...")
        
        db = SessionLocal()
        try:
            results = {'sitoo': 0, 'shopify': 0}
            
            # Sync Sitoo
            if self.sitoo.authenticate():
                sitoo_status = self._get_sync_status(db, 'sitoo')
                
                # Determine start date
                from_date = sitoo_status.last_order_date
                if from_date:
                    # Go back 1 day to catch any late updates
                    from_date = from_date - timedelta(days=1)
                else:
                    # First sync - get last 30 days
                    from_date = datetime.now() - timedelta(days=30)
                
                self._update_sync_status(db, 'sitoo', sync_in_progress=True, last_error=None)
                
                try:
                    orders = self.sitoo.get_detailed_orders(from_date=from_date, limit=500)
                    self._save_sales_orders(db, orders)
                    results['sitoo'] = len(orders)
                    
                    # Update status
                    max_date = max([o['order_date'] for o in orders if o.get('order_date')], default=None)
                    self._update_sync_status(db, 'sitoo',
                        sync_in_progress=False,
                        last_incremental_sync=datetime.now(),
                        last_order_date=max_date or sitoo_status.last_order_date,
                        last_sync_orders_count=len(orders)
                    )
                    logger.info(f"Incremental sync: {len(orders)} orders from Sitoo")
                except Exception as e:
                    self._update_sync_status(db, 'sitoo', sync_in_progress=False, last_error=str(e))
                    logger.error(f"Sitoo incremental sync error: {e}")
            
            # Sync Shopify
            if self.shopify.authenticate():
                shopify_status = self._get_sync_status(db, 'shopify')
                
                # Determine start date
                from_date = shopify_status.last_order_date
                if from_date:
                    from_date = from_date - timedelta(days=1)
                else:
                    from_date = datetime.now() - timedelta(days=30)
                
                self._update_sync_status(db, 'shopify', sync_in_progress=True, last_error=None)
                
                try:
                    orders = self.shopify.get_detailed_orders(from_date=from_date, limit=250)
                    self._save_sales_orders(db, orders)
                    results['shopify'] = len(orders)
                    
                    max_date = max([o['order_date'] for o in orders if o.get('order_date')], default=None)
                    self._update_sync_status(db, 'shopify',
                        sync_in_progress=False,
                        last_incremental_sync=datetime.now(),
                        last_order_date=max_date or shopify_status.last_order_date,
                        last_sync_orders_count=len(orders)
                    )
                    logger.info(f"Incremental sync: {len(orders)} orders from Shopify")
                except Exception as e:
                    self._update_sync_status(db, 'shopify', sync_in_progress=False, last_error=str(e))
                    logger.error(f"Shopify incremental sync error: {e}")
            
            logger.info(f"Incremental sync complete: Sitoo={results['sitoo']}, Shopify={results['shopify']}")
            return results
            
        finally:
            db.close()
    
    # ============== FULL HISTORICAL SYNC ==============
    
    def sync_full_history(self, max_orders: int = None, progress_callback: Callable = None):
        """
        Full historical sync - fetches ALL orders with pagination.
        Use this for initial data load or rebuilding history.
        This can take a while for large order histories.
        """
        logger.info("Starting full historical sync...")
        
        db = SessionLocal()
        try:
            results = {'sitoo': 0, 'shopify': 0}
            
            # Sync Sitoo
            if self.sitoo.authenticate():
                self._update_sync_status(db, 'sitoo', sync_in_progress=True, last_error=None)
                
                try:
                    logger.info("Fetching all Sitoo orders (this may take a while)...")
                    orders = self.sitoo.get_all_detailed_orders(
                        max_orders=max_orders,
                        progress_callback=progress_callback
                    )
                    
                    # Save in batches to avoid memory issues
                    batch_size = 500
                    for i in range(0, len(orders), batch_size):
                        batch = orders[i:i+batch_size]
                        self._save_sales_orders(db, batch)
                        logger.info(f"Saved Sitoo batch {i+len(batch)}/{len(orders)}")
                    
                    results['sitoo'] = len(orders)
                    
                    # Get total count from DB
                    total = db.query(SalesOrder).filter(SalesOrder.source_system == 'sitoo').count()
                    max_date = db.query(SalesOrder.order_date).filter(
                        SalesOrder.source_system == 'sitoo'
                    ).order_by(SalesOrder.order_date.desc()).first()
                    
                    self._update_sync_status(db, 'sitoo',
                        sync_in_progress=False,
                        last_full_sync=datetime.now(),
                        last_order_date=max_date[0] if max_date else None,
                        total_orders_synced=total,
                        last_sync_orders_count=len(orders)
                    )
                    logger.info(f"Full sync: {len(orders)} orders from Sitoo")
                    
                except Exception as e:
                    self._update_sync_status(db, 'sitoo', sync_in_progress=False, last_error=str(e))
                    logger.error(f"Sitoo full sync error: {e}")
            
            # Sync Shopify
            if self.shopify.authenticate():
                self._update_sync_status(db, 'shopify', sync_in_progress=True, last_error=None)
                
                try:
                    logger.info("Fetching all Shopify orders (this may take a while)...")
                    orders = self.shopify.get_all_detailed_orders(
                        max_orders=max_orders,
                        progress_callback=progress_callback
                    )
                    
                    # Save in batches
                    batch_size = 500
                    for i in range(0, len(orders), batch_size):
                        batch = orders[i:i+batch_size]
                        self._save_sales_orders(db, batch)
                        logger.info(f"Saved Shopify batch {i+len(batch)}/{len(orders)}")
                    
                    results['shopify'] = len(orders)
                    
                    total = db.query(SalesOrder).filter(SalesOrder.source_system == 'shopify').count()
                    max_date = db.query(SalesOrder.order_date).filter(
                        SalesOrder.source_system == 'shopify'
                    ).order_by(SalesOrder.order_date.desc()).first()
                    
                    self._update_sync_status(db, 'shopify',
                        sync_in_progress=False,
                        last_full_sync=datetime.now(),
                        last_order_date=max_date[0] if max_date else None,
                        total_orders_synced=total,
                        last_sync_orders_count=len(orders)
                    )
                    logger.info(f"Full sync: {len(orders)} orders from Shopify")
                    
                except Exception as e:
                    self._update_sync_status(db, 'shopify', sync_in_progress=False, last_error=str(e))
                    logger.error(f"Shopify full sync error: {e}")
            
            logger.info(f"Full historical sync complete: Sitoo={results['sitoo']}, Shopify={results['shopify']}")
            return results
            
        finally:
            db.close()
    
    # ============== LEGACY METHODS (for backward compatibility) ==============
    
    def sync_today(self):
        """Sync today's sales data"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.sync_from_date(today)
    
    def sync_from_date(self, from_date: datetime, to_date: datetime = None):
        """Sync sales data from a specific date"""
        if to_date is None:
            to_date = datetime.now()
        
        logger.info(f"Syncing sales data from {from_date} to {to_date}")
        
        db = SessionLocal()
        try:
            # Sync from Sitoo
            if self.sitoo.authenticate():
                sitoo_orders = self.sitoo.get_detailed_orders(from_date, to_date)
                self._save_sales_orders(db, sitoo_orders)
                logger.info(f"Synced {len(sitoo_orders)} orders from Sitoo")
            else:
                logger.warning("Failed to authenticate with Sitoo")
            
            # Sync from Shopify
            if self.shopify.authenticate():
                shopify_orders = self.shopify.get_detailed_orders(from_date, to_date)
                self._save_sales_orders(db, shopify_orders)
                logger.info(f"Synced {len(shopify_orders)} orders from Shopify")
            else:
                logger.warning("Failed to authenticate with Shopify")
            
            logger.info("Sales sync completed")
            
        except Exception as e:
            logger.error(f"Sales sync failed: {e}")
            db.rollback()
            raise
        finally:
            db.close()
    
    def sync_all(self, limit: int = 1000):
        """Sync all available sales data (legacy - use sync_full_history instead)"""
        logger.info(f"Syncing all sales data (limit: {limit})")
        
        db = SessionLocal()
        try:
            # Sync from Sitoo
            if self.sitoo.authenticate():
                sitoo_orders = self.sitoo.get_detailed_orders(limit=limit)
                self._save_sales_orders(db, sitoo_orders)
                logger.info(f"Synced {len(sitoo_orders)} orders from Sitoo")
            
            # Sync from Shopify
            if self.shopify.authenticate():
                shopify_orders = self.shopify.get_detailed_orders(limit=min(limit, 250))
                self._save_sales_orders(db, shopify_orders)
                logger.info(f"Synced {len(shopify_orders)} orders from Shopify")
            
            logger.info("Full sales sync completed")
            
        except Exception as e:
            logger.error(f"Sales sync failed: {e}")
            db.rollback()
            raise
        finally:
            db.close()
    
    def _load_mappings(self, db: Session) -> Dict[str, Any]:
        """Load all mapping tables into memory for fast lookup"""
        mappings = {
            'staff_by_userid': {},
            'staff_by_extid': {},
            'categories': {},
            'existing_skus': set()
        }
        
        # Load staff mappings
        try:
            staff_records = db.query(StaffMapping).all()
            for staff in staff_records:
                if staff.staff_userid:
                    mappings['staff_by_userid'][staff.staff_userid] = staff.full_name
                if staff.staff_externalid:
                    mappings['staff_by_extid'][staff.staff_externalid] = staff.full_name
            logger.info(f"Loaded {len(staff_records)} staff mappings")
        except Exception as e:
            logger.warning(f"Could not load staff mappings: {e}")
        
        # Load category mappings
        try:
            category_records = db.query(CategoryMapping).all()
            for cat in category_records:
                if cat.sku:
                    mappings['categories'][cat.sku] = cat.standard_category
                    mappings['existing_skus'].add(cat.sku)
            logger.info(f"Loaded {len(category_records)} category mappings")
        except Exception as e:
            logger.warning(f"Could not load category mappings: {e}")
        
        return mappings
    
    def _create_sku_mappings(self, db: Session, orders: List[Dict[str, Any]], existing_skus: set):
        """Create category and parent SKU mappings for new SKUs"""
        import re
        
        new_skus = {}
        
        # Collect new SKUs from orders
        for order in orders:
            source = order.get('source_system')
            for item in order.get('items', []):
                sku = item.get('sku')
                if sku and sku not in existing_skus and sku not in new_skus:
                    new_skus[sku] = {
                        'product_name': item.get('product_name'),
                        'product_category': item.get('product_category'),
                        'source': source
                    }
        
        if not new_skus:
            return
        
        logger.info(f"Creating mappings for {len(new_skus)} new SKUs")
        
        # Keyword rules for category inference (simplified)
        def infer_category(name, sku, original_cat):
            if not name:
                return original_cat or 'Uncategorized', 'original'
            
            name_lower = name.lower()
            sku_lower = sku.lower() if sku else ''
            
            # Check for vintage SKUs
            is_vintage = sku_lower.startswith(('ext-vn-', 'ext-vin-', 'vn-', 'vin-'))
            
            # Simple keyword matching
            keywords = [
                (r'skredder|tailor|repair', 'Services'),
                (r'\bjeans\b|\blevis\b|selvage|selvedge', 'Jeans'),
                (r'\bshirt\b', 'Shirt'),
                (r'\bjacket\b', 'Jacket'),
                (r'\bknit\b|\bsweater\b|\bcardigan\b', 'Knitwear'),
                (r'\bt-shirt\b|\btee\b', 'T-Shirt'),
                (r'\btrouser\b|\bpant\b|\bchino\b', 'Trouser'),
                (r'\bdress\b', 'Dress'),
                (r'\bskirt\b', 'Skirt'),
                (r'\bcoat\b', 'Coat'),
                (r'\bscarf\b', 'Scarf'),
                (r'\bsocks?\b', 'Socks'),
                (r'\bboot\b', 'Boots'),
                (r'\bshoe\b', 'Shoes'),
            ]
            
            for pattern, category in keywords:
                if re.search(pattern, name_lower):
                    if is_vintage:
                        return f'Vintage {category}', 'keyword_inference'
                    return category, 'keyword_inference'
            
            if is_vintage:
                return 'Vintage Other', 'keyword_inference'
            
            return original_cat or 'Uncategorized', 'original'
        
        # Size extraction for parent SKU
        def extract_parent_sku(sku):
            if not sku:
                return sku, None, None
            patterns = [
                (r'^(.+)-(\d{4})$', 'denim'),
                (r'^(.+)-(XXS|XS|S|M|L|XL|XXL|2XL|3XL)$', 'letter'),
                (r'^(.+)-(OS)$', 'one_size'),
                (r'^(.+)-(\d{1,2})$', 'numeric'),
            ]
            for pattern, size_type in patterns:
                match = re.match(pattern, sku, re.IGNORECASE)
                if match:
                    return match.group(1), match.group(2).upper(), size_type
            return sku, None, None
        
        # Create mappings
        for sku, info in new_skus.items():
            try:
                # Category mapping
                category, source = infer_category(info['product_name'], sku, info['product_category'])
                cat_mapping = CategoryMapping(
                    sku=sku,
                    original_category=info['product_category'],
                    product_name=info['product_name'],
                    standard_category=category,
                    mapping_source=source if info['source'] != 'shopify' else 'shopify',
                    confidence=1.0 if info['source'] == 'shopify' else 0.7
                )
                db.add(cat_mapping)
                
                # Parent SKU mapping
                parent_sku, size_code, size_type = extract_parent_sku(sku)
                parent_mapping = ParentSkuMapping(
                    sku=sku,
                    parent_sku=parent_sku,
                    size_code=size_code,
                    size_type=size_type,
                    product_name=info['product_name'],
                    base_product_name=info['product_name'],  # Simplified
                    variant_count=1
                )
                db.add(parent_mapping)
                
            except Exception as e:
                logger.warning(f"Could not create mapping for SKU {sku}: {e}")
                continue
        
        db.commit()
        logger.info(f"Created mappings for {len(new_skus)} new SKUs")
    
    def _enrich_order(self, order_data: Dict[str, Any], mappings: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich order data with staff names, categories, and standardized vendors from mappings"""
        
        # Enrich staff name for Sitoo orders
        if order_data.get('source_system') == 'sitoo':
            staff_name = None
            
            # Try to get name from staff_userid (GUID) first
            staff_userid = order_data.get('staff_userid')
            if staff_userid and staff_userid in mappings['staff_by_userid']:
                staff_name = mappings['staff_by_userid'][staff_userid]
            
            # Fall back to staff_id (external ID)
            if not staff_name:
                staff_id = order_data.get('staff_id')
                if staff_id and staff_id in mappings['staff_by_extid']:
                    staff_name = mappings['staff_by_extid'][staff_id]
                elif staff_id:
                    staff_name = f"Staff {staff_id}"
            
            order_data['staff_name'] = staff_name
        
        # Enrich item categories and standardize vendors
        for item in order_data.get('items', []):
            sku = item.get('sku')
            if sku and sku in mappings['categories']:
                item['product_category'] = mappings['categories'][sku]
            
            # Standardize vendor name
            if item.get('vendor'):
                item['vendor'] = standardize_vendor(item['vendor'])
        
        return order_data
    
    def _save_sales_orders(self, db: Session, orders: List[Dict[str, Any]]):
        """Save sales orders to database with enrichment from mapping tables"""
        
        # Load mappings once for the batch
        mappings = self._load_mappings(db)
        
        # Create mappings for any new SKUs first
        self._create_sku_mappings(db, orders, mappings['existing_skus'])
        
        # Reload mappings to include newly created ones
        mappings = self._load_mappings(db)
        
        for order_data in orders:
            try:
                # Enrich order with staff names and categories
                order_data = self._enrich_order(order_data, mappings)
                
                # Check if order exists
                existing = db.query(SalesOrder).filter(
                    and_(
                        SalesOrder.source_system == order_data['source_system'],
                        SalesOrder.source_id == order_data['source_id']
                    )
                ).first()
                
                if existing:
                    # Update existing order
                    for key, value in order_data.items():
                        if key not in ('items', 'refunds') and hasattr(existing, key):
                            setattr(existing, key, value)

                    # Update items - delete old and add new
                    db.query(SalesOrderItem).filter(
                        SalesOrderItem.order_id == existing.id
                    ).delete()

                    for item_data in order_data.get('items', []):
                        item = SalesOrderItem(order_id=existing.id, **item_data)
                        db.add(item)

                    # Upsert refunds
                    for refund_data in order_data.get('refunds', []):
                        existing_refund = db.query(SalesRefund).filter(
                            SalesRefund.source_system == refund_data['source_system'],
                            SalesRefund.source_id == refund_data['source_id']
                        ).first()
                        if not existing_refund:
                            refund = SalesRefund(order_id=existing.id, **refund_data)
                            db.add(refund)
                else:
                    # Create new order
                    items_data = order_data.pop('items', [])
                    refunds_data = order_data.pop('refunds', [])
                    order = SalesOrder(**order_data)
                    db.add(order)
                    db.flush()  # Get the order ID

                    for item_data in items_data:
                        item = SalesOrderItem(order_id=order.id, **item_data)
                        db.add(item)

                    for refund_data in refunds_data:
                        refund = SalesRefund(order_id=order.id, **refund_data)
                        db.add(refund)
                
            except Exception as e:
                logger.error(f"Error saving order {order_data.get('source_id')}: {e}")
                continue
        
        db.commit()


def get_sync_status_summary() -> Dict[str, Any]:
    """Get sync status for all sources"""
    db = SessionLocal()
    try:
        statuses = db.query(SyncStatus).all()
        return {
            status.source_system: {
                'last_full_sync': status.last_full_sync.isoformat() if status.last_full_sync else None,
                'last_incremental_sync': status.last_incremental_sync.isoformat() if status.last_incremental_sync else None,
                'last_order_date': status.last_order_date.isoformat() if status.last_order_date else None,
                'total_orders_synced': status.total_orders_synced,
                'last_sync_orders_count': status.last_sync_orders_count,
                'sync_in_progress': status.sync_in_progress,
                'last_error': status.last_error
            }
            for status in statuses
        }
    finally:
        db.close()
