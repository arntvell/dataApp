import requests
import base64
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from .base_connector import BaseConnector
import logging

logger = logging.getLogger(__name__)


class SitooRateLimitError(Exception):
    """Raised when Sitoo rate limiting cannot be recovered from after retries."""

class SitooConnector(BaseConnector):
    """Connector for Sitoo POS system"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('base_url', '').rstrip('/')  # Remove trailing slash
        self.username = config.get('api_id', '91622-165')  # Sitoo API ID from config
        self.api_key = config.get('api_key')
        
        # Create Basic Auth header
        credentials = base64.b64encode(f"{self.username}:{self.api_key}".encode()).decode()
        self.headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Cached lookups for manufacturers (lazy loaded)
        self._manufacturer_lookup = None  # {externalcompanyid: name}
        self._sku_manufacturer_lookup = None  # {sku: manufacturer_name}
    
    def _get_with_retry(self, url: str, max_retries: int = 8,
                        initial_backoff: float = 5.0,
                        max_backoff: float = 120.0) -> requests.Response:
        """
        GET with exponential backoff on HTTP 429 (rate limit).
        Honors Retry-After header if present, otherwise uses exponential
        backoff capped at max_backoff. Total wait time across the default
        8 retries is roughly 5+10+20+40+80+120+120+120 = ~8.5 minutes,
        enough to ride out most short rate-limit windows without giving up.
        Raises SitooRateLimitError if still rate-limited after max_retries.
        Non-429 responses are returned as-is for the caller to handle.
        """
        backoff = initial_backoff
        for attempt in range(max_retries + 1):
            response = requests.get(url, headers=self.headers)
            if response.status_code != 429:
                return response

            if attempt == max_retries:
                self.logger.error(
                    f"Sitoo rate limit not cleared after {max_retries} retries: {url}"
                )
                raise SitooRateLimitError(
                    f"Rate limited after {max_retries} retries on {url}"
                )

            retry_after = response.headers.get('Retry-After')
            try:
                wait = float(retry_after) if retry_after else backoff
            except ValueError:
                wait = backoff
            self.logger.warning(
                f"Sitoo 429 (attempt {attempt + 1}/{max_retries}); sleeping {wait:.1f}s"
            )
            time.sleep(wait)
            backoff = min(backoff * 2, max_backoff)

    def authenticate(self) -> bool:
        """Test API connection"""
        try:
            # Test with a simple endpoint
            response = requests.get(f"{self.base_url}/sites/1/productgroups.json", headers=self.headers)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def get_products(self) -> List[Dict[str, Any]]:
        """Retrieve products from Sitoo"""
        try:
            # Sitoo API supports pagination with start and num params
            response = requests.get(f"{self.base_url}/sites/1/products.json?num=500", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                self.logger.info(f"Sitoo returned {data.get('totalcount', 0)} total products, fetched {len(items)}")
                return self._transform_products(items)
            else:
                self.logger.error(f"Failed to get products: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting products: {e}")
            return []
    
    def get_customers(self) -> List[Dict[str, Any]]:
        """Retrieve customers from Sitoo"""
        try:
            response = requests.get(f"{self.base_url}/sites/1/customers.json?num=500", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                self.logger.info(f"Sitoo returned {data.get('totalcount', 0)} total customers, fetched {len(items)}")
                return self._transform_customers(items)
            else:
                self.logger.error(f"Failed to get customers: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting customers: {e}")
            return []
    
    def get_orders(self) -> List[Dict[str, Any]]:
        """Retrieve orders from Sitoo"""
        try:
            response = requests.get(f"{self.base_url}/sites/1/orders.json?num=500", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                self.logger.info(f"Sitoo returned {data.get('totalcount', 0)} total orders, fetched {len(items)}")
                return self._transform_orders(items)
            else:
                self.logger.error(f"Failed to get orders: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting orders: {e}")
            return []
    
    def get_inventory(self) -> List[Dict[str, Any]]:
        """Retrieve inventory from Sitoo via warehouses"""
        try:
            # Sitoo uses warehouses for inventory
            response = requests.get(f"{self.base_url}/sites/1/warehouses.json", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                self.logger.info(f"Sitoo returned {len(items)} warehouses")
                return self._transform_inventory(items)
            else:
                self.logger.error(f"Failed to get inventory: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting inventory: {e}")
            return []
    
    def _transform_products(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Sitoo product data to unified format"""
        transformed = []
        for product in data:
            # Parse price strings to floats
            price = float(product.get('moneyprice', '0').replace(',', '.'))
            cost = float(product.get('moneypricein', '0').replace(',', '.'))
            
            transformed.append({
                'sku': product.get('sku'),
                'name': product.get('title'),
                'description': product.get('description1', ''),  # Sitoo uses description1
                'price': price,
                'cost': cost,
                'inventory_quantity': 0,  # Stock is fetched separately via warehouses
                'source_system': 'sitoo',
                'source_id': str(product.get('productid'))
            })
        return transformed
    
    def _transform_customers(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Sitoo customer data to unified format"""
        transformed = []
        for customer in data:
            transformed.append({
                'email': customer.get('email'),
                'first_name': customer.get('namefirst'),
                'last_name': customer.get('namelast'),
                'phone': customer.get('mobile') or customer.get('phone'),
                'source_system': 'sitoo',
                'source_id': str(customer.get('customerid'))
            })
        return transformed
    
    def _transform_orders(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Sitoo order data to unified format"""
        transformed = []
        for order in data:
            # Map order state ID to status text
            state_map = {10: 'PENDING', 20: 'CONFIRMED', 30: 'PROCESSING', 40: 'COMPLETED', 50: 'CANCELLED'}
            status = state_map.get(order.get('orderstateid'), 'UNKNOWN')
            
            # Parse total amount
            total = float(order.get('moneytotal_gross_all', '0').replace(',', '.'))
            
            transformed.append({
                'order_number': str(order.get('orderid')),
                'customer_id': order.get('customerid'),  # May be None for POS orders
                'total_amount': total,
                'status': status,
                'source_system': 'sitoo',
                'source_id': str(order.get('orderid'))
            })
        return transformed
    
    def _transform_inventory(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Sitoo inventory data to unified format"""
        transformed = []
        for inventory in data:
            transformed.append({
                'product_id': inventory.get('productId'),
                'location': inventory.get('store'),
                'quantity': inventory.get('quantity'),
                'reserved_quantity': inventory.get('reserved', 0)
            })
        return transformed
    
    # ============== MANUFACTURER/VENDOR METHODS ==============
    
    def get_all_manufacturers(self) -> Dict[int, str]:
        """Fetch all manufacturers and return lookup dict {externalcompanyid: name}"""
        manufacturers = {}
        start = 0
        batch_size = 100
        
        try:
            while True:
                url = f"{self.base_url}/sites/1/manufacturers.json?start={start}&num={batch_size}"
                response = requests.get(url, headers=self.headers)
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to get manufacturers: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('items', [])
                
                if not items:
                    break
                
                for mfr in items:
                    mfr_id = mfr.get('externalcompanyid')
                    name = mfr.get('name')
                    if mfr_id and name:
                        manufacturers[mfr_id] = name
                
                self.logger.info(f"Fetched {len(manufacturers)} manufacturers so far")
                
                if len(items) < batch_size:
                    break
                start += batch_size
            
            self.logger.info(f"Total manufacturers loaded: {len(manufacturers)}")
            return manufacturers
            
        except Exception as e:
            self.logger.error(f"Error fetching manufacturers: {e}")
            return manufacturers
    
    def get_sku_manufacturer_lookup(self) -> Dict[str, str]:
        """Build SKU -> manufacturer name lookup from products"""
        if self._sku_manufacturer_lookup is not None:
            return self._sku_manufacturer_lookup
        
        # First get manufacturer lookup
        if self._manufacturer_lookup is None:
            self._manufacturer_lookup = self.get_all_manufacturers()
        
        sku_lookup = {}
        start = 0
        batch_size = 500
        
        try:
            while True:
                url = f"{self.base_url}/sites/1/products.json?start={start}&num={batch_size}"
                response = requests.get(url, headers=self.headers)
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to get products for vendor lookup: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('items', [])
                
                if not items:
                    break
                
                for product in items:
                    sku = product.get('sku')
                    mfr_id = product.get('manufacturerid')
                    if sku and mfr_id:
                        mfr_name = self._manufacturer_lookup.get(mfr_id)
                        if mfr_name:
                            sku_lookup[sku] = mfr_name
                
                self.logger.info(f"Built vendor lookup for {len(sku_lookup)} SKUs so far")
                
                if len(items) < batch_size:
                    break
                start += batch_size
            
            self._sku_manufacturer_lookup = sku_lookup
            self.logger.info(f"Total SKU->vendor mappings: {len(sku_lookup)}")
            return sku_lookup
            
        except Exception as e:
            self.logger.error(f"Error building SKU vendor lookup: {e}")
            self._sku_manufacturer_lookup = sku_lookup
            return sku_lookup
    
    # ============== SALES DASHBOARD METHODS ==============

    def get_order_count(self) -> int:
        """Get total order count with a single lightweight API call."""
        url = f"{self.base_url}/sites/1/orders.json?num=1"
        response = self._get_with_retry(url)
        if response.status_code == 200:
            return response.json().get('totalcount', 0)
        return 0

    def get_recent_orders(self, count: int, batch_size: int = 500) -> List[Dict[str, Any]]:
        """Fetch the N most recent orders.

        Sitoo returns orders newest-first (start=0 = most recent) and its
        date filter parameters are ignored, so the only reliable way to get
        recent orders is positional: fetch from start=0 up to *count*.

        For daily incremental sync, *count* should be the delta between the
        current API totalcount and the last-known total from SyncStatus,
        plus a small overlap buffer.
        """
        all_orders = []

        # Pre-load vendor lookup (shared across batches)
        self.logger.info("Pre-loading manufacturer/vendor lookup...")
        sku_vendor_lookup = self.get_sku_manufacturer_lookup()
        self.logger.info(f"Vendor lookup ready with {len(sku_vendor_lookup)} SKU mappings")

        start = 0
        remaining = count
        while remaining > 0:
            fetch = min(remaining, batch_size)
            url = f"{self.base_url}/sites/1/orders.json?num={fetch}&start={start}"
            response = self._get_with_retry(url)

            if response.status_code != 200:
                msg = f"Sitoo recent orders fetch failed at offset {start}: {response.status_code}"
                self.logger.error(msg)
                raise RuntimeError(msg)

            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            transformed = self._transform_detailed_orders(items, sku_vendor_lookup)
            all_orders.extend(transformed)
            self.logger.info(f"Sitoo recent: fetched {len(all_orders)}/{count} orders")

            if len(items) < fetch:
                break

            start += fetch
            remaining -= len(items)

            if remaining > 0:
                time.sleep(0.5)  # same throttle as paginated fetch

        return all_orders

    def get_detailed_orders(self, from_date: datetime = None, to_date: datetime = None, limit: int = 500) -> List[Dict[str, Any]]:
        """Retrieve detailed orders for sales dashboard (single batch)"""
        try:
            # Build query params
            params = f"num={limit}"
            
            # Add date filters if provided (Sitoo uses Unix timestamps)
            if from_date:
                params += f"&datelastmodified-from={int(from_date.timestamp())}"
            if to_date:
                params += f"&datelastmodified-to={int(to_date.timestamp())}"
            
            url = f"{self.base_url}/sites/1/orders.json?{params}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                self.logger.info(f"Sitoo returned {data.get('totalcount', 0)} orders, fetched {len(items)}")
                # Load vendor lookup for order items
                sku_vendor_lookup = self.get_sku_manufacturer_lookup()
                return self._transform_detailed_orders(items, sku_vendor_lookup)
            else:
                self.logger.error(f"Failed to get detailed orders: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting detailed orders: {e}")
            return []
    
    def get_all_detailed_orders(self, from_date: datetime = None, to_date: datetime = None,
                                 batch_size: int = 500, max_orders: int = None,
                                 progress_callback=None,
                                 inter_batch_sleep: float = 0.5) -> List[Dict[str, Any]]:
        """Retrieve ALL detailed orders with pagination"""
        all_orders = []
        start = 0
        total_count = None
        
        # Pre-load manufacturer lookup for vendor data
        self.logger.info("Pre-loading manufacturer/vendor lookup...")
        sku_vendor_lookup = self.get_sku_manufacturer_lookup()
        self.logger.info(f"Vendor lookup ready with {len(sku_vendor_lookup)} SKU mappings")
        
        # NOTE: do not catch-and-return-partial here. The pipeline advances a
        # high watermark from the returned set, so a partial fetch silently
        # creates a data gap. Let exceptions propagate to the caller.
        while True:
            # Build query params
            params = f"num={batch_size}&start={start}"

            if from_date:
                params += f"&datelastmodified-from={int(from_date.timestamp())}"
            if to_date:
                params += f"&datelastmodified-to={int(to_date.timestamp())}"

            url = f"{self.base_url}/sites/1/orders.json?{params}"
            response = self._get_with_retry(url)

            if response.status_code != 200:
                # Non-429 failure (e.g. 5xx). Raise rather than silently
                # truncate — the caller advances a high watermark based on
                # the returned set, so a partial fetch corrupts state.
                msg = (
                    f"Sitoo orders fetch failed at offset {start}: "
                    f"{response.status_code} {response.text[:200]}"
                )
                self.logger.error(msg)
                raise RuntimeError(msg)

            data = response.json()
            items = data.get('items', [])

            if total_count is None:
                total_count = data.get('totalcount', 0)
                self.logger.info(f"Sitoo has {total_count} total orders to fetch")

            if not items:
                break

            transformed = self._transform_detailed_orders(items, sku_vendor_lookup)
            all_orders.extend(transformed)

            # Progress callback
            if progress_callback:
                progress_callback(len(all_orders), total_count, 'sitoo')

            self.logger.info(f"Sitoo: Fetched {len(all_orders)}/{total_count} orders")

            # Check if we've reached the limit or end
            if max_orders and len(all_orders) >= max_orders:
                all_orders = all_orders[:max_orders]
                break

            if len(items) < batch_size:
                break  # No more items

            start += batch_size

            # Preventive throttle: stay under Sitoo's rate limit instead of
            # bursting through and getting blocked deep into the fetch.
            if inter_batch_sleep > 0:
                time.sleep(inter_batch_sleep)

        return all_orders
    
    def get_staff_list(self) -> Dict[str, str]:
        """Get mapping of staff IDs to names"""
        try:
            response = requests.get(f"{self.base_url}/sites/1/users.json", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                staff = {}
                for user in data.get('items', []):
                    ext_id = user.get('externalid')
                    name = f"{user.get('namefirst', '')} {user.get('namelast', '')}".strip()
                    if ext_id:
                        staff[ext_id] = name or f"Staff {ext_id}"
                return staff
            return {}
        except Exception as e:
            self.logger.error(f"Error getting staff list: {e}")
            return {}

    def get_users_by_userid(self) -> Dict[str, Dict[str, str]]:
        """Get full user list keyed by userid (GUID).

        Returns {userid: {'name': ..., 'externalid': ...}} for every
        Sitoo user, so the sync pipeline can auto-create staff_mappings
        for newly added POS users.
        """
        try:
            response = self._get_with_retry(
                f"{self.base_url}/sites/1/users.json?num=500"
            )
            if response.status_code != 200:
                self.logger.error(f"Failed to get users: {response.status_code}")
                return {}
            users = {}
            for user in response.json().get('items', []):
                uid = user.get('userid', '')
                name = f"{user.get('namefirst', '')} {user.get('namelast', '')}".strip()
                users[uid] = {
                    'name': name or f"User {uid[:8]}",
                    'externalid': user.get('externalid', ''),
                }
            return users
        except Exception as e:
            self.logger.error(f"Error getting users by userid: {e}")
            return {}
    
    def _transform_detailed_orders(self, orders: List[Dict[str, Any]], 
                                     sku_vendor_lookup: Dict[str, str] = None) -> List[Dict[str, Any]]:
        """Transform Sitoo orders to detailed sales format"""
        if sku_vendor_lookup is None:
            sku_vendor_lookup = {}
        
        transformed = []
        for order in orders:
            try:
                # Extract additional data
                additional = order.get('additionaldata', {})
                
                # Parse amounts
                subtotal = float(order.get('moneyfinal_net', '0').replace(',', '.'))
                total = float(order.get('moneytotal_gross_all', '0').replace(',', '.'))
                
                # Get staff info - store both externalid and userid (GUID)
                # The pipeline will look up the actual name from staff_mappings table
                staff_externalid = additional.get('pos-staff-externalid')
                staff_userid = additional.get('pos-staff-userid')  # GUID for mapping
                
                # Get payment method from payments array
                payments = order.get('payments', [])
                payment_method = payments[0].get('name', 'Unknown') if payments else 'Unknown'
                card_issuer = payments[0].get('cardissuer') if payments else None
                if card_issuer:
                    payment_method = f"{payment_method} ({card_issuer})"
                
                # Parse order date
                order_timestamp = order.get('orderdate')
                order_date = datetime.fromtimestamp(order_timestamp) if order_timestamp else None
                
                # Map status
                state_map = {10: 'PENDING', 20: 'PAID', 30: 'PROCESSING', 40: 'COMPLETED', 50: 'CANCELLED'}
                status = state_map.get(order.get('orderstateid'), 'UNKNOWN')
                
                # Build order items - category will be enriched by pipeline
                items = []
                for item in order.get('orderitems', []):
                    unit_price = float(item.get('moneyoriginalprice', '0').replace(',', '.'))
                    discount = float(item.get('moneydiscount', '0').replace(',', '.'))
                    quantity = item.get('quantity', 1)
                    # Use actual net + vat values from API (not manual calculation)
                    net_total = float(item.get('moneyitemtotal_net', '0').replace(',', '.'))
                    vat_total = float(item.get('moneyitemtotal_vat', '0').replace(',', '.'))
                    line_total = net_total + vat_total  # Gross = Net + VAT
                    
                    # Get vendor from SKU lookup (manufacturers API)
                    sku = item.get('sku')
                    vendor = sku_vendor_lookup.get(sku) if sku else None
                    
                    # Fallback to additional data if not in lookup
                    if not vendor:
                        item_additional = item.get('additionaldata', {})
                        vendor = item_additional.get('manufacturer-name') or item_additional.get('supplier-name')
                    
                    items.append({
                        'sku': sku,
                        'product_name': item.get('productname'),
                        'product_category': item.get('additionaldata', {}).get('product-group-name', 'Standard'),
                        'vendor': vendor,  # From manufacturers lookup or additional data
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'discount_amount': discount,
                        'line_total': line_total,
                        'source_product_id': str(item.get('productid'))
                    })
                
                transformed.append({
                    'order_number': str(order.get('orderid')),
                    'source_system': 'sitoo',
                    'source_id': str(order.get('orderid')),
                    'location': additional.get('store-name', 'Unknown Store'),
                    'channel': order.get('checkouttypename', 'POS'),
                    'staff_id': staff_externalid,
                    'staff_userid': staff_userid,  # Store GUID for future mapping
                    'staff_name': None,  # Will be enriched by pipeline
                    'subtotal': subtotal,
                    'total_discount': 0,  # Calculate from items if needed
                    'total_amount': total,
                    'total_refunded': 0,
                    'currency': order.get('currencycode', 'NOK'),
                    'status': status,
                    'payment_method': payment_method,
                    'customer_source_id': str(order.get('customerid')) if order.get('customerid') else None,
                    'is_new_customer': None,  # Not available from Sitoo
                    'order_date': order_date,
                    'items': items
                })
            except Exception as e:
                self.logger.error(f"Error transforming order {order.get('orderid')}: {e}")
                continue
        
        return transformed