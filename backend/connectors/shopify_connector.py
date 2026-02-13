import requests
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from .base_connector import BaseConnector
import logging

logger = logging.getLogger(__name__)

class ShopifyConnector(BaseConnector):
    """Connector for Shopify e-commerce using GraphQL Admin API"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('base_url')
        self.api_key = config.get('api_key')
        self.api_version = "2025-07"  # Latest stable version
        
        # GraphQL endpoint
        self.graphql_url = f"{self.base_url}/admin/api/{self.api_version}/graphql.json"
        
        # Headers for GraphQL requests
        self.headers = {
            'X-Shopify-Access-Token': self.api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def _make_graphql_request(self, query: str, variables: Dict = None) -> Dict:
        """Make a GraphQL request to Shopify"""
        try:
            payload = {
                'query': query,
                'variables': variables or {}
            }
            
            response = requests.post(
                self.graphql_url,
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"GraphQL request failed: {response.status_code} - {response.text}")
                return {'errors': [{'message': f'HTTP {response.status_code}'}]}
                
        except Exception as e:
            self.logger.error(f"GraphQL request error: {e}")
            return {'errors': [{'message': str(e)}]}
    
    def authenticate(self) -> bool:
        """Test API connection"""
        try:
            query = """
            {
                shop {
                    id
                    name
                    email
                    myshopifyDomain
                }
            }
            """
            
            result = self._make_graphql_request(query)
            
            if 'errors' not in result and 'data' in result:
                shop_data = result['data']['shop']
                self.logger.info(f"Connected to Shopify store: {shop_data.get('name', 'Unknown')}")
                return True
            else:
                errors = result.get('errors', [])
                for error in errors:
                    self.logger.error(f"Shopify authentication error: {error.get('message', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def get_products(self) -> List[Dict[str, Any]]:
        """Retrieve products from Shopify"""
        try:
            query = """
            {
                products(first: 250) {
                    edges {
                        node {
                            id
                            title
                            description
                            handle
                            status
                            variants(first: 10) {
                                edges {
                                    node {
                                        id
                                        sku
                                        price
                                        inventoryQuantity
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
            
            result = self._make_graphql_request(query)
            
            if 'errors' not in result and 'data' in result:
                products = result['data']['products']['edges']
                return self._transform_products(products)
            else:
                self.logger.error(f"Failed to get products: {result.get('errors', [])}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting products: {e}")
            return []
    
    def get_customers(self) -> List[Dict[str, Any]]:
        """Retrieve customers from Shopify"""
        try:
            query = """
            {
                customers(first: 250) {
                    edges {
                        node {
                            id
                            firstName
                            lastName
                            email
                            phone
                            createdAt
                        }
                    }
                }
            }
            """
            
            result = self._make_graphql_request(query)
            
            if 'errors' not in result and 'data' in result:
                customers = result['data']['customers']['edges']
                return self._transform_customers(customers)
            else:
                self.logger.error(f"Failed to get customers: {result.get('errors', [])}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting customers: {e}")
            return []
    
    def get_orders(self) -> List[Dict[str, Any]]:
        """Retrieve orders from Shopify"""
        try:
            query = """
            {
                orders(first: 250) {
                    edges {
                        node {
                            id
                            name
                            email
                            totalPriceSet {
                                shopMoney {
                                    amount
                                }
                            }
                            displayFinancialStatus
                            displayFulfillmentStatus
                            customer {
                                id
                            }
                            createdAt
                        }
                    }
                }
            }
            """
            
            result = self._make_graphql_request(query)
            
            if 'errors' not in result and 'data' in result:
                orders = result['data']['orders']['edges']
                return self._transform_orders(orders)
            else:
                self.logger.error(f"Failed to get orders: {result.get('errors', [])}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting orders: {e}")
            return []
    
    def get_inventory(self) -> List[Dict[str, Any]]:
        """Retrieve inventory from Shopify via locations"""
        try:
            query = """
            {
                locations(first: 10) {
                    edges {
                        node {
                            id
                            name
                            inventoryLevels(first: 250) {
                                edges {
                                    node {
                                        id
                                        quantities(names: ["available"]) {
                                            name
                                            quantity
                                        }
                                        item {
                                            id
                                            variant {
                                                id
                                                product {
                                                    id
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
            
            result = self._make_graphql_request(query)
            
            if 'errors' not in result and 'data' in result:
                locations = result['data']['locations']['edges']
                return self._transform_inventory(locations)
            else:
                self.logger.error(f"Failed to get inventory: {result.get('errors', [])}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting inventory: {e}")
            return []
    
    def get_pages(self, query_filter: str = None) -> List[Dict[str, Any]]:
        """Retrieve pages from Shopify using REST API, optionally filtered by query"""
        try:
            # Use REST API endpoint instead of GraphQL
            rest_url = f"{self.base_url}/admin/api/{self.api_version}/pages.json"
            
            # Set up parameters
            params = {'limit': 250}
            
            response = requests.get(
                rest_url,
                headers={'X-Shopify-Access-Token': self.api_key},
                params=params
            )
            
            if response.status_code == 200:
                all_pages = response.json().get('pages', [])
                
                # Filter pages if query_filter is provided
                if query_filter:
                    filtered_pages = [
                        page for page in all_pages
                        if query_filter.lower() in (page.get('title') or '').lower() 
                        or query_filter.lower() in (page.get('body_html') or '').lower()
                    ]
                    return filtered_pages
                
                return all_pages
            else:
                self.logger.error(f"Failed to get pages: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting pages: {e}")
            return []
    
    def _transform_products(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Shopify product data to unified format"""
        transformed = []
        for product_edge in products:
            product = product_edge['node']
            variants = product.get('variants', {}).get('edges', [])
            
            if variants:
                variant = variants[0]['node']
                transformed.append({
                    'sku': variant.get('sku'),
                    'name': product.get('title'),
                    'description': product.get('description'),
                    'price': float(variant.get('price', 0)),
                    'cost': 0,  # Shopify doesn't provide cost by default
                    'inventory_quantity': variant.get('inventoryQuantity', 0),
                    'source_system': 'shopify',
                    'source_id': str(product.get('id')).split('/')[-1]  # Extract ID from GraphQL ID
                })
        
        return transformed
    
    def _transform_customers(self, customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Shopify customer data to unified format"""
        transformed = []
        for customer_edge in customers:
            customer = customer_edge['node']
            transformed.append({
                'email': customer.get('email'),
                'first_name': customer.get('firstName'),
                'last_name': customer.get('lastName'),
                'phone': customer.get('phone'),
                'source_system': 'shopify',
                'source_id': str(customer.get('id')).split('/')[-1]  # Extract ID from GraphQL ID
            })
        return transformed
    
    def _transform_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Shopify order data to unified format"""
        transformed = []
        for order_edge in orders:
            order = order_edge['node']
            customer = order.get('customer') or {}
            
            # Get total price from nested structure
            total_price_set = order.get('totalPriceSet', {})
            shop_money = total_price_set.get('shopMoney', {})
            total_amount = float(shop_money.get('amount', 0))
            
            transformed.append({
                'order_number': order.get('name'),
                'customer_id': str(customer.get('id')).split('/')[-1] if customer.get('id') else None,
                'total_amount': total_amount,
                'status': order.get('displayFinancialStatus'),
                'source_system': 'shopify',
                'source_id': str(order.get('id')).split('/')[-1]  # Extract ID from GraphQL ID
            })
        return transformed
    
    def _transform_inventory(self, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Shopify inventory data to unified format"""
        transformed = []
        for location_edge in locations:
            location = location_edge['node']
            location_name = location.get('name', 'Unknown')
            
            inventory_levels = location.get('inventoryLevels', {}).get('edges', [])
            for level_edge in inventory_levels:
                level = level_edge['node']
                item = level.get('item', {})
                variant = item.get('variant') or {}
                product = variant.get('product') or {}
                
                # Get available quantity from quantities array
                quantities = level.get('quantities', [])
                available = 0
                for q in quantities:
                    if q.get('name') == 'available':
                        available = q.get('quantity', 0)
                        break
                
                transformed.append({
                    'product_id': str(product.get('id')).split('/')[-1] if product.get('id') else None,
                    'location': location_name,
                    'quantity': available,
                    'reserved_quantity': 0  # Shopify doesn't provide reserved quantity by default
                })
        return transformed
    
    # ============== SALES DASHBOARD METHODS ==============
    
    def get_detailed_orders(self, from_date: datetime = None, to_date: datetime = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve detailed orders for sales dashboard (single batch, most recent first)"""
        try:
            # Build date filter
            date_filter = ""
            if from_date:
                date_filter = f'created_at:>={from_date.strftime("%Y-%m-%d")}'
            if to_date:
                if date_filter:
                    date_filter += f' created_at:<={to_date.strftime("%Y-%m-%d")}'
                else:
                    date_filter = f'created_at:<={to_date.strftime("%Y-%m-%d")}'
            
            query_filter = f', query: "{date_filter}"' if date_filter else ''
            
            # Shopify limits to 250 per request
            fetch_limit = min(limit, 250)
            
            # Sort by created_at descending to get most recent first
            query = f"""
            {{
                orders(first: {fetch_limit}, sortKey: CREATED_AT, reverse: true{query_filter}) {{
                    edges {{
                        node {{
                            id
                            name
                            createdAt
                            displayFinancialStatus
                            returnStatus
                            totalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                            subtotalPriceSet {{ shopMoney {{ amount }} }}
                            totalDiscountsSet {{ shopMoney {{ amount }} }}
                            totalRefundedSet {{ shopMoney {{ amount }} }}
                            discountCodes
                            refunds {{
                                id
                                createdAt
                                totalRefundedSet {{ shopMoney {{ amount currencyCode }} }}
                            }}
                            customer {{
                                id
                                numberOfOrders
                            }}
                            channelInformation {{
                                channelDefinition {{ handle }}
                            }}
                            lineItems(first: 50) {{
                                edges {{
                                    node {{
                                        id
                                        name
                                        sku
                                        quantity
                                        originalUnitPriceSet {{ shopMoney {{ amount }} }}
                                        discountedUnitPriceSet {{ shopMoney {{ amount }} }}
                                        totalDiscountSet {{ shopMoney {{ amount }} }}
                                        product {{
                                            id
                                            productType
                                            vendor
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            """

            result = self._make_graphql_request(query)

            if 'errors' not in result and 'data' in result:
                orders = result['data']['orders']['edges']
                self.logger.info(f"Shopify returned {len(orders)} detailed orders")
                return self._transform_detailed_orders(orders)
            else:
                self.logger.error(f"Failed to get detailed orders: {result.get('errors', [])}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting detailed orders: {e}")
            return []
    
    def get_all_detailed_orders(self, from_date: datetime = None, to_date: datetime = None,
                                 batch_size: int = 250, max_orders: int = None,
                                 progress_callback=None) -> List[Dict[str, Any]]:
        """Retrieve ALL detailed orders with cursor-based pagination"""
        all_orders = []
        cursor = None
        has_next_page = True
        
        try:
            # Build date filter
            date_filter = ""
            if from_date:
                date_filter = f'created_at:>={from_date.strftime("%Y-%m-%d")}'
            if to_date:
                if date_filter:
                    date_filter += f' created_at:<={to_date.strftime("%Y-%m-%d")}'
                else:
                    date_filter = f'created_at:<={to_date.strftime("%Y-%m-%d")}'
            
            query_filter = f', query: "{date_filter}"' if date_filter else ''
            
            while has_next_page:
                # Build cursor param
                after_param = f', after: "{cursor}"' if cursor else ''
                
                query = f"""
                {{
                    orders(first: {batch_size}, sortKey: CREATED_AT{query_filter}{after_param}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        edges {{
                            node {{
                                id
                                name
                                createdAt
                                displayFinancialStatus
                                returnStatus
                                totalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                                subtotalPriceSet {{ shopMoney {{ amount }} }}
                                totalDiscountsSet {{ shopMoney {{ amount }} }}
                                totalRefundedSet {{ shopMoney {{ amount }} }}
                                discountCodes
                                refunds {{
                                    id
                                    createdAt
                                    totalRefundedSet {{ shopMoney {{ amount currencyCode }} }}
                                }}
                                customer {{
                                    id
                                    numberOfOrders
                                }}
                                channelInformation {{
                                    channelDefinition {{ handle }}
                                }}
                                lineItems(first: 50) {{
                                    edges {{
                                        node {{
                                            id
                                            name
                                            sku
                                            quantity
                                            originalUnitPriceSet {{ shopMoney {{ amount }} }}
                                            discountedUnitPriceSet {{ shopMoney {{ amount }} }}
                                            totalDiscountSet {{ shopMoney {{ amount }} }}
                                            product {{
                                                id
                                                productType
                                                vendor
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                """

                result = self._make_graphql_request(query)
                
                if 'errors' in result or 'data' not in result:
                    self.logger.error(f"Shopify pagination error: {result.get('errors', [])}")
                    break
                
                orders_data = result['data']['orders']
                page_info = orders_data.get('pageInfo', {})
                orders = orders_data.get('edges', [])
                
                if not orders:
                    break
                
                transformed = self._transform_detailed_orders(orders)
                all_orders.extend(transformed)
                
                # Progress callback
                if progress_callback:
                    progress_callback(len(all_orders), None, 'shopify')
                
                self.logger.info(f"Shopify: Fetched {len(all_orders)} orders so far")
                
                # Check limits
                if max_orders and len(all_orders) >= max_orders:
                    all_orders = all_orders[:max_orders]
                    break
                
                # Update pagination
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
                if not cursor:
                    break
            
            self.logger.info(f"Shopify: Total fetched {len(all_orders)} orders")
            return all_orders
            
        except Exception as e:
            self.logger.error(f"Error in paginated Shopify fetch: {e}")
            return all_orders
    
    def _transform_detailed_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Shopify orders to detailed sales format"""
        transformed = []
        
        for order_edge in orders:
            try:
                order = order_edge['node']
                customer = order.get('customer') or {}
                channel_info = order.get('channelInformation') or {}
                channel_def = channel_info.get('channelDefinition') or {}
                
                # Parse amounts
                # totalPriceSet = Total sales (includes products, shipping, after discounts)
                # subtotalPriceSet = Product subtotal only (before shipping)
                # Note: Line items sum to subtotal, not totalPrice (difference is shipping)
                total_price = float(order.get('totalPriceSet', {}).get('shopMoney', {}).get('amount', 0))
                subtotal = float(order.get('subtotalPriceSet', {}).get('shopMoney', {}).get('amount', 0))
                total_discount = float(order.get('totalDiscountsSet', {}).get('shopMoney', {}).get('amount', 0))
                total_refunded = float(order.get('totalRefundedSet', {}).get('shopMoney', {}).get('amount', 0))
                currency = order.get('totalPriceSet', {}).get('shopMoney', {}).get('currencyCode', 'NOK')
                
                # Parse date
                created_at = order.get('createdAt')
                order_date = datetime.fromisoformat(created_at.replace('Z', '+00:00')) if created_at else None
                
                # Determine if new customer (1 order = new)
                num_orders = int(customer.get('numberOfOrders', 0)) if customer.get('numberOfOrders') else 0
                is_new_customer = num_orders <= 1 if customer.get('id') else None
                
                # Get channel/location
                channel = channel_def.get('handle', 'web')
                location = 'Online' if channel in ['web', 'online_store'] else channel.title()
                
                # Map status
                status = order.get('displayFinancialStatus', 'UNKNOWN')
                
                # Build items
                items = []
                for item_edge in order.get('lineItems', {}).get('edges', []):
                    item = item_edge['node']
                    product = item.get('product') or {}
                    
                    unit_price = float(item.get('originalUnitPriceSet', {}).get('shopMoney', {}).get('amount', 0))
                    discounted_price = float(item.get('discountedUnitPriceSet', {}).get('shopMoney', {}).get('amount', 0))
                    item_discount = float(item.get('totalDiscountSet', {}).get('shopMoney', {}).get('amount', 0))
                    quantity = item.get('quantity', 1)
                    
                    items.append({
                        'sku': item.get('sku'),
                        'product_name': item.get('name'),
                        'product_category': product.get('productType', 'Uncategorized'),
                        'vendor': product.get('vendor'),  # Product vendor/manufacturer
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'discount_amount': item_discount / quantity if quantity > 0 else 0,
                        'line_total': discounted_price * quantity,
                        'source_product_id': str(product.get('id')).split('/')[-1] if product.get('id') else None
                    })
                
                # Parse individual refunds with their own dates
                refunds = []
                for refund in order.get('refunds', []):
                    refund_amount = float(refund.get('totalRefundedSet', {}).get('shopMoney', {}).get('amount', 0))
                    refund_created = refund.get('createdAt')
                    refund_date = datetime.fromisoformat(refund_created.replace('Z', '+00:00')) if refund_created else order_date
                    refund_currency = refund.get('totalRefundedSet', {}).get('shopMoney', {}).get('currencyCode', currency)
                    if refund_amount > 0:
                        refunds.append({
                            'source_system': 'shopify',
                            'source_id': str(refund.get('id')).split('/')[-1],
                            'amount': refund_amount,
                            'currency': refund_currency,
                            'refund_date': refund_date,
                        })

                transformed.append({
                    'order_number': order.get('name'),
                    'source_system': 'shopify',
                    'source_id': str(order.get('id')).split('/')[-1],
                    'location': location,
                    'channel': channel,
                    'staff_id': None,  # Not applicable for e-commerce
                    'staff_name': None,
                    'subtotal': subtotal,
                    'total_discount': total_discount,
                    'total_amount': total_price,
                    'total_refunded': total_refunded,
                    'currency': currency,
                    'status': status,
                    'payment_method': 'Online',
                    'customer_source_id': str(customer.get('id')).split('/')[-1] if customer.get('id') else None,
                    'is_new_customer': is_new_customer,
                    'order_date': order_date,
                    'items': items,
                    'refunds': refunds,
                })
                
            except Exception as e:
                self.logger.error(f"Error transforming order: {e}")
                continue
        
        return transformed