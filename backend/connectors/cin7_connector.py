"""
Cin7 Core (DEAR Inventory) connector for ERP/inventory data.
Base URL: https://inventory.dearsystems.com/ExternalApi/v2
Auth: api-auth-accountid + api-auth-applicationkey headers
Rate limit: 55 req/min (safety margin on 60)
"""

import logging
import time
import requests
from collections import deque
from typing import Dict, List, Any, Optional
from datetime import datetime
from connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
RATE_LIMIT = 55  # requests per minute (safety margin on 60)


class Cin7Connector(BaseConnector):
    """Connector for Cin7 Core REST API"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._account_id = config.get("account_id", "")
        self._api_key = config.get("api_key", "")
        self._request_times: deque = deque()

    # ---- Auth ----

    def _headers(self) -> Dict[str, str]:
        return {
            "api-auth-accountid": self._account_id,
            "api-auth-applicationkey": self._api_key,
            "Content-Type": "application/json",
        }

    def authenticate(self) -> bool:
        """Test connection by fetching first page of product availability"""
        try:
            resp = self._get(f"{BASE_URL}/ref/productavailability", params={"Page": 1, "Limit": 1})
            data = resp.json()
            return "ProductAvailabilityList" in data
        except Exception as e:
            self.logger.error(f"Cin7 auth failed: {e}")
            return False

    # ---- Rate limiter ----

    def _wait_for_rate_limit(self):
        """Simple deque-based rate limiter: max RATE_LIMIT requests per 60s"""
        now = time.time()
        while self._request_times and self._request_times[0] < now - 60:
            self._request_times.popleft()
        if len(self._request_times) >= RATE_LIMIT:
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                self.logger.debug(f"Rate limit: sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self._request_times.append(time.time())

    def _get(self, url: str, params: Dict[str, Any] = None, timeout: int = 30) -> requests.Response:
        """GET with rate limiting and automatic retry on 429"""
        max_retries = 3
        for attempt in range(max_retries):
            self._wait_for_rate_limit()
            resp = requests.get(url, headers=self._headers(), params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = 62  # wait just over 1 minute
                self.logger.warning(f"429 rate limited, waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                self._request_times.clear()
                continue
            resp.raise_for_status()
            return resp
        resp.raise_for_status()
        return resp

    # ---- Generic pagination ----

    def _paginate(self, endpoint: str, list_key: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Paginate through a Cin7 list endpoint.

        Args:
            endpoint: API path (e.g. 'saleList')
            list_key: JSON key containing the list (e.g. 'SaleList')
            params: Extra query params
        """
        all_results: List[Dict[str, Any]] = []
        page = 1
        page_size = 100
        base_params = params or {}

        while True:
            req_params = {**base_params, "Page": page, "Limit": page_size}
            resp = self._get(f"{BASE_URL}/{endpoint}", params=req_params, timeout=60)
            data = resp.json()

            rows = data.get(list_key, [])
            if not rows:
                break

            all_results.extend(rows)
            self.logger.info(f"Cin7 {endpoint} page {page}: {len(rows)} rows (total {len(all_results)})")

            total = data.get("Total", 0)
            if len(all_results) >= total or len(rows) < page_size:
                break
            page += 1

        return all_results

    # ---- Stock ----

    def get_product_availability(self) -> List[Dict[str, Any]]:
        """Fetch full stock snapshot (all products, all locations)"""
        return self._paginate("ref/productavailability", "ProductAvailabilityList")

    def get_locations(self) -> List[Dict[str, Any]]:
        """Fetch warehouse/location list"""
        resp = self._get(f"{BASE_URL}/ref/location")
        return resp.json().get("LocationList", [])

    # ---- Sales (wholesale) ----

    def get_sale_list(self, from_date: datetime = None) -> List[Dict[str, Any]]:
        """Fetch sale order list, optionally since from_date"""
        params = {}
        if from_date:
            params["UpdatedSince"] = from_date.strftime("%Y-%m-%dT%H:%M:%S")
        return self._paginate("saleList", "SaleList", params)

    def get_sale_detail(self, sale_id: str) -> Dict[str, Any]:
        """Fetch single sale order with line items"""
        resp = self._get(f"{BASE_URL}/sale", params={"ID": sale_id})
        data = resp.json()
        # Flatten: line items and total are nested under Order
        order = data.get("Order", {})
        data["Lines"] = order.get("Lines", [])
        if not data.get("Total"):
            data["Total"] = order.get("Total", 0)
        # Map SaleOrderDate → OrderDate for pipeline compatibility
        if not data.get("OrderDate"):
            data["OrderDate"] = data.get("SaleOrderDate")
        return data

    # ---- Purchases ----

    def get_purchase_list(self, from_date: datetime = None) -> List[Dict[str, Any]]:
        """Fetch purchase order list, optionally since from_date"""
        params = {}
        if from_date:
            params["UpdatedSince"] = from_date.strftime("%Y-%m-%dT%H:%M:%S")
        return self._paginate("purchaseList", "PurchaseList", params)

    def get_purchase_detail(self, purchase_id: str) -> Dict[str, Any]:
        """Fetch single purchase order with line items (advanced-purchase endpoint)"""
        resp = self._get(f"{BASE_URL}/advanced-purchase", params={"ID": purchase_id})
        data = resp.json()
        # Flatten: line items are nested under Order.Lines
        order = data.get("Order", {})
        data["Lines"] = order.get("Lines", [])
        if not data.get("Total"):
            data["Total"] = order.get("Total", 0)
        return data

    # ---- BaseConnector stubs ----

    def get_products(self) -> List[Dict[str, Any]]:
        return self._paginate("product", "Products")

    def get_customers(self) -> List[Dict[str, Any]]:
        return self._paginate("customer", "CustomerList")

    def get_orders(self) -> List[Dict[str, Any]]:
        return self.get_sale_list()

    def get_inventory(self) -> List[Dict[str, Any]]:
        return self.get_product_availability()
