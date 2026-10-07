"""
Loom connector for stock levels by location.
Base URL: configured (LOOM_BASE_URL), e.g. https://loom.livid.no
Auth: Authorization: Bearer <token>, scope `stock:read`
Rate limit: 240 req/min per token

Read-only. Loom replaces Cin7 Core as the source of stock levels; it does not
carry prices, costs, purchase orders or wholesale documents.
"""

import logging
import time
import requests
from collections import deque
from typing import Dict, List, Any, Optional
from connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

API_PREFIX = "/api/partner/v1/stock"
RATE_LIMIT = 200        # requests per minute (safety margin on Loom's 240)
PAGE_LIMIT = 5000       # Loom's documented maximum


class LoomConnector(BaseConnector):
    """Connector for the Loom partner stock API"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._base = (config.get("base_url") or "").rstrip("/")
        self._api_key = config.get("api_key", "")
        self._request_times: deque = deque()

    # ---- Auth ----

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }

    def authenticate(self) -> bool:
        """Test the token by fetching the location list (cheap, unpaginated)."""
        if not self._base or not self._api_key:
            self.logger.error("Loom not configured (LOOM_BASE_URL / LOOM_API_KEY missing)")
            return False
        try:
            body = self._get("locations")
            return bool(body.get("ok")) and "locations" in body
        except Exception as e:
            self.logger.error(f"Loom auth failed: {e}")
            return False

    # ---- Rate limiter ----

    def _wait_for_rate_limit(self):
        now = time.time()
        while self._request_times and self._request_times[0] < now - 60:
            self._request_times.popleft()
        if len(self._request_times) >= RATE_LIMIT:
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                self.logger.debug(f"Rate limit: sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self._request_times.append(time.time())

    def _get(self, path: str, params: Dict[str, Any] = None, timeout: int = 90) -> Dict[str, Any]:
        """GET a Loom stock endpoint and return the decoded body.

        Loom answers errors as {"ok": false, "error": "..."} with a 4xx/5xx status.
        4xx means the request itself is wrong and retrying will not help, so we
        surface the server's message rather than a bare HTTPError.
        """
        self._wait_for_rate_limit()
        url = f"{self._base}{API_PREFIX}/{path}"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=timeout)
        if resp.status_code >= 400:
            try:
                msg = resp.json().get("error", resp.text[:200])
            except ValueError:
                msg = resp.text[:200]
            raise RuntimeError(f"Loom {path} -> {resp.status_code}: {msg}")
        return resp.json()

    # ---- Stock ----

    def get_locations(self) -> List[Dict[str, Any]]:
        """Every stock location: location_id, name, source (pio/sitoo/shopify/virtual)."""
        return self._get("locations").get("locations", [])

    def get_stock_levels(self, updated_since: Optional[str] = None) -> Dict[str, Any]:
        """Full snapshot of stock levels, following the keyset cursor to the last page.

        Returns {"as_of": <ISO ts from the first page>, "rows": [...]}. Loom's guide
        recommends treating the full snapshot as the source of truth; `updated_since`
        is accepted for ad-hoc delta pulls but deltas never report deleted rows, so
        the scheduled sync always takes a full snapshot.
        """
        rows: List[Dict[str, Any]] = []
        cursor, as_of, pages = None, None, 0

        while True:
            params = {"limit": PAGE_LIMIT}
            if cursor:
                params["cursor"] = cursor
            if updated_since:
                params["updated_since"] = updated_since

            body = self._get("levels", params=params)
            as_of = as_of or body.get("asOf")
            page_rows = body.get("rows", [])
            rows.extend(page_rows)
            pages += 1
            self.logger.info(f"Loom levels page {pages}: {len(page_rows)} rows (total {len(rows)})")

            cursor = body.get("nextCursor")
            if not cursor:
                break

        return {"as_of": as_of, "rows": rows}

    # ---- BaseConnector contract (Loom only serves stock) ----

    def get_products(self) -> List[Dict[str, Any]]:
        return []

    def get_customers(self) -> List[Dict[str, Any]]:
        return []

    def get_orders(self) -> List[Dict[str, Any]]:
        return []

    def get_inventory(self) -> List[Dict[str, Any]]:
        return self.get_stock_levels()["rows"]
