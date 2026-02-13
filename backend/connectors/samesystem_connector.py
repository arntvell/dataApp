"""
SameSystem connector for staff/budget management data.
Auth: OAuth2 per department (ctx_token in URL path), token has 1h TTL.
API docs: https://samesystemapiv1.docs.apiary.io/
"""

import logging
import time
import requests
from typing import Dict, List, Any, Optional
from datetime import date
from connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

BASE_URL = "https://api.samesystem.com/api/v1"
TOKEN_TTL = 3300  # 55 minutes (token valid 1h, refresh early)


class SameSystemConnector(BaseConnector):
    """Connector for SameSystem budget and worktime APIs"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._email = config.get("email", "")
        self._password = config.get("password", "")
        self._departments: Dict[str, str] = config.get("departments", {})
        # Token cache per ctx_token: {ctx_token: (access_token, timestamp)}
        self._tokens: Dict[str, tuple] = {}

    # ---- Auth (OAuth2 per department) ----

    def _ensure_token(self, ctx_token: str):
        """Get or refresh OAuth2 token for a specific department ctx_token"""
        cached = self._tokens.get(ctx_token)
        if cached and (time.time() - cached[1]) < TOKEN_TTL:
            return

        self.logger.info(f"Fetching OAuth2 token for ctx {ctx_token[:8]}...")
        resp = requests.post(
            f"{BASE_URL}/{ctx_token}/oauth/token",
            json={
                "username": self._email,
                "password": self._password,
                "grant_type": "password",
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        access_token = data.get("access_token")
        if not access_token:
            raise ValueError(f"SameSystem OAuth did not return access_token for ctx {ctx_token[:8]}")
        self._tokens[ctx_token] = (access_token, time.time())
        self.logger.info(f"SameSystem token refreshed for ctx {ctx_token[:8]}")

    def _headers(self, ctx_token: str) -> Dict[str, str]:
        self._ensure_token(ctx_token)
        token = self._tokens[ctx_token][0]
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def authenticate(self) -> bool:
        """Test authentication by getting a token for the first department"""
        if not self._departments:
            self.logger.error("No SameSystem departments configured")
            return False
        try:
            first_ctx = next(iter(self._departments.values()))
            self._ensure_token(first_ctx)
            return True
        except Exception as e:
            self.logger.error(f"SameSystem auth failed: {e}")
            return False

    @property
    def departments(self) -> Dict[str, str]:
        return self._departments

    # ---- Budget endpoints ----

    def get_daily_sales_budgets(self, ctx_token: str, year: int, month: int) -> List[Dict[str, Any]]:
        """GET /{ctx_token}/budget/daily/{year}/{month}"""
        resp = requests.get(
            f"{BASE_URL}/{ctx_token}/budget/daily/{year}/{month:02d}",
            headers=self._headers(ctx_token),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("daily_budgets", data.get("data", [])) if isinstance(data, dict) else data

    def get_daily_salary_budgets(self, ctx_token: str, year: int, month: int) -> List[Dict[str, Any]]:
        """GET /{ctx_token}/salary_budget/daily/{year}/{month}"""
        resp = requests.get(
            f"{BASE_URL}/{ctx_token}/salary_budget/daily/{year}/{month:02d}",
            headers=self._headers(ctx_token),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("daily_salary_budgets", data.get("data", [])) if isinstance(data, dict) else data

    def get_monthly_sales_budgets(self, ctx_token: str, year: int) -> List[Dict[str, Any]]:
        """GET /{ctx_token}/budget/monthly/{year}"""
        resp = requests.get(
            f"{BASE_URL}/{ctx_token}/budget/monthly/{year}",
            headers=self._headers(ctx_token),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("monthly_budgets", data.get("data", [])) if isinstance(data, dict) else data

    def get_monthly_salary_budgets(self, ctx_token: str, year: int) -> List[Dict[str, Any]]:
        """GET /{ctx_token}/salary_budget/monthly/{year}"""
        resp = requests.get(
            f"{BASE_URL}/{ctx_token}/salary_budget/monthly/{year}",
            headers=self._headers(ctx_token),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("monthly_salary_budgets", data.get("data", [])) if isinstance(data, dict) else data

    # ---- Worktime / Calendar export endpoints ----

    def get_worktime_export(self, ctx_token: str, from_date: date, to_date: date) -> List[Dict[str, Any]]:
        """GET /{ctx_token}/export/calendar with salary data"""
        resp = requests.get(
            f"{BASE_URL}/{ctx_token}/export/calendar",
            headers=self._headers(ctx_token),
            params={
                "start_date": from_date.isoformat(),
                "end_date": to_date.isoformat(),
                "salary": "true",
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])

    def get_salary_export(self, ctx_token: str, from_date: date, to_date: date) -> List[Dict[str, Any]]:
        """GET /{ctx_token}/export/calendar with salary + bonuses"""
        resp = requests.get(
            f"{BASE_URL}/{ctx_token}/export/calendar",
            headers=self._headers(ctx_token),
            params={
                "start_date": from_date.isoformat(),
                "end_date": to_date.isoformat(),
                "salary": "true",
                "bonuses": "true",
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])

    # ---- BaseConnector stubs (not applicable for SameSystem) ----

    def get_products(self) -> List[Dict[str, Any]]:
        return []

    def get_customers(self) -> List[Dict[str, Any]]:
        return []

    def get_orders(self) -> List[Dict[str, Any]]:
        return []

    def get_inventory(self) -> List[Dict[str, Any]]:
        return []
