import os
import json
from dotenv import load_dotenv
from typing import Dict, Any

load_dotenv()

class Settings:
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/dataapp")

    # API Settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "DataApp"

    # Sitoo Configuration
    SITOO_BASE_URL: str = os.getenv("SITOO_BASE_URL", "")
    SITOO_API_ID: str = os.getenv("SITOO_API_ID", "91622-165")
    SITOO_API_KEY: str = os.getenv("SITOO_API_KEY", "")

    # Shopify Configuration
    SHOPIFY_BASE_URL: str = os.getenv("SHOPIFY_BASE_URL", "")
    SHOPIFY_API_KEY: str = os.getenv("SHOPIFY_API_KEY", "")
    SHOPIFY_PASSWORD: str = os.getenv("SHOPIFY_PASSWORD", "")

    # SameSystem Configuration
    SAMESYSTEM_EMAIL: str = os.getenv("SAMESYSTEM_EMAIL", "")
    SAMESYSTEM_PASSWORD: str = os.getenv("SAMESYSTEM_PASSWORD", "")

    SAMESYSTEM_DEPARTMENTS: Dict[str, str] = json.loads(os.getenv("SAMESYSTEM_DEPARTMENTS", "{}")) if os.getenv("SAMESYSTEM_DEPARTMENTS", "{}").strip().startswith("{") else {}

    # Cin7 Core Configuration
    CIN7_ACCOUNT_ID: str = os.getenv("CIN7_ACCOUNT_ID", "")
    CIN7_API_KEY: str = os.getenv("CIN7_API_KEY", "")

    # Scheduler
    SCHEDULER_ENABLED: bool = os.getenv("SCHEDULER_ENABLED", "false").lower() in ("true", "1", "yes")

    # Redis Configuration
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

    def get_connector_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configuration for all connectors"""
        return {
            "sitoo": {
                "base_url": self.SITOO_BASE_URL,
                "api_id": self.SITOO_API_ID,
                "api_key": self.SITOO_API_KEY
            },
            "shopify": {
                "base_url": self.SHOPIFY_BASE_URL,
                "api_key": self.SHOPIFY_API_KEY,
                "password": self.SHOPIFY_PASSWORD
            },
            "samesystem": {
                "email": self.SAMESYSTEM_EMAIL,
                "password": self.SAMESYSTEM_PASSWORD,
                "departments": self.SAMESYSTEM_DEPARTMENTS
            },
            "cin7": {
                "account_id": self.CIN7_ACCOUNT_ID,
                "api_key": self.CIN7_API_KEY
            }
        }

settings = Settings()
