from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Base class for all system connectors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the system"""
        pass
    
    @abstractmethod
    def get_products(self) -> List[Dict[str, Any]]:
        """Retrieve products from the system"""
        pass
    
    @abstractmethod
    def get_customers(self) -> List[Dict[str, Any]]:
        """Retrieve customers from the system"""
        pass
    
    @abstractmethod
    def get_orders(self) -> List[Dict[str, Any]]:
        """Retrieve orders from the system"""
        pass
    
    @abstractmethod
    def get_inventory(self) -> List[Dict[str, Any]]:
        """Retrieve inventory from the system"""
        pass
    
    def validate_response(self, response: Dict[str, Any]) -> bool:
        """Validate API response"""
        return response.get('status') == 'success' if 'status' in response else True
