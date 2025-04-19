from abc import ABC, abstractmethod
from typing import List, Dict

class BranchCrawler(ABC):
    chain: str                  # e.g. 'cooponline'

    @abstractmethod
    async def crawl_branches(self) -> List[Dict]:
        """Return list of dicts: {
            id, chain, name, address, phone,
            district, ward, lat, lon
        }"""
        pass
    async def crawl_prices(self) -> List[Dict]:
        """Return list of dicts: {
            id, chain, sku, price
        }"""
        pass