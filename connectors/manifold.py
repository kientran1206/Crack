"""
manifold.py - Manifold Markets Cross-Platform Connector
"""
import asyncio
from decimal import Decimal
import logging
from typing import Dict, List, Optional
import httpx

logger = logging.getLogger("ManifoldConnector")

class ManifoldConnector:
    def __init__(self, api_base: str = "https://api.manifold.markets/v0"):
        self.api_base = api_base
        self.client = httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "Mozilla/5.0"})
        self.cached_markets: List[Dict] = []

    async def fetch_active_binary_markets(self, limit: int = 40) -> List[Dict]:
        """Fetch active binary markets with probabilities."""
        url = f"{self.api_base}/markets?limit={limit}&sort=created-time&order=desc"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                raw = resp.json()
                parsed = []
                for m in raw:
                    if m.get("outcomeType") == "BINARY" and not m.get("isResolved"):
                        prob = Decimal(str(m.get("probability", 0.5)))
                        parsed.append({
                            "id": m.get("id"),
                            "question": m.get("question"),
                            "slug": m.get("slug"),
                            "prob": prob,
                            "volume": float(m.get("volume", 0) or 0)
                        })
                self.cached_markets = parsed
                logger.info(f"Loaded {len(self.cached_markets)} active binary markets from Manifold.")
                return self.cached_markets
        except Exception as e:
            logger.debug(f"Manifold fetch error: {e}")
        return self.cached_markets

    async def close(self):
        await self.client.aclose()
