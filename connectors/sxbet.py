"""
sxbet.py - SX Bet Decentralized P2P Sports Exchange Connector
"""
import asyncio
from decimal import Decimal
import logging
from typing import Dict, List, Optional
import httpx

logger = logging.getLogger("SXBetConnector")

class SXBetConnector:
    def __init__(self, api_base: str = "https://api.sx.bet"):
        self.api_base = api_base
        self.client = httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "Mozilla/5.0"})
        self.cached_markets: List[Dict] = []

    async def fetch_active_markets(self, limit: int = 30) -> List[Dict]:
        """Fetch active sports betting markets from SX Bet."""
        url = f"{self.api_base}/markets/active"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                raw = resp.json()
                data = raw.get("data", {})
                markets = data.get("markets", [])
                parsed = []
                for m in markets[:limit]:
                    title = f"{m.get('teamOneName', '')} vs {m.get('teamTwoName', '')} ({m.get('sportLabel', 'Sports')})"
                    o1_name = m.get("outcomeOneName", "Outcome 1")
                    o2_name = m.get("outcomeTwoName", "Outcome 2")
                    market_hash = m.get("marketHash")
                    
                    parsed.append({
                        "market_hash": market_hash,
                        "title": title if len(title) > 8 else m.get("outcomeVoidName", "SX Sports Match"),
                        "outcome_1": o1_name,
                        "outcome_2": o2_name,
                        "sport": m.get("sportLabel"),
                        "league": m.get("leagueLabel")
                    })
                self.cached_markets = parsed
                logger.info(f"Loaded {len(self.cached_markets)} active markets from SX Bet.")
                return self.cached_markets
        except Exception as e:
            logger.debug(f"SX Bet fetch error: {e}")
        return self.cached_markets

    async def get_best_odds(self, market_hash: str) -> Optional[Dict]:
        """Fetch best current orders / odds for a given market hash."""
        url = f"{self.api_base}/orders/best?marketHash={market_hash}"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                raw = resp.json()
                return raw.get("data")
        except Exception as e:
            logger.debug(f"SX Bet order error: {e}")
        return None

    async def close(self):
        await self.client.aclose()
