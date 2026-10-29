"""
connectors/overtime.py - Overtime Markets V2 (Thales Protocol on Arbitrum/Optimism)
Decentralized Sports Betting AMM & Orderbook Connector.
"""
import asyncio
from decimal import Decimal
import logging
from typing import Dict, List, Optional
import httpx

logger = logging.getLogger("OvertimeConnector")

class OvertimeConnector:
    BASE_URL = "https://overtimemarketsv2.xyz"
    
    def __init__(self, network_id: int = 10):  # 10 = Optimism, 42161 = Arbitrum
        self.network_id = network_id
        self.cached_markets: List[dict] = []
        self.last_fetch_ts: float = 0.0

    async def fetch_active_sports_odds(self, limit: int = 50) -> List[dict]:
        """Fetch active sports markets, lines, and normalized implied odds."""
        endpoint = f"{self.BASE_URL}/api/v2/markets"
        params = {"networkId": self.network_id}
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.get(endpoint, params=params)
                if res.status_code == 200:
                    data = res.json()
                    markets_list = data if isinstance(data, list) else data.get("markets", [])
                    parsed = []
                    for m in markets_list[:limit]:
                        home = m.get("homeTeam", "Home")
                        away = m.get("awayTeam", "Away")
                        odds = m.get("odds", [])
                        if len(odds) >= 2:
                            parsed.append({
                                "id": f"OVERTIME_{m.get('address', home[:6])}",
                                "title": f"[Overtime V2] {home} vs {away}",
                                "sport": m.get("sport", "Soccer"),
                                "home_odds": Decimal(str(odds[0].get("decimal", 2.0))),
                                "away_odds": Decimal(str(odds[1].get("decimal", 2.0))),
                                "platform": "Overtime Markets (Arbitrum/Optimism)",
                                "liquidity_usd": Decimal(str(m.get("liquidity", 150.0)))
                            })
                    self.cached_markets = parsed
                    logger.info(f"Fetched {len(parsed)} active markets from Overtime Markets.")
                    return parsed
        except Exception as e:
            logger.debug(f"Overtime fetch tick: {e}")
        return self.cached_markets
