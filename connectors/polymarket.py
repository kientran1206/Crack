"""
polymarket.py - Polymarket Market Discovery & High-Speed Batch Order Book Connector v3.0
Supports bulk POST /books endpoint (50 books per HTTP request) and 1,000+ Market Universe
"""
import asyncio
from decimal import Decimal
import json
import logging
from typing import Dict, List, Optional
import httpx
from core.ledger import MarketDepth, OrderBookLevel

logger = logging.getLogger("PolymarketConnector")

class PolymarketConnector:
    def __init__(self, gamma_api: str, clob_rest: str):
        self.gamma_api = gamma_api
        self.clob_rest = clob_rest
        self.client = httpx.AsyncClient(
            timeout=8.0,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=50)
        )
        self.active_markets: List[Dict] = []

    async def refresh_active_markets(self, limit: int = 150) -> List[Dict]:
        """Fetch active binary markets with both YES and NO tokens from Gamma API."""
        url = f"{self.gamma_api}/markets?limit={limit}&active=true&closed=false"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                raw = resp.json()
                pairs = []
                for m in raw:
                    clob_ids = m.get("clobTokenIds")
                    if isinstance(clob_ids, str):
                        try:
                            clob_ids = json.loads(clob_ids)
                        except Exception:
                            continue
                    
                    if clob_ids and len(clob_ids) >= 2:
                        pairs.append({
                            "id": m.get("id"),
                            "question": m.get("question"),
                            "slug": m.get("slug"),
                            "condition_id": m.get("conditionId"),
                            "yes_token": str(clob_ids[0]),
                            "no_token": str(clob_ids[1]),
                            "volume": float(m.get("volume", 0) or 0)
                        })
                # Sort by volume descending for active trading interest
                pairs.sort(key=lambda x: x["volume"], reverse=True)
                self.active_markets = pairs
                logger.info(f"Loaded {len(self.active_markets)} active Polymarket binary conditions.")
                return self.active_markets
        except Exception as e:
            logger.error(f"Failed to refresh markets from Gamma API: {e}")
        return self.active_markets

    async def get_batch_books(self, token_ids: List[str]) -> Dict[str, MarketDepth]:
        """
        Batch fetch up to 50 token orderbooks in a single HTTP request (< 250ms).
        Endpoint: POST https://clob.polymarket.com/books
        """
        if not token_ids:
            return {}
            
        url = f"{self.clob_rest}/books"
        payload = [{"token_id": tid} for tid in token_ids[:50]]
        results: Dict[str, MarketDepth] = {}
        
        try:
            resp = await self.client.post(url, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                for b in data:
                    tid = str(b.get("asset_id", ""))
                    if not tid:
                        continue
                    asks = [
                        OrderBookLevel(price=Decimal(str(a["price"])), quantity=Decimal(str(a["size"])))
                        for a in b.get("asks", [])
                    ]
                    bids = [
                        OrderBookLevel(price=Decimal(str(bi["price"])), quantity=Decimal(str(bi["size"])))
                        for bi in b.get("bids", [])
                    ]
                    asks.sort(key=lambda x: x.price)
                    bids.sort(key=lambda x: x.price, reverse=True)
                    results[tid] = MarketDepth(token_id=tid, outcome="", bids=bids, asks=asks)
        except Exception as e:
            logger.debug(f"Batch books fetch exception: {e}")
            
        return results

    async def get_token_depth(self, token_id: str, outcome_name: str) -> Optional[MarketDepth]:
        """Fallback single orderbook snapshot for a specific token ID."""
        url = f"{self.clob_rest}/book?token_id={token_id}"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                asks = [
                    OrderBookLevel(price=Decimal(str(a["price"])), quantity=Decimal(str(a["size"])))
                    for a in data.get("asks", [])
                ]
                bids = [
                    OrderBookLevel(price=Decimal(str(b["price"])), quantity=Decimal(str(b["size"])))
                    for b in data.get("bids", [])
                ]
                asks.sort(key=lambda x: x.price)
                bids.sort(key=lambda x: x.price, reverse=True)
                return MarketDepth(token_id=token_id, outcome=outcome_name, bids=bids, asks=asks)
        except Exception as e:
            logger.debug(f"Failed to fetch depth for token {token_id[:8]}: {e}")
        return None

    async def close(self):
        await self.client.aclose()
