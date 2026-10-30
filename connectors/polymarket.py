"""
polymarket.py - Polymarket Market Discovery & High-Speed Batch Order Book Connector v3.0
Supports bulk POST /books endpoint (50 books per HTTP request) and 1,000+ Market Universe
"""
import asyncio
from decimal import Decimal
import json
import logging
import time
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
        """
        Fetch active real-world markets across ALL horizons (Near-Term, Mid-Term, Far-Dated)
        including Sports (Europa League, Champions League, Premier League, NBA) and World Events.
        Prioritizes Top 24h Volume Events to continuously capture live matchdays and in-play markets.
        """
        now = time.time()
        pairs = []

        try:
            # 1. Fetch Top 24h Volume Events (Brings today's live sports, NBA, EPL, LoL Worlds, etc.)
            events_url = f"{self.gamma_api}/events?closed=false&order=volume24hr&ascending=false&limit=60"
            resp = await self.client.get(events_url)
            if resp.status_code == 200:
                events_data = resp.json()
                for ev in events_data:
                    ev_title = ev.get("title", "")
                    ev_slug = ev.get("slug", "")
                    ev_end = ev.get("endDate", "") or ""
                    
                    # Compute Horizon Tag
                    horizon_tag = "[Kèo Dài Hạn / Futures]"
                    if ev_end:
                        try:
                            import datetime
                            ed = datetime.datetime.fromisoformat(ev_end.replace("Z", "+00:00"))
                            diff_days = (ed.timestamp() - now) / 86400.0
                            if diff_days <= 1.0:
                                horizon_tag = "[Hôm Nay / 24h]"
                            elif diff_days <= 7.0:
                                horizon_tag = f"[Tuần Này / {int(diff_days)} ngày]"
                            else:
                                horizon_tag = f"[Dài Hạn / {int(diff_days)} ngày]"
                        except Exception:
                            pass

                    # Extract sub-markets
                    for sm in ev.get("markets", []):
                        clob_ids = sm.get("clobTokenIds")
                        if isinstance(clob_ids, str):
                            try:
                                clob_ids = json.loads(clob_ids)
                            except Exception:
                                continue
                        
                        if clob_ids and len(clob_ids) >= 2:
                            sm_q = sm.get("question", "")
                            full_q = f"{ev_title}: {sm_q}" if ev_title not in sm_q else sm_q
                            pairs.append({
                                "id": sm.get("id"),
                                "question": full_q,
                                "slug": ev_slug or sm.get("slug"),
                                "url": f"https://polymarket.com/event/{ev_slug}",
                                "condition_id": sm.get("conditionId"),
                                "yes_token": str(clob_ids[0]),
                                "no_token": str(clob_ids[1]),
                                "volume": float(sm.get("volume", 0) or 0),
                                "end_date": ev_end,
                                "horizon_tag": horizon_tag
                            })

            # 2. Also fetch top standalone volume markets
            markets_url = f"{self.gamma_api}/markets?limit={limit}&active=true&closed=false&order=volume24hr&ascending=false"
            m_resp = await self.client.get(markets_url)
            if m_resp.status_code == 200:
                raw_m = m_resp.json()
                for m in raw_m:
                    clob_ids = m.get("clobTokenIds")
                    if isinstance(clob_ids, str):
                        try:
                            clob_ids = json.loads(clob_ids)
                        except Exception:
                            continue
                    
                    if clob_ids and len(clob_ids) >= 2:
                        end_date_str = m.get("endDate", "") or ""
                        horizon_tag = "[Kèo Dài Hạn / Futures]"
                        if end_date_str:
                            try:
                                import datetime
                                ed = datetime.datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                                diff_days = (ed.timestamp() - now) / 86400.0
                                if diff_days <= 1.0:
                                    horizon_tag = "[Hôm Nay / 24h]"
                                elif diff_days <= 7.0:
                                    horizon_tag = f"[Tuần Này / {int(diff_days)} ngày]"
                                else:
                                    horizon_tag = f"[Dài Hạn / {int(diff_days)} ngày]"
                            except Exception:
                                pass

                        pairs.append({
                            "id": m.get("id"),
                            "question": m.get("question"),
                            "slug": m.get("slug"),
                            "url": f"https://polymarket.com/event/{m.get('slug')}",
                            "condition_id": m.get("conditionId"),
                            "yes_token": str(clob_ids[0]),
                            "no_token": str(clob_ids[1]),
                            "volume": float(m.get("volume", 0) or 0),
                            "end_date": end_date_str,
                            "horizon_tag": horizon_tag
                        })

            # Deduplicate by condition_id / yes_token
            unique_pairs = []
            seen_tokens = set()
            for p in pairs:
                if p["yes_token"] not in seen_tokens:
                    seen_tokens.add(p["yes_token"])
                    unique_pairs.append(p)

            unique_pairs.sort(key=lambda x: x["volume"], reverse=True)
            self.active_markets = unique_pairs
            logger.info(f"Loaded {len(self.active_markets)} active real markets across dynamic 24h horizons.")
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
