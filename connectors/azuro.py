"""
azuro.py - Azuro Protocol Decentralized Sports Liquidity Connector v3.0
Supports Grassroots / "Bóng Cỏ" Sports Coverage (Lower leagues, ITF, TT Cup) & Pre-Match Safety Layer
"""
import asyncio
from decimal import Decimal
import logging
import time
from typing import Dict, List, Optional
import httpx

logger = logging.getLogger("AzuroConnector")

ODDS_DECIMALS = Decimal("1000000000000")  # 10^12 precision

GRASSROOTS_KEYWORDS = [
    "u19", "u20", "u21", "u23", "regional", "amateur", "division 2", "division 3", 
    "division 4", "oberliga", "tercera", "itf", "tt cup", "liga pro", "challenger", 
    "youth", "reserve", "vietnam", "thailand", "indonesia", "cup", "friendly"
]

class AzuroConnector:
    def __init__(self, api_base: str):
        self.api_base = api_base
        self.client = httpx.AsyncClient(timeout=8.0, headers={"User-Agent": "Mozilla/5.0"})
        self.cached_games: List[Dict] = []
        self.grassroots_count: int = 0

    async def fetch_live_sports_odds(self, limit: int = 80) -> List[Dict]:
        """
        Fetch active sports games from Azuro feed, classifying Grassroots vs Tier-1 matches.
        """
        url = f"{self.api_base}/games?limit={limit}&status=Active"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                games = []
                raw_games = data if isinstance(data, list) else data.get("games", data.get("data", []))
                
                grassroots_hits = 0
                now = int(time.time())
                
                for g in raw_games:
                    title = f"{g.get('title', '')} ({g.get('sport', 'Sports')})"
                    league = str(g.get("league", "")).lower()
                    title_lower = title.lower()
                    
                    # Detect Grassroots ("Bóng cỏ / Siêu cỏ")
                    is_grassroots = any(kw in title_lower or kw in league for kw in GRASSROOTS_KEYWORDS)
                    if is_grassroots:
                        grassroots_hits += 1
                        
                    starts_at = g.get("startsAt", 0) or 0
                    is_pre_match = starts_at > now
                    
                    conditions = g.get("conditions", [])
                    for cond in conditions:
                        outcomes = cond.get("outcomes", [])
                        if len(outcomes) == 2:
                            raw1 = Decimal(str(outcomes[0].get("currentOdds", 0) or 0))
                            raw2 = Decimal(str(outcomes[1].get("currentOdds", 0) or 0))
                            
                            if raw1 > 0 and raw2 > 0:
                                dec1 = raw1 / ODDS_DECIMALS if raw1 > Decimal("1000") else raw1
                                dec2 = raw2 / ODDS_DECIMALS if raw2 > Decimal("1000") else raw2
                                
                                games.append({
                                    "game_id": g.get("gameId", g.get("id")),
                                    "title": title,
                                    "is_grassroots": is_grassroots,
                                    "is_pre_match": is_pre_match,
                                    "tier": "Bóng Cỏ (Grassroots)" if is_grassroots else "Bóng Chính (Major)",
                                    "outcome_1": outcomes[0].get("title", "Outcome 1"),
                                    "odds_1": dec1,
                                    "outcome_2": outcomes[1].get("title", "Outcome 2"),
                                    "odds_2": dec2,
                                    "starts_at": starts_at
                                })
                self.cached_games = games
                self.grassroots_count = grassroots_hits
                logger.info(f"Loaded {len(self.cached_games)} active sports odds lines from Azuro ({grassroots_hits} Grassroots / Bóng Cỏ).")
                return self.cached_games
        except Exception as e:
            logger.debug(f"Azuro feed fetch exception: {e}")
        return self.cached_games

    async def close(self):
        await self.client.aclose()
