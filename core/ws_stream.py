"""
ws_stream.py - Ultra-Fast Push-Based WebSocket Client for Polymarket CLOB (< 1ms In-Memory Latency)
Maintains zero-polling, lock-free in-memory orderbooks and performs microsecond Dutching evaluation.
"""
import asyncio
from decimal import Decimal
import json
import logging
import time
from typing import Dict, List, Optional, Callable

import websockets

logger = logging.getLogger("PushWebSocket")

WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

class PolymarketWebSocketStream:
    def __init__(self, on_opportunity_callback: Optional[Callable] = None):
        self.ws_url = WS_MARKET_URL
        self.on_opportunity_callback = on_opportunity_callback
        self.is_running: bool = False
        self.subscribed_token_ids: List[str] = []
        self.market_map: Dict[str, dict] = {}  # token_id -> market metadata
        self.orderbooks: Dict[str, dict] = {}  # token_id -> {"best_bid": Decimal, "best_ask": Decimal, "ask_size": Decimal}
        
        # Telemetry
        self.messages_received: int = 0
        self.last_in_memory_latency_us: float = 0.0
        self.last_in_memory_latency_ms: float = 0.0
        self.connected: bool = False
        self.last_heartbeat: float = time.time()
        self._ws = None

    def register_markets(self, markets: List[dict]):
        """Register active market pairs to watch via push stream."""
        new_tokens = []
        for m in markets:
            y_tok = m.get("yes_token")
            n_tok = m.get("no_token")
            if y_tok and n_tok:
                self.market_map[y_tok] = m
                self.market_map[n_tok] = m
                if y_tok not in self.subscribed_token_ids:
                    new_tokens.append(y_tok)
                    self.subscribed_token_ids.append(y_tok)
                if n_tok not in self.subscribed_token_ids:
                    new_tokens.append(n_tok)
                    self.subscribed_token_ids.append(n_tok)
        
        logger.info(f"Registered {len(self.subscribed_token_ids)} tokens across {len(markets)} markets for push streaming.")
        return new_tokens

    async def start(self):
        """Main resilient WebSocket streaming loop with auto-reconnect."""
        self.is_running = True
        logger.info(f"Starting Polymarket Push-WebSocket client to {self.ws_url}...")

        backoff = 1.0
        while self.is_running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10_000_000
                ) as ws:
                    self._ws = ws
                    self.connected = True
                    backoff = 1.0
                    logger.info("Connected to Polymarket CLOB WebSocket server. Subscribing to market channels...")

                    # Subscribe in chunks of 50 tokens
                    chunk_size = 50
                    for i in range(0, len(self.subscribed_token_ids), chunk_size):
                        chunk = self.subscribed_token_ids[i:i + chunk_size]
                        sub_msg = {
                            "type": "market",
                            "assets_ids": chunk
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.05)

                    logger.info(f"Successfully subscribed {len(self.subscribed_token_ids)} tokens to push feed.")

                    # Event processing loop
                    async for raw_message in ws:
                        self.messages_received += 1
                        self.last_heartbeat = time.time()
                        self._process_ws_frame(raw_message)

            except asyncio.CancelledError:
                logger.info("WebSocket worker cancelled.")
                break
            except Exception as e:
                self.connected = False
                logger.warning(f"WebSocket connection dropped: {e}. Reconnecting in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 15.0)

    def _process_ws_frame(self, raw_message: str):
        """Process incoming delta frame in RAM with zero locks (< 50 microseconds)."""
        t0 = time.perf_counter()
        try:
            data = json.loads(raw_message)
            if not isinstance(data, dict):
                return

            event_type = data.get("event_type")
            asset_id = str(data.get("asset_id", ""))

            # Handle full book snapshot or price update
            if event_type == "book":
                asks = data.get("asks", [])
                bids = data.get("bids", [])
                best_ask = Decimal(str(asks[0]["price"])) if asks else None
                ask_size = Decimal(str(asks[0]["size"])) if asks else Decimal("0")
                best_bid = Decimal(str(bids[0]["price"])) if bids else None
                self.orderbooks[asset_id] = {
                    "best_ask": best_ask,
                    "ask_size": ask_size,
                    "best_bid": best_bid,
                    "updated_at": time.time()
                }
            elif event_type == "price_change":
                price = Decimal(str(data.get("price", "0")))
                side = data.get("side", "").upper()
                size = Decimal(str(data.get("size", "0")))
                entry = self.orderbooks.setdefault(asset_id, {"best_ask": None, "ask_size": Decimal("0"), "best_bid": None})
                if side == "SELL":
                    entry["best_ask"] = price
                    entry["ask_size"] = size
                elif side == "BUY":
                    entry["best_bid"] = price
                entry["updated_at"] = time.time()

            # Immediate In-Memory Dutching Check for paired YES/NO
            market = self.market_map.get(asset_id)
            if market:
                y_tok = market.get("yes_token")
                n_tok = market.get("no_token")
                y_book = self.orderbooks.get(y_tok)
                n_book = self.orderbooks.get(n_tok)

                if y_book and n_book and y_book["best_ask"] and n_book["best_ask"]:
                    bundle_cost = y_book["best_ask"] + n_book["best_ask"]
                    if bundle_cost < Decimal("1.00"):
                        # Sub-millisecond arb detected!
                        spread = Decimal("1.00") - bundle_cost
                        if self.on_opportunity_callback:
                            self.on_opportunity_callback(market, y_book, n_book, spread)

        except Exception as e:
            logger.debug(f"Frame parsing error: {e}")
        finally:
            elapsed_us = (time.perf_counter() - t0) * 1_000_000
            self.last_in_memory_latency_us = elapsed_us
            self.last_in_memory_latency_ms = round(elapsed_us / 1000.0, 3)

    def get_status(self) -> dict:
        return {
            "connected": self.connected,
            "subscribed_tokens": len(self.subscribed_token_ids),
            "cached_orderbooks": len(self.orderbooks),
            "messages_received": self.messages_received,
            "in_memory_compute_latency_us": round(self.last_in_memory_latency_us, 2),
            "in_memory_compute_latency_ms": self.last_in_memory_latency_ms
        }
