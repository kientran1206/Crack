"""
prediction_bot/core/limitless_engine.py
Engine giám sát thời gian thực và tự động săn kèo Late-Cycle Convergence trên Limitless Exchange (Base L2).
Tích hợp:
- Binance WebSocket stream (Cập nhật giá BTC/ETH mỗi 100ms)
- Bộ lọc Buffer & Volatility an toàn cho tài khoản $10 USD
- Trình tạo chữ ký EIP-712 off-chain không tốn gas
"""

import asyncio
from decimal import Decimal
import json
import logging
import time
from typing import Callable, Dict, List, Optional
import websockets

try:
    from prediction_bot.connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal
except ImportError:
    from connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal

logger = logging.getLogger("limitless_engine")


class LimitlessArbitrageWatcher:
    """
    Watcher liên tục lắng nghe Binance WebSocket và đối soát với Orderbook Limitless.
    """
    BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/solusdt@ticker"

    def __init__(
        self,
        connector: LimitlessConnector,
        bankroll_usdc: Decimal = Decimal("10.00"),
        on_signal_callback: Optional[Callable[[LimitlessSignal], None]] = None
    ):
        self.connector = connector
        self.bankroll_usdc = bankroll_usdc
        self.on_signal_callback = on_signal_callback
        
        # Cache giá Spot thời gian thực
        self.spot_prices: Dict[str, Decimal] = {
            "BTC": Decimal("0"),
            "ETH": Decimal("0"),
            "SOL": Decimal("0")
        }
        self.is_running = False
        self._last_market_refresh = 0
        self._active_markets: List[LimitlessMarket] = []

    def update_spot_price(self, symbol: str, price: Decimal):
        """Cập nhật giá Spot từ Binance."""
        clean_symbol = symbol.replace("USDT", "").upper()
        if clean_symbol in self.spot_prices:
            self.spot_prices[clean_symbol] = price

    def refresh_markets_if_needed(self):
        """Làm mới danh sách thị trường mỗi 60 giây."""
        now = time.time()
        if now - self._last_market_refresh > 60:
            logger.info("Đang làm mới danh sách thị trường Limitless...")
            self._active_markets = self.connector.fetch_active_markets()
            self._last_market_refresh = now
            logger.info("Đã nạp %d thị trường từ Limitless.", len(self._active_markets))

    def scan_for_opportunities(self) -> List[LimitlessSignal]:
        """
        Quét toàn bộ thị trường đang mở và tính toán tín hiệu vào lệnh.
        """
        signals: List[LimitlessSignal] = []
        for market in self._active_markets:
            spot = self.spot_prices.get(market.target_asset, Decimal("0"))
            if spot <= Decimal("0"):
                continue

            sig = self.connector.evaluate_late_cycle_convergence(
                market=market,
                current_spot=spot,
                bankroll_usdc=self.bankroll_usdc
            )
            if sig.is_safe:
                signals.append(sig)
                if self.on_signal_callback:
                    self.on_signal_callback(sig)

        return signals

    async def run_binance_stream(self):
        """Lắng nghe WebSocket Binance để cập nhật giá tức thì từng tích tắc."""
        self.is_running = True
        logger.info("Bắt đầu kết nối Binance WebSocket: %s", self.BINANCE_WS_URL)
        
        while self.is_running:
            try:
                async with websockets.connect(self.BINANCE_WS_URL, ping_interval=20) as ws:
                    logger.info("Đã kết nối Binance WebSocket thành công.")
                    while self.is_running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        symbol = data.get("s", "")
                        price_str = data.get("c", "0")  # Last price
                        if symbol and price_str:
                            self.update_spot_price(symbol, Decimal(price_str))

                        # Định kỳ quét thị trường
                        self.refresh_markets_if_needed()
                        opportunities = self.scan_for_opportunities()
                        if opportunities:
                            for opp in opportunities:
                                logger.info(
                                    "[TÍN HIỆU NGON] Kèo: %s | Cửa: %s | Giá share: %s | Lãi dự kiến: +%.1f%% | Cược: $%s",
                                    opp.market_slug, opp.recommended_side, opp.share_price, opp.expected_roi_pct, opp.allocated_usd
                                )

            except Exception as e:
                logger.warning("Mất kết nối WebSocket Binance (%s), thử lại sau 3s...", e)
                await asyncio.sleep(3)

    def stop(self):
        self.is_running = False
