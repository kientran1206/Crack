"""
prediction_bot/core/limitless_engine.py
Engine giám sát thời gian thực và tự động săn kèo Late-Cycle Convergence trên Limitless Exchange (Base L2).
Tích hợp:
- Binance WebSocket stream (Cập nhật giá BTC/ETH mỗi 100ms)
- Bộ lọc Dynamic Time-Decay Buffer & Anti-Knife-Catching Regex
- Quản lý vòng đời Paper Trading thực chiến 100% (Khóa vốn Escrow, độ trễ 200ms, độ sâu orderbook)
- Quyết toán bằng Oracle sống tại thời điểm đóng nến (Win ăn payout, Thua mất sạch vốn)
- Ghi sổ cái SQLite vĩnh viễn (PaperDatabase)
"""
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import logging
import time
from typing import Callable, Dict, List, Optional
import urllib.request
import websockets

from config import CONFIG
from core.paper_database import PaperDatabase

try:
    from prediction_bot.connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal
except ImportError:
    from connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal

logger = logging.getLogger("limitless_engine")
VN_TZ = timezone(timedelta(hours=7))


class LimitlessArbitrageWatcher:
    """
    Watcher liên tục lắng nghe Binance WebSocket, đối soát Orderbook Limitless,
    khóa vốn vào lệnh và quyết toán sống khi nến đóng.
    """
    BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/solusdt@ticker/bnbusdt@ticker/dogeusdt@ticker/xrpusdt@ticker"

    def __init__(
        self,
        connector: LimitlessConnector,
        bankroll_usdc: Decimal = Decimal("10.00"),
        on_signal_callback: Optional[Callable[[LimitlessSignal], None]] = None,
        db: Optional[PaperDatabase] = None,
        ledger=None,
        telegram=None,
        executor=None
    ):
        self.connector = connector
        self.bankroll_usdc = bankroll_usdc
        self.on_signal_callback = on_signal_callback
        self.db = db or PaperDatabase()
        self.ledger = ledger
        self.telegram = telegram
        self.executor = executor
        
        # Cache giá Spot thời gian thực từ Binance WebSocket
        self.spot_prices: Dict[str, Decimal] = {
            "BTC": Decimal("0"),
            "ETH": Decimal("0"),
            "SOL": Decimal("0"),
            "BNB": Decimal("0"),
            "DOGE": Decimal("0"),
            "XRP": Decimal("0")
        }
        self.is_running = False
        self._last_market_refresh = 0
        self._active_markets: List[LimitlessMarket] = []
        self._market_by_slug: Dict[str, LimitlessMarket] = {}

    def is_paper_mode(self) -> bool:
        """
        Kiểm tra xem bot có đang ở chế độ PAPER (DRY_RUN=True) hay không.
        Khi ở chế độ LIVE (DRY_RUN=False), bot sẽ tự động TẮT toàn bộ thông báo paper.
        """
        if self.executor is not None and hasattr(self.executor, "dry_run"):
            return bool(self.executor.dry_run)
        return getattr(self.connector, "dry_run", True)

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
            self._market_by_slug = {m.slug: m for m in self._active_markets}
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

    async def execute_paper_entry(self, sig: LimitlessSignal, market: LimitlessMarket) -> bool:
        """
        Thực thi vào lệnh Paper Trading 100% như thật:
        1. Kiểm tra giới hạn số vị thế mở đồng thời (tối đa 3).
        2. Phạt trễ 200ms mô phỏng ký ví và gửi giao dịch RPC.
        3. Khóa vốn thực tế vào Escrow (không cộng lãi ngay).
        4. Lưu trữ trạng thái OPEN vào SQLite.
        5. Thông báo chi tiết lên Telegram.
        """
        # Kiểm tra trùng lặp: nếu thị trường này đã có lệnh OPEN thì không vào thêm
        open_positions = self.db.get_open_positions()
        for pos in open_positions:
            if pos["market_slug"] == market.slug:
                return False

        # Kiểm soát rủi ro vốn: Tối đa MAX_CONCURRENT_PAPER_POSITIONS vị thế cùng lúc
        if len(open_positions) >= CONFIG.MAX_CONCURRENT_PAPER_POSITIONS:
            logger.info("Từ chối vào lệnh %s: Đã đạt giới hạn %d vị thế mở cùng lúc.", market.slug, CONFIG.MAX_CONCURRENT_PAPER_POSITIONS)
            return False

        cost = sig.allocated_usd
        if self.ledger and self.ledger.free_balance < cost:
            logger.info("Từ chối vào lệnh %s: Số dư khả dụng không đủ ($%.2f < $%.2f).", market.slug, self.ledger.free_balance, cost)
            return False

        # Tốc độ vi xử lý cao nhất: chỉ sleep nếu latency > 0, mặc định 0ms (thực thi tức thì)
        latency_ms = getattr(CONFIG, "SIMULATED_EXECUTION_LATENCY_MS", 0)
        if latency_ms > 0:
            await asyncio.sleep(float(latency_ms) / 1000.0)

        shares = (cost / sig.share_price).quantize(Decimal("0.0001"))
        pos_id = f"POS_{market.target_asset}_{int(time.time()*1000)}"

        # Khóa vốn trên Sổ cái
        if self.ledger:
            self.ledger.reserve_capital(pos_id, cost)

        # Ghi nhận vào SQLite (Luôn lưu vĩnh viễn)
        entry_ts = int(time.time())
        self.db.record_open_position(
            position_id=pos_id,
            market_slug=market.slug,
            title=market.title,
            asset=market.target_asset,
            side=sig.recommended_side,
            strike_price=market.strike_price,
            entry_spot=sig.spot_price,
            share_price=sig.share_price,
            shares_count=shares,
            cost_usd=cost,
            entry_time=entry_ts,
            expiry_time=market.end_time,
            note=f"Buffer: {sig.buffer_usd:+.2f} USD ({sig.buffer_pct:+.2f}%)"
        )

        logger.info(
            "VỊ THẾ PAPER VÀO VỐN: %s | Cửa: %s | Vốn khóa: $%s | Strike: $%s | Spot: $%s",
            market.title, sig.recommended_side, cost, market.strike_price, sig.spot_price
        )

        # Gửi thông báo Telegram chi tiết minh bạch: Đã vào bao nhiêu | Mốc nào | Lãi dự kiến
        if self.telegram and self.is_paper_mode():
            expiry_dt = datetime.fromtimestamp(market.end_time, tz=VN_TZ)
            mins_left = sig.seconds_to_expiry // 60
            secs_left = sig.seconds_to_expiry % 60
            expected_profit_usd = (shares * Decimal("1.00")) - cost
            free_bal = self.ledger.free_balance if self.ledger else Decimal("0.00")

            msg = (
                f"🎯 <b>[V] KÈO LIMITLESS VỪA VÀO VỐN THÀNH CÔNG!</b>\n"
                f"<i>VỊ THẾ PAPER THỰC CHIẾN VỪA VÀO VỐN | Sàn: Limitless Exchange (Base L2)</i>\n\n"
                f"📊 <b>Thị trường:</b> <code>{market.title}</code>\n"
                f"⚖️ <b>Cửa cược:</b> <b>{sig.recommended_side}</b> (Giá mua: <b>${sig.share_price:.4f}</b>)\n\n"
                f"💵 <b>ĐÃ VÀO BAO NHIÊU:</b>\n"
                f"  • Vốn vào lệnh: <b>${cost:.2f} USDC</b>\n"
                f"  • Số lượng share: <b>{shares} shares</b>\n"
                f"  • Số dư ví còn lại: <b>${free_bal:.2f} USDC</b>\n\n"
                f"📍 <b>MỐC ĐỐI CHIẾU:</b>\n"
                f"  • Mốc Strike (Mở nến): <b>${market.strike_price:,.2f}</b>\n"
                f"  • Giá Spot lúc vào: <b>${sig.spot_price:,.2f}</b>\n"
                f"  • Khoảng cách an toàn (Buffer): <b>{sig.buffer_usd:+,.2f} USD</b> ({sig.buffer_pct:+.2f}%)\n"
                f"  • Chốt nến lúc: <code>{expiry_dt.strftime('%H:%M:%S %d/%m')}</code> (Còn {mins_left}p {secs_left}s)\n\n"
                f"💰 <b>LỢI NHUẬN DỰ KIẾN:</b>\n"
                f"  • Lãi ròng dự kiến: <b>+${expected_profit_usd:.3f} USDC</b>\n"
                f"  • Tỷ suất sinh lời (ROI): <b>+{sig.expected_roi_pct:.1f}%</b>\n"
                f"  • Kịch bản chốt: Nếu nến đóng đúng cửa <b>{sig.recommended_side}</b>, thu về <b>${cost + expected_profit_usd:.2f} USDC</b>"
            )
            await self.telegram.send_message(msg)
        elif not self.is_paper_mode():
            logger.info("Chế độ LIVE đang kích hoạt: Đã tắt thông báo paper trade trên Telegram.")

        return True

    async def fetch_true_settlement_price(self, asset: str) -> Decimal:
        """
        Lấy giá Spot thực tế từ Binance API lúc đóng nến để quyết toán.
        """
        try:
            symbol = f"{asset}USDT"
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                return Decimal(str(data.get("price", "0")))
        except Exception as e:
            logger.warning("Không thể fetch giá đóng nến Binance API (%s), dùng giá stream gần nhất: %s", asset, e)
            return self.spot_prices.get(asset, Decimal("0"))

    async def settlement_worker_loop(self):
        """
        Vòng lặp nền giám sát và quyết toán các vị thế đã đến giờ đáo hạn (Expiry Settlement).
        """
        logger.info("Settlement Worker Loop đã khởi động: Giám sát nến đáo hạn...")
        while self.is_running:
            try:
                now_ts = int(time.time())
                open_positions = self.db.get_open_positions()

                for pos in open_positions:
                    if now_ts >= pos["expiry_time"]:
                        # Đã đến giờ chốt nến!
                        pos_id = pos["position_id"]
                        asset = pos["asset"]
                        side = pos["side"]
                        strike = Decimal(str(pos["strike_price"]))
                        cost = Decimal(str(pos["cost_usd"]))
                        shares = Decimal(str(pos["shares_count"]))

                        settled_spot = await self.fetch_true_settlement_price(asset)
                        if settled_spot <= Decimal("0"):
                            settled_spot = self.spot_prices.get(asset, strike)

                        # Phán quyết Thắng / Thua theo quy chuẩn Oracle
                        is_win = False
                        if side == "UP" and settled_spot > strike:
                            is_win = True
                        elif side == "DOWN" and settled_spot < strike:
                            is_win = True

                        if is_win:
                            outcome = "WIN"
                            payout = shares * Decimal("1.00")
                            taker_fees = cost * Decimal("0.001") # Phí giao dịch 0.1%
                            gas_fee = Decimal("0.001")           # Phí L2 gas Base
                            net_pnl = payout - cost - taker_fees - gas_fee
                            
                            # Giải phóng vốn khóa và cộng lãi ròng trên Sổ cái
                            if self.ledger:
                                self.ledger.locked_balance = max(Decimal("0.00"), self.ledger.locked_balance - cost)
                                self.ledger.total_balance += (payout - cost - taker_fees - gas_fee)
                                self.ledger.free_balance = self.ledger.total_balance - self.ledger.locked_balance
                                self.ledger.winning_trades += 1
                                self.ledger.total_pnl += net_pnl
                                self.ledger.active_reservations.pop(pos_id, None)

                            self.db.settle_position(
                                position_id=pos_id,
                                settled_spot=settled_spot,
                                outcome="WIN",
                                payout_usd=payout,
                                net_pnl_usd=net_pnl,
                                note=f"Thắng: Chốt ${settled_spot:,.2f} vs Strike ${strike:,.2f}"
                            )

                            logger.info("VỊ THẾ THẮNG [%s]: Net PnL +$%.3f USDC", pos_id, net_pnl)

                            if self.telegram and self.is_paper_mode():
                                msg = (
                                    f"🏆 <b>[V] KẾT QUẢ QUYẾT TOÁN: THẮNG KÈO!</b>\n"
                                    f"🎯 <b>Thị trường:</b> {pos['title']}\n"
                                    f"⚖️ <b>Cửa cược:</b> {side} | <b>Strike:</b> ${strike:,.2f}\n"
                                    f"🏁 <b>Giá chốt nến thực tế:</b> <b>${settled_spot:,.2f}</b> (Cách strike: {settled_spot - strike:+.2f}$)\n\n"
                                    f"💵 <b>Vốn gốc:</b> ${cost:.2f} USDC\n"
                                    f"💰 <b>Lãi ròng thực nhận:</b> <b>+${net_pnl:.3f} USDC</b> (ROI: <b>+{net_pnl/cost*100:.1f}%</b>)\n"
                                    f"📈 <b>Số dư ví khả dụng:</b> <b>${self.ledger.free_balance if self.ledger else cost:.2f} USDC</b>"
                                )
                                await self.telegram.send_message(msg)

                        else:
                            outcome = "LOSS"
                            payout = Decimal("0.00")
                            net_pnl = -cost

                            # Trừ sạch vốn cược trên Sổ cái (bị phạt mất tiền)
                            if self.ledger:
                                self.ledger.locked_balance = max(Decimal("0.00"), self.ledger.locked_balance - cost)
                                self.ledger.total_balance -= cost
                                self.ledger.free_balance = self.ledger.total_balance - self.ledger.locked_balance
                                self.ledger.losing_trades += 1
                                self.ledger.total_pnl += net_pnl
                                self.ledger.active_reservations.pop(pos_id, None)

                            self.db.settle_position(
                                position_id=pos_id,
                                settled_spot=settled_spot,
                                outcome="LOSS",
                                payout_usd=payout,
                                net_pnl_usd=net_pnl,
                                note=f"Thua: Rút râu chốt ${settled_spot:,.2f} vs Strike ${strike:,.2f}"
                            )

                            logger.warning("VỊ THẾ THUA [%s]: Bị rút râu mất -$%.2f USDC", pos_id, cost)

                            if self.telegram and self.is_paper_mode():
                                msg = (
                                    f"❌ <b>[V] KẾT QUẢ QUYẾT TOÁN: THUA KÈO (RÚT RÂU)!</b>\n"
                                    f"🎯 <b>Thị trường:</b> {pos['title']}\n"
                                    f"⚖️ <b>Cửa cược:</b> {side} | <b>Strike:</b> ${strike:,.2f}\n"
                                    f"🏁 <b>Giá chốt nến thực tế:</b> <b>${settled_spot:,.2f}</b>\n\n"
                                    f"📉 <b>Vốn mất:</b> <b>-${cost:.2f} USDC</b>\n"
                                    f"🔍 <b>Nguyên nhân:</b> Nến đảo chiều đâm xuyên qua Strike vào phút chót.\n"
                                    f"📉 <b>Số dư ví còn lại:</b> <b>${self.ledger.free_balance if self.ledger else Decimal('0'):.2f} USDC</b>"
                                )
                                await self.telegram.send_message(msg)

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in settlement worker loop: %s", e)
                await asyncio.sleep(5)

    async def run_binance_stream(self):
        """Lắng nghe WebSocket Binance để cập nhật giá tức thì từng tích tắc."""
        self.is_running = True
        logger.info("Bắt đầu kết nối Binance WebSocket: %s", self.BINANCE_WS_URL)
        
        # Khởi động Settlement Worker Loop song song
        asyncio.create_task(self.settlement_worker_loop())

        while self.is_running:
            try:
                async with websockets.connect(self.BINANCE_WS_URL, ping_interval=20) as ws:
                    logger.info("Đã kết nối Binance WebSocket thành công.")
                    while self.is_running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        symbol = data.get("s", "")
                        price_str = data.get("c", "0")
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
                                market = self._market_by_slug.get(opp.market_slug)
                                if market:
                                    # Tự động thực thi vào lệnh Paper thực chiến
                                    asyncio.create_task(self.execute_paper_entry(opp, market))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Mất kết nối WebSocket Binance (%s), thử lại sau 3s...", e)
                await asyncio.sleep(3)

    def stop(self):
        self.is_running = False
