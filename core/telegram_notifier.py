"""
telegram_notifier.py - Telegram Automated Alert & Daily Report System
Sends instant execution notifications and daily financial digests directly to dj's phone.
"""
import asyncio
import logging
import os
import time
from typing import Optional
import httpx

logger = logging.getLogger("TelegramNotifier")

class TelegramNotifier:
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.client = httpx.AsyncClient(timeout=10.0)
        self.last_notify_ts: float = 0.0

    @property
    def is_configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def send_message(self, text: str) -> bool:
        """Send a markdown message to the configured Telegram chat."""
        if not self.is_configured:
            logger.info(f"[TELEGRAM (UNCONFIGURED)]: {text[:100]}...")
            return False
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        try:
            resp = await self.client.post(url, json=payload)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Failed to deliver Telegram notification: {e}")
            return False

    async def notify_trade_executed(self, trade_info: dict):
        """Send instant alert when an arbitrage trade is filled (throttled to avoid 429 spam)."""
        now = time.time()
        if now - self.last_notify_ts < 15.0:
            return  # Throttle to max 1 Telegram notification per 15s to keep UI responsive
        self.last_notify_ts = now

        horizon = trade_info.get('horizon', 'Real-Time Orderbook')
        url = trade_info.get('url', '')
        link_str = f"\n🔗 <b>Kiểm chứng sàn:</b> <a href=\"{url}\">Mở trang thị trường</a>" if url else ""

        msg = (
            f"🔔 <b>[V] KÈO ARBITRAGE VỪA KHỚP TỰ ĐỘNG!</b>\n\n"
            f"🎯 <b>Thị trường:</b> {trade_info.get('market')}\n"
            f"⏱️ <b>Kỳ hạn thi đấu:</b> <code>{horizon}</code>\n"
            f"⚡ <b>Chiến lược:</b> {trade_info.get('strategy')}\n"
            f"💵 <b>Vốn vào lệnh:</b> ${trade_info.get('entry_cost'):.2f} USDC\n"
            f"💰 <b>Lãi ròng ăn chắc:</b> +${trade_info.get('net_pnl'):.3f} USDC (ROI: +{trade_info.get('roi_pct'):.2f}%)\n"
            f"📊 <b>Số dư khả dụng:</b> <b>${trade_info.get('new_balance'):.2f} USDC</b>\n"
            f"⏰ <i>Thời gian tick: {trade_info.get('timestamp')}</i>"
            f"{link_str}"
        )
        await self.send_message(msg)

    async def notify_limitless_signal(self, sig):
        """Bắn tín hiệu săn kèo Late-Cycle Convergence trên Limitless Exchange trực tiếp về Telegram dj."""
        now = time.time()
        if now - self.last_notify_ts < 10.0:
            return  # Throttle 10s
        self.last_notify_ts = now

        mins = sig.seconds_to_expiry // 60
        secs = sig.seconds_to_expiry % 60
        time_str = f"{mins}m {secs}s"

        msg = (
            f"🚀 <b>[V] TÍN HIỆU SĂN KÈO LIMITLESS (BASE L2)!</b>\n\n"
            f"🎯 <b>Thị trường:</b> <code>{sig.market_slug}</code>\n"
            f"⏱️ <b>Thời gian còn lại:</b> <b>{time_str}</b>\n"
            f"💎 <b>Tài sản:</b> {sig.asset} | Strike: <b>${sig.strike_price:,.2f}</b>\n"
            f"⚡ <b>Giá Spot (Binance):</b> <b>${sig.spot_price:,.2f}</b>\n"
            f"🛡️ <b>Buffer an toàn:</b> <b>{sig.buffer_usd:+,.2f} USD</b> ({sig.buffer_pct:+.2f}%)\n"
            f"👉 <b>Cửa cược khuyến nghị:</b> <b>{sig.recommended_side}</b> (Giá share: {sig.share_price:.2f}¢)\n"
            f"📈 <b>Lãi dự kiến:</b> <b>+{sig.expected_roi_pct:.1f}%</b> (Maker 0% gas)\n"
            f"💵 <b>Vốn cược (Bảo vệ Bankroll $10):</b> <b>${sig.allocated_usd:.2f} USDC</b>\n\n"
            f"🔒 <i>Chế độ: PostOnly: True | Oracle: Chainlink Data Streams</i>"
        )
        await self.send_message(msg)

    async def send_daily_digest(self, metrics: dict):
        """Send end-of-day summary digest to dj."""
        msg = (
            f"📊 <b>[V] BÁO CÁO TỔNG KẾT NGÀY - QUANTITATIVE ENGINE</b>\n\n"
            f"💼 <b>Số dư ví hiện tại:</b> <b>${metrics.get('current_balance'):.2f} USDC</b>\n"
            f"📈 <b>Tổng lợi nhuận ròng:</b> <b>+${metrics.get('total_pnl'):.3f} USDC</b>\n"
            f"🚀 <b>Tỷ suất sinh lời (ROI):</b> +{metrics.get('roi_percent'):.2f}%\n"
            f"🎯 <b>Tổng số lệnh đã khớp:</b> {metrics.get('trades_count')} lệnh\n"
            f"🏆 <b>Tỷ lệ thắng:</b> {metrics.get('win_rate'):.1f}%\n"
            f"📡 <b>Tổng lượt quét sổ lệnh:</b> {metrics.get('scan_count'):,} lần\n\n"
            f"<i>Trạng thái: Bot đang chạy ngầm 24/7 ổn định. dj cứ an tâm làm việc.</i>"
        )
        await self.send_message(msg)

    async def close(self):
        await self.client.aclose()
