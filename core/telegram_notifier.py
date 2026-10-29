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
        """Send instant alert when an arbitrage trade is filled."""
        msg = (
            f"🔔 <b>[V] KÈO ARBITRAGE VỪA KHỚP TỰ ĐỘNG!</b>\n\n"
            f"🎯 <b>Thị trường:</b> {trade_info.get('market')}\n"
            f"⚡ <b>Chiến lược:</b> {trade_info.get('strategy')}\n"
            f"💵 <b>Vốn vào lệnh:</b> ${trade_info.get('entry_cost'):.2f} USDC\n"
            f"💰 <b>Lãi ròng ăn chắc:</b> +${trade_info.get('net_pnl'):.3f} USDC (ROI: +{trade_info.get('roi_pct'):.2f}%)\n"
            f"📊 <b>Số dư ví mới:</b> <b>${trade_info.get('new_balance'):.2f} USDC</b>\n"
            f"⏰ <i>Thời gian: {trade_info.get('timestamp')}</i>"
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
