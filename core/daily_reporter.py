"""
core/daily_reporter.py - Automated Daily Audit & Telegram Performance Reporter
Fires scheduled reports at 10:00 AM VN and 22:00 PM VN, and provides instant /daily_report rendering.
"""
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import logging
import time
from typing import Optional

from config import CONFIG
from core.paper_database import PaperDatabase

logger = logging.getLogger("DailyReporter")
VN_TZ = timezone(timedelta(hours=7))


def format_audit_report(db: PaperDatabase, current_balance: Decimal, starting_balance: Decimal = Decimal("10.00"), title_suffix: str = "") -> str:
    """Generate a clean, professional, markdown audit report."""
    now_vn = datetime.now(VN_TZ)
    perf = db.get_recent_performance(lookback_seconds=86400) # 24h performance
    
    total_trades = perf["settled_count"]
    win_trades = perf["winning_count"]
    loss_trades = perf["losing_count"]
    win_rate = perf["win_rate_pct"]
    net_pnl = perf["total_pnl_usd"]
    max_dd = perf["max_drawdown_usd"]
    open_count = perf["open_count"]
    
    roi_pct = ((float(current_balance) - float(starting_balance)) / float(starting_balance) * 100) if starting_balance > 0 else 0.0
    pnl_sign = "+" if net_pnl >= 0 else ""
    roi_sign = "+" if roi_pct >= 0 else ""

    text = (
        f"📊 <b>[V] BÁO CÁO KIỂM TOÁN HIỆU SUẤT TRADING {title_suffix}</b>\n"
        f"<i>Cơ chế 100% Thực chiến | Phạt trễ 200ms | Quyết toán Oracle sống</i>\n"
        f"📅 <b>Thời gian:</b> <code>{now_vn.strftime('%d/%m/%Y %H:%M:%S')}</code> (Giờ VN)\n\n"
        f"💼 <b>Vốn ban đầu:</b> ${starting_balance:.2f} USDC\n"
        f"💵 <b>Số dư hiện tại:</b> <b>${current_balance:.2f} USDC</b>\n"
        f"📈 <b>Lợi nhuận ròng (24h):</b> <b>{pnl_sign}${net_pnl:.3f} USDC</b> (ROI: <b>{roi_sign}{roi_pct:.2f}%</b>)\n"
        f"📉 <b>Sụt giảm vốn sâu nhất:</b> -${max_dd:.3f} USDC\n\n"
        f"🎯 <b>TỔNG KẾT VỊ THẾ:</b>\n"
        f"• Tổng số lệnh đã chốt: <b>{total_trades} lệnh</b>\n"
        f"• 🏆 Thắng: <b>{win_trades}</b> ({win_rate:.1f}%) | ❌ Thua: <b>{loss_trades}</b>\n"
        f"• ⏳ Vị thế đang giam vốn: <b>{open_count} lệnh</b>\n\n"
    )

    recent = perf.get("recent_settled", [])
    if recent:
        text += "🔍 <b>CHI TIẾT CÁC LỆNH ĐÃ CHỐT GẦN NHẤT:</b>\n"
        for idx, r in enumerate(recent[:6], 1):
            outcome_icon = "🟢 THẮNG" if r["outcome"] == "WIN" else "🔴 THUA"
            pnl_val = float(r["net_pnl_usd"])
            sign = "+" if pnl_val >= 0 else ""
            asset = r["asset"]
            side = r["side"]
            strike = r["strike_price"]
            close_p = r.get("settled_spot") or 0.0
            text += (
                f"{idx}. {outcome_icon} <b>{asset} {side}</b> | Strike: ${strike:,.0f} | Chốt: ${close_p:,.0f}\n"
                f"   💵 Vốn: ${r['cost_usd']:.2f} | PnL: <b>{sign}${pnl_val:.3f} USDC</b>\n"
            )
    else:
        text += "ℹ️ <i>Chưa có vị thế nào đáo hạn trong 24h qua. Đang quét nến Hourly liên tục...</i>\n"

    open_pos = perf.get("open_positions", [])
    if open_pos:
        text += "\n⏳ <b>VỊ THẾ ĐANG CHỜ ĐÓNG NẾN (ESCROW):</b>\n"
        for op in open_pos:
            sec_left = max(0, op["expiry_time"] - int(time.time()))
            text += f"• <b>{op['asset']} {op['side']}</b>: Vốn khóa ${op['cost_usd']:.2f} (Còn {sec_left}s)\n"

    return text


class AutomatedDailyReporter:
    """
    Background worker that triggers automated Telegram reports at 10:00 AM VN and 22:00 PM VN.
    """
    def __init__(self, db: PaperDatabase, telegram_bot, ledger):
        self.db = db
        self.telegram = telegram_bot
        self.ledger = ledger
        self.is_running = False
        self._last_reported_date_hour = ""

    async def run_loop(self):
        """Check every 30 seconds for 10:00 AM or 22:00 PM VN trigger."""
        self.is_running = True
        logger.info("AutomatedDailyReporter online: Listening for 10:00 AM & 22:00 PM VN...")
        
        while self.is_running:
            try:
                now_vn = datetime.now(VN_TZ)
                current_hour = now_vn.hour
                current_minute = now_vn.minute
                date_str = now_vn.strftime("%Y-%m-%d")
                key = f"{date_str}_{current_hour}"

                # Trigger window: at minute 0-2 of 10h and 22h
                if current_hour in CONFIG.DAILY_REPORT_HOURS_VN and current_minute <= 2:
                    if self._last_reported_date_hour != key:
                        self._last_reported_date_hour = key
                        hour_label = "10:00 SÁNG" if current_hour == 10 else "22:00 ĐÊM"
                        report_text = format_audit_report(
                            db=self.db,
                            current_balance=self.ledger.total_balance,
                            starting_balance=self.ledger.starting_balance,
                            title_suffix=f"({hour_label})"
                        )
                        
                        logger.info("Sending scheduled daily report (%s) to Telegram...", hour_label)
                        if self.telegram:
                            await self.telegram.send_message(report_text)
                            
                        # Save audit record
                        audit_id = f"AUDIT_{key}"
                        perf = self.db.get_recent_performance(lookback_seconds=86400)
                        self.db.record_daily_audit(
                            audit_id=audit_id,
                            date_str=date_str,
                            report_hour=current_hour,
                            starting_balance=float(self.ledger.starting_balance),
                            current_balance=float(self.ledger.total_balance),
                            perf=perf,
                            report_text=report_text
                        )

                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in daily report loop: %s", e)
                await asyncio.sleep(10)

    def stop(self):
        self.is_running = False
