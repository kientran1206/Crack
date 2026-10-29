"""
core/telegram_bot.py - Interactive 2-Way Telegram Control & Security Dispatcher v2.2
Full Inline Keyboard Navigation, Callback Query Router, In-Place Message Editing & Native Menu.
"""
import asyncio
from decimal import Decimal
import logging
import secrets
import time
from typing import Dict, Optional, Tuple
import httpx
from web3 import Web3

from config import CONFIG

logger = logging.getLogger("TelegramBot")

class TelegramCommandHandler:
    def __init__(self, engine, executor, ledger):
        self.engine = engine
        self.executor = executor
        self.ledger = ledger
        self.bot_token = CONFIG.TELEGRAM_BOT_TOKEN
        self.allowed_chat_id = str(CONFIG.TELEGRAM_CHAT_ID)
        self.allowed_user_id = CONFIG.TELEGRAM_USER_ID
        self.cold_wallet = CONFIG.COLD_WALLET_ADDRESS
        self.client = httpx.AsyncClient(timeout=30.0)
        self.is_running = False
        self.last_update_id = 0
        
        # Pending withdrawal requests: {token: (amount, dest_wallet, expire_timestamp)}
        self.pending_withdrawals: Dict[str, Tuple[Decimal, str, float]] = {}

    async def register_native_commands(self):
        """Register native blue Menu command list with Telegram Bot API."""
        url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
        commands = [
            {"command": "dashboard", "description": "🎛️ Bảng điều khiển nút bấm 1 chạm"},
            {"command": "balance", "description": "💼 Số dư vốn & Báo cáo PnL"},
            {"command": "wallet", "description": "💳 Ví giao dịch & Nạp on-chain"},
            {"command": "mode", "description": "⚙️ Đổi chế độ Live / Paper"},
            {"command": "status", "description": "📡 Kiểm tra kết nối & Độ trễ"},
            {"command": "pause", "description": "⏸️ Tạm dừng quét săn kèo"},
            {"command": "resume", "description": "▶️ Tiếp tục quét săn kèo"},
            {"command": "help", "description": "❓ Xem hướng dẫn toàn tập"}
        ]
        try:
            resp = await self.client.post(url, json={"commands": commands})
            if resp.status_code == 200:
                logger.info("Native Telegram commands menu registered successfully.")
        except Exception as e:
            logger.error(f"Failed to register native commands: {e}")

    async def send_message(self, text: str, reply_markup: Optional[dict] = None) -> Optional[int]:
        """Send a new message and return its message_id."""
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.allowed_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            resp = await self.client.post(url, json=payload, timeout=10.0)
            if resp.status_code == 200:
                return resp.json().get("result", {}).get("message_id")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
        return None

    async def edit_message(self, message_id: int, text: str, reply_markup: Optional[dict] = None) -> bool:
        """Edit an existing message in-place to avoid chat clutter."""
        url = f"https://api.telegram.org/bot{self.bot_token}/editMessageText"
        payload = {
            "chat_id": self.allowed_chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            resp = await self.client.post(url, json=payload, timeout=10.0)
            return resp.status_code == 200
        except Exception as e:
            logger.debug(f"Edit message skipped: {e}")
            return False

    async def answer_callback(self, callback_id: str, text: str = "", alert: bool = False):
        """Acknowledge callback query to stop loading spinner on mobile."""
        url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
        payload = {"callback_query_id": callback_id, "text": text, "show_alert": alert}
        try:
            await self.client.post(url, json=payload, timeout=5.0)
        except Exception as e:
            logger.debug(f"Error answering callback: {e}")

    # ==========================================
    # UI BUILDERS (INLINE KEYBOARDS)
    # ==========================================
    def get_dashboard_markup(self) -> dict:
        is_live = not self.executor.dry_run
        mode_btn = "🟢 Chế độ: LIVE" if is_live else "🟡 Chế độ: PAPER"
        run_btn = "⏸️ Tạm Dừng" if self.engine.is_running else "▶️ Tiếp Tục"
        return {
            "inline_keyboard": [
                [
                    {"text": "💼 Số Dư & PnL", "callback_data": "menu_balance"},
                    {"text": "💳 Ví & Nạp Tiền", "callback_data": "menu_wallet"}
                ],
                [
                    {"text": "🏛️ 60+ Sàn & Nhà Cái", "callback_data": "menu_venues"},
                    {"text": "📡 Sức Khỏe Máy", "callback_data": "menu_status"}
                ],
                [
                    {"text": mode_btn, "callback_data": "toggle_mode"},
                    {"text": run_btn, "callback_data": "toggle_running"}
                ],
                [
                    {"text": "🔒 Rút Tiền Về", "callback_data": "menu_withdraw"},
                    {"text": "🔄 Làm Mới Bảng Điều Khiển", "callback_data": "refresh_dashboard"}
                ]
            ]
        }

    def get_back_markup(self) -> dict:
        return {
            "inline_keyboard": [
                [{"text": "🔙 Quay Lại Bảng Điều Khiển", "callback_data": "refresh_dashboard"}]
            ]
        }

    def get_withdraw_options_markup(self) -> dict:
        return {
            "inline_keyboard": [
                [
                    {"text": "Rút 25%", "callback_data": "withdraw_pct_25"},
                    {"text": "Rút 50%", "callback_data": "withdraw_pct_50"},
                    {"text": "Rút Tất Cả (100%)", "callback_data": "withdraw_pct_100"}
                ],
                [{"text": "🔙 Hủy & Quay Lại", "callback_data": "refresh_dashboard"}]
            ]
        }

    def render_dashboard_text(self) -> str:
        snap = self.engine.get_telemetry_snapshot()["metrics"]
        is_live = not self.executor.dry_run
        status_str = "🟢 ĐANG QUÉT (ACTIVE)" if self.engine.is_running else "⏸️ ĐÃ TẠM DỪNG (PAUSED)"
        mode_badge = "🟢 <b>LIVE ON-CHAIN (Tiền Thật)</b>" if is_live else "🟡 <b>PAPER TRADING (Giả Lập)</b>"
        
        onchain_usdc = self.executor.get_onchain_balance()
        onchain_pol = self.executor.get_pol_gas_balance()
        short_addr = f"{self.executor.address[:6]}...{self.executor.address[-4:]}" if self.executor.address else "Chưa tạo"

        # Mini progress bar for Win Rate
        wr = float(snap['win_rate'])
        filled_slots = int(wr / 10)
        bar = "█" * filled_slots + "░" * (10 - filled_slots)

        ws_info = snap.get('ws_status', {})
        ws_badge = "🟢 CONNECTED (Sub-0.1ms)" if ws_info.get('connected') else "🟡 STREAMING"
        super_count = snap.get('super_grassroots_count', 0)

        return (
            f"⚡ <b>VANTA // QUANTITATIVE CONTROL CENTER v4.0</b>\n"
            f"<i>Động cơ săn chênh lệch tỷ lệ cược bóng cỏ tức thì Sub-5ms</i>\n\n"
            f"⚙️ <b>Vận hành:</b> {status_str}\n"
            f"🛡️ <b>Môi trường:</b> {mode_badge}\n"
            f"📍 <b>Ví Bot (Polygon L2):</b> <code>{short_addr}</code>\n\n"
            f"💵 <b>Vốn On-Chain:</b> <code>${onchain_usdc:.2f} USDC.e</code>\n"
            f"⛽ <b>Phí Gas Dự Trữ:</b> <code>{onchain_pol:.4f} POL</code>\n"
            f"📊 <b>Vốn Giao Dịch:</b> <code>${snap['current_balance']:.2f} USDC</code>\n"
            f"📈 <b>Lợi Nhuận Ròng (PnL):</b> <b>+${snap['total_pnl']:.3f} USDC</b> (ROI: +{snap['roi_percent']:.2f}%)\n"
            f"🎯 <b>Hiệu Suất:</b> [{bar}] {wr:.1f}% ({snap['trades_count']} lệnh)\n"
            f"⚡ <b>Độ Trễ In-Memory:</b> <b>{snap['latency_ms']} ms</b> | WS: {ws_badge}\n"
            f"🌐 <b>Vũ Trụ Theo Dõi:</b> <b>{snap['active_markets_watched']:,} cặp</b> (Siêu cỏ & TT Cup: {super_count:,}+ trận)\n\n"
            f"👉 <i>Chạm vào các nút bên dưới để điều khiển trực tiếp:</i>"
        )

    # ==========================================
    # POLLING & EVENT LOOP
    # ==========================================
    async def start_polling(self):
        """Asynchronous long-polling loop handling Messages & Callback Queries."""
        if not self.bot_token or not self.allowed_chat_id:
            logger.warning("Telegram Bot credentials not configured. Interactive polling skipped.")
            return

        self.is_running = True
        logger.info("Telegram Command Handler v2.2 listening for dj's orders 24/7...")

        # Register Native Command Menu & Initial Greeting safely
        try:
            await self.register_native_commands()
            await self.send_message(self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())
        except Exception as e:
            logger.error(f"Error in initial telegram greeting: {e}", exc_info=True)

        while self.is_running:
            try:
                url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
                params = {"offset": self.last_update_id + 1, "timeout": 20}
                resp = await self.client.get(url, params=params, timeout=25.0)
                
                if resp.status_code == 200:
                    data = resp.json()
                    for update in data.get("result", []):
                        self.last_update_id = update["update_id"]
                        
                        # Handle button clicks (callback query)
                        if "callback_query" in update:
                            await self.handle_callback_query(update["callback_query"])
                        # Handle text messages
                        elif "message" in update:
                            await self.handle_incoming_message(update["message"])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Telegram polling tick: {e}")
                await asyncio.sleep(2.0)

    # ==========================================
    # CALLBACK QUERY ROUTER (NÚT BẤM 1 CHẠM)
    # ==========================================
    async def handle_callback_query(self, query: dict):
        query_id = query.get("id")
        user_id = query.get("from", {}).get("id")
        data = query.get("data", "")
        message = query.get("message", {})
        message_id = message.get("message_id")

        # Hard-lock security check
        if self.allowed_user_id and user_id != self.allowed_user_id:
            await self.answer_callback(query_id, "⛔ Quyền truy cập bị từ chối!", alert=True)
            return

        if data == "refresh_dashboard":
            await self.answer_callback(query_id, "🔄 Đã cập nhật số liệu mới nhất!")
            await self.edit_message(message_id, self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())

        elif data == "toggle_mode":
            is_currently_live = not self.executor.dry_run
            if not is_currently_live:
                # Want to switch to LIVE
                pol = self.executor.get_pol_gas_balance()
                if pol < Decimal("0.02"):
                    await self.answer_callback(query_id, f"⚠️ Ví cần >= 0.02 POL làm gas (hiện có {pol} POL)!", alert=True)
                    return
                self.executor.dry_run = False
                await self.answer_callback(query_id, "🟢 Đã BẬT chế độ LIVE ON-CHAIN (Tiền Thật)!", alert=True)
            else:
                self.executor.dry_run = True
                await self.answer_callback(query_id, "🟡 Đã chuyển về chế độ PAPER (Giả Lập)!")
            await self.edit_message(message_id, self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())

        elif data == "toggle_running":
            self.engine.is_running = not self.engine.is_running
            status_toast = "▶️ Đã tiếp tục săn kèo!" if self.engine.is_running else "⏸️ Đã tạm dừng động cơ!"
            await self.answer_callback(query_id, status_toast)
            await self.edit_message(message_id, self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())

        elif data == "menu_wallet":
            await self.answer_callback(query_id)
            addr = self.executor.address or "Chưa khởi tạo"
            onchain_usdc = self.executor.get_onchain_balance()
            onchain_pol = self.executor.get_pol_gas_balance()
            text = (
                f"💳 <b>THÔNG TIN VÍ HOT CUSTODY (POLYGON L2)</b>\n\n"
                f"📍 <b>Địa chỉ ví Bot:</b>\n<code>{addr}</code>\n\n"
                f"💵 <b>USDC.e (Vốn giao dịch):</b> <code>${onchain_usdc:.2f} USDC</code>\n"
                f"⛽ <b>POL (Phí Gas on-chain):</b> <code>{onchain_pol:.4f} POL</code>\n\n"
                f"📥 <b>Hướng dẫn nạp vốn:</b>\n"
                f"1. Gửi <b>5 - 20 USDC.e</b> (mạng Polygon PoS) vào địa chỉ trên.\n"
                f"2. Gửi thêm khoảng <b>0.2 - 0.5 POL</b> làm phí gas ký lệnh.\n"
                f"3. Sau khi gửi, bấm nút Làm Mới để kiểm tra số dư."
            )
            await self.edit_message(message_id, text, reply_markup=self.get_back_markup())

        elif data == "menu_venues":
            await self.answer_callback(query_id)
            snap = self.engine.get_telemetry_snapshot()
            m = snap["metrics"]
            text = (
                f"🏛️ <b>DANH SÁCH 60+ NHÀ CÁI & SÀN ĐANG ĐƯỢC QUÉT 24/7</b>\n\n"
                f"1️⃣ <b>Sàn Web3 Prediction & Sports (4 sàn):</b>\n"
                f"• <b>Polymarket</b> (Polygon L2 CLOB WS - Sub-0.1ms)\n"
                f"• <b>SX Bet</b> (Arbitrum Centrifugo WS - Best Odds v3)\n"
                f"• <b>Azuro Protocol</b> (Onchain feed 80+ sports)\n"
                f"• <b>Overtime Markets V2</b> (Thales AMM on Optimism)\n\n"
                f"2️⃣ <b>Cổng Asian Brokerage (5 nhà cái châu Á lớn):</b>\n"
                f"• <b>SBOBET</b>, <b>Singbet (Crown)</b>, <b>BetISN</b>, <b>IBCBET</b>, <b>Pinnacle Asian</b>\n"
                f"👉 <i>Cơ chế: Smart Order Routing (SOR) gom chung 1 tài khoản, chính sách 'Winners Welcome' không bao giờ giới hạn tiền cược!</i>\n\n"
                f"3️⃣ <b>Top Tier Global (50+ Nhà cái quốc tế):</b>\n"
                f"• Pinnacle Sharp EU, Betfair Exchange P2P, Bet365 Global, DraftKings, Bovada US, Unibet EU, 888sport, William Hill\n"
                f"👉 <i>Cơ chế: The-Odds-API Gateway qua Azure Datacenter US (< 5ms)</i>\n\n"
                f"4️⃣ <b>Vũ Trụ Bóng Cỏ & Siêu Cỏ In-Play (150+ giải đấu):</b>\n"
                f"• <b>Bóng bàn TT Cup 24/7</b> (Ukraine/Ba Lan - 15 phút/trận)\n"
                f"• <b>Tennis ITF $15k/$25k</b> (Monastir, Antalya)\n"
                f"• <b>Bóng cỏ hạng 4-5</b>: Đức (Regionalliga), Anh (National League), Brazil Serie D, U19 UEFA\n"
                f"👉 <i>Cơ chế: BetsAPI B2B In-play Feed + Bộ lọc Palpable Error & Khớp lệnh không đối xứng.</i>\n\n"
                f"📊 <b>Tổng thị trường đang trực tiếp định giá:</b> <b>{m['active_markets_watched']:,} cặp</b>"
            )
            await self.edit_message(message_id, text, reply_markup=self.get_back_markup())

        elif data == "menu_balance":
            await self.answer_callback(query_id)
            snap = self.engine.get_telemetry_snapshot()["metrics"]
            text = (
                f"💼 <b>BÁO CÁO TÀI SẢN & PHÂN BỔ VỐN</b>\n\n"
                f"💰 <b>Tổng số dư:</b> <b>${snap['current_balance']:.2f} USDC</b>\n"
                f"🔓 <b>Khả dụng vào lệnh:</b> ${snap['free_balance']:.2f} USDC\n"
                f"🔒 <b>Ký quỹ đang khóa:</b> ${snap['locked_balance']:.2f} USDC\n\n"
                f"📈 <b>Lợi nhuận ròng (Net PnL):</b> <b>+${snap['total_pnl']:.3f} USDC</b>\n"
                f"🚀 <b>Tỷ suất sinh lời (ROI):</b> +{snap['roi_percent']:.2f}%\n"
                f"🎯 <b>Tổng số lệnh chốt:</b> {snap['trades_count']} lệnh\n"
                f"🏆 <b>Tỷ lệ thắng (Win Rate):</b> {snap['win_rate']:.1f}%\n"
                f"📡 <b>Tổng lượt quét sổ lệnh:</b> {snap['scan_count']:,} lần"
            )
            await self.edit_message(message_id, text, reply_markup=self.get_back_markup())

        elif data == "menu_status":
            await self.answer_callback(query_id)
            snap = self.engine.get_telemetry_snapshot()
            state = "🟢 RUNNING" if self.engine.is_running else "⏸️ PAUSED"
            ws_stat = snap['metrics'].get('ws_status', {})
            ws_conn = "🟢 CONNECTED (Sub-0.1ms)" if ws_stat.get('connected') else "🟡 STREAMING"
            text = (
                f"📡 <b>SỨC KHỎE HỆ THỐNG QUANTITATIVE v4.0</b>\n\n"
                f"⚙️ <b>Động cơ:</b> {state}\n"
                f"⚡ <b>Độ trễ In-Memory:</b> <b>{snap['metrics']['latency_ms']} ms</b> (Microsecond Solver)\n"
                f"🔌 <b>Push WebSocket CLOB:</b> {ws_conn}\n"
                f"📊 <b>Tổng lượt quét:</b> {snap['metrics']['scan_count']:,} lần\n"
                f"🌐 <b>Vũ trụ thị trường theo dõi:</b> <b>{snap['metrics']['active_markets_watched']:,} cặp</b>\n"
                f"⚽ <b>Bóng cỏ & siêu cỏ:</b> <b>{snap['metrics'].get('grassroots_count', 0):,} giải đấu</b>\n"
                f"🏓 <b>Bóng siêu cỏ & TT Cup 24/7:</b> {snap['metrics'].get('super_grassroots_count', 0):,} trận\n"
                f"🔥 <b>Cơ hội chênh lệch mở:</b> {len(snap['active_opportunities'])} kèo\n\n"
                f"<i>Môi trường: Python 3.14 + Lock-Free Event Loop + Pre-Match Safe Gate</i>"
            )
            await self.edit_message(message_id, text, reply_markup=self.get_back_markup())

        elif data == "menu_withdraw":
            await self.answer_callback(query_id)
            balance = self.executor.get_onchain_balance()
            text = (
                f"🔒 <b>RÚT TIỀN VỀ VÍ LẠNH AN TOÀN</b>\n\n"
                f"💵 <b>Số dư on-chain khả dụng:</b> <code>${balance:.2f} USDC.e</code>\n"
                f"📍 <b>Ví nhận tiền mặc định:</b>\n"
                f"<code>{self.cold_wallet or 'Chưa cấu hình (Có thể nhập thủ công)'}</code>\n\n"
                f"Chọn nhanh tỷ lệ rút bên dưới:"
            )
            await self.edit_message(message_id, text, reply_markup=self.get_withdraw_options_markup())

        elif data.startswith("withdraw_pct_"):
            pct = int(data.split("_")[-1])
            balance = self.executor.get_onchain_balance()
            if balance <= Decimal("0.10"):
                await self.answer_callback(query_id, f"❌ Số dư không đủ để rút (${balance:.2f})!", alert=True)
                return
            
            amount = (balance * Decimal(str(pct)) / Decimal("100")).quantize(Decimal("0.01"))
            dest = self.cold_wallet
            if not dest:
                await self.answer_callback(query_id, "Vui lòng nhập lệnh: /withdraw all 0xĐịaChỉVí", alert=True)
                return

            token = f"WT-{secrets.randbelow(8999) + 1000}"
            self.pending_withdrawals[token] = (amount, dest, time.time() + 60.0)

            await self.answer_callback(query_id, f"Đã sinh mã xác nhận rút ${amount} USDC!")
            text = (
                f"🔐 <b>XÁC NHẬN RÚT TIỀN 2 BƯỚC (2-STEP OTP)</b>\n\n"
                f"💵 <b>Số tiền rút:</b> ${amount:.2f} USDC.e ({pct}% vốn)\n"
                f"📍 <b>Ví nhận của dj:</b>\n<code>{dest}</code>\n\n"
                f"👉 Gõ lệnh sau trong <b>60 giây</b> để hoàn tất:\n"
                f"<code>/confirm {token}</code>"
            )
            await self.edit_message(message_id, text, reply_markup=self.get_back_markup())

    # ==========================================
    # TEXT MESSAGE COMMAND HANDLER
    # ==========================================
    async def handle_incoming_message(self, message: dict):
        chat_id = str(message.get("chat", {}).get("id"))
        user_id = message.get("from", {}).get("id")
        text = message.get("text", "").strip()

        # Hard-lock security check
        if chat_id != self.allowed_chat_id or (self.allowed_user_id and user_id != self.allowed_user_id):
            logger.warning(f"Unauthorized message dropped from ChatID={chat_id}")
            return

        if not text:
            return

        parts = text.split()
        command = parts[0].lower()
        args = parts[1:]

        if command in ("/start", "/dashboard", "/menu"):
            await self.send_message(self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())
        elif command == "/wallet":
            await self.send_message(
                f"💳 <b>ĐỊA CHỈ VÍ BOT:</b>\n<code>{self.executor.address}</code>\n\n"
                f"Vốn: ${self.executor.get_onchain_balance():.2f} USDC.e | Gas: {self.executor.get_pol_gas_balance():.4f} POL",
                reply_markup=self.get_dashboard_markup()
            )
        elif command == "/balance":
            await self.send_message(self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())
        elif command == "/status":
            snap = self.engine.get_telemetry_snapshot()
            await self.send_message(
                f"📡 <b>TRẠNG THÁI:</b> Latency: {snap['metrics']['latency_ms']}ms | Markets: {snap['metrics']['active_markets_watched']}",
                reply_markup=self.get_dashboard_markup()
            )
        elif command == "/pause":
            self.engine.is_running = False
            await self.send_message("⏸️ <b>ĐÃ TẠM DỪNG SĂN KÈO!</b>", reply_markup=self.get_dashboard_markup())
        elif command == "/resume":
            self.engine.is_running = True
            await self.send_message("▶️ <b>ĐÃ TIẾP TỤC QUÉT SỔ LỆNH!</b>", reply_markup=self.get_dashboard_markup())
        elif command == "/mode":
            if args and args[0].lower() == "live":
                self.executor.dry_run = False
                await self.send_message("🟢 <b>ĐÃ BẬT CHẾ ĐỘ LIVE TIỀN THẬT!</b>", reply_markup=self.get_dashboard_markup())
            elif args and args[0].lower() == "paper":
                self.executor.dry_run = True
                await self.send_message("🟡 <b>ĐÃ CHUYỂN VỀ CHẾ ĐỘ GIẢ LẬP!</b>", reply_markup=self.get_dashboard_markup())
            else:
                await self.send_message(self.render_dashboard_text(), reply_markup=self.get_dashboard_markup())
        elif command == "/withdraw":
            await self.cmd_withdraw_text(args)
        elif command == "/confirm":
            await self.cmd_confirm_text(args)
        elif command == "/help":
            await self.send_message(
                "⚡ <b>HƯỚNG DẪN:</b> Bấm phím <b>Menu</b> góc trái hoặc bấm trực tiếp các nút trên Bảng điều khiển để thao tác.",
                reply_markup=self.get_dashboard_markup()
            )

    async def cmd_withdraw_text(self, args):
        balance = self.executor.get_onchain_balance()
        if balance <= Decimal("0.10"):
            await self.send_message(f"❌ Số dư không đủ để rút (${balance:.2f}).")
            return

        dest = self.cold_wallet
        if not dest and args:
            cand = args[-1]
            if cand.startswith("0x") and len(cand) == 42:
                dest = cand
                args = args[:-1]

        if not dest:
            await self.send_message("❌ Cú pháp rút tiền: <code>/withdraw all 0xĐịaChỉVíCủaDj</code>")
            return

        target_amount = balance
        if args and args[0].lower() != "all":
            try:
                target_amount = Decimal(args[0])
            except Exception:
                await self.send_message("❌ Số tiền không hợp lệ.")
                return

        token = f"WT-{secrets.randbelow(8999) + 1000}"
        self.pending_withdrawals[token] = (target_amount, dest, time.time() + 60.0)
        await self.send_message(
            f"🔐 <b>XÁC NHẬN RÚT ${target_amount:.2f} USDC VỀ:</b>\n<code>{dest}</code>\n\n"
            f"👉 Nhập trong 60 giây: <code>/confirm {token}</code>"
        )

    async def cmd_confirm_text(self, args):
        if not args:
            await self.send_message("❌ Vui lòng nhập mã OTP. Ví dụ: <code>/confirm WT-1234</code>")
            return

        token = args[0].strip().upper()
        if token not in self.pending_withdrawals:
            await self.send_message("❌ Mã xác nhận không tồn tại hoặc đã hết hạn!")
            return

        amount, dest, expires_at = self.pending_withdrawals.pop(token)
        if time.time() > expires_at:
            await self.send_message("⏱️ Mã xác nhận đã hết hạn.")
            return

        await self.send_message("⏳ Đang ký giao dịch on-chain chuyển tiền về ví của dj...")
        res = await self.executor.withdraw_to_cold_wallet(amount, dest)
        if res.get("success"):
            tx_h = res.get("tx_hash", "")
            await self.send_message(
                f"✅ <b>RÚT TIỀN THÀNH CÔNG!</b>\n"
                f"💵 Số tiền: ${amount:.2f} USDC.e\n"
                f"🔗 TxHash: <a href='https://polygonscan.com/tx/{tx_h}'>{tx_h[:16]}...</a>"
            )
        else:
            await self.send_message(f"❌ Rút tiền thất bại: {res.get('error')}")

    async def close(self):
        self.is_running = False
        await self.client.aclose()
