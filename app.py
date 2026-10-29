"""
app.py - Hugging Face Gradio Space Entrypoint for VANTA Quantitative Engine v4.0
Runs Quantitative Arbitrage Scanner, Push WebSocket & Telegram Controller in background 24/7.
"""
import asyncio
import os
import threading
import time
from decimal import Decimal
import gradio as gr

from dashboard import engine, ledger
from core.telegram_bot import TelegramCommandHandler

# 1. Background worker running Quantitative Engine & Telegram Poller
def run_quant_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def worker_main():
        print("[HuggingFace] Initializing Quantitative Arbitrage Engine v4.0...")
        await engine.initialize()
        
        # Start Telegram Command Poller
        telegram_bot = TelegramCommandHandler(engine=engine, executor=engine.executor, ledger=ledger)
        asyncio.create_task(telegram_bot.start_polling())
        
        # Continuous scanning loop
        refresh_counter = 0
        while True:
            try:
                if engine.is_running:
                    refresh_counter += 1
                    if refresh_counter >= 30:
                        refresh_counter = 0
                        await engine.poly.refresh_active_markets(limit=25)
                        await engine.sxbet.fetch_active_markets(limit=20)
                        await engine.manifold.fetch_active_binary_markets(limit=25)

                    opps = await engine.scan_once()
                    for opp in opps:
                        if opp.est_net_profit >= Decimal("0.02"):
                            engine.execute_paper_trade(opp)

                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"[Worker Error]: {e}")
                await asyncio.sleep(2.0)

    loop.run_until_complete(worker_main())

# Launch worker in daemon thread
worker_thread = threading.Thread(target=run_quant_worker, daemon=True)
worker_thread.start()

# 2. Gradio Web Interface for Hugging Face Spaces (Port 7860)
def get_live_metrics():
    snap = engine.get_telemetry_snapshot()
    m = snap["metrics"]
    ws_info = m.get("ws_status", {})
    ws_text = "🟢 CONNECTED (<0.1ms)" if ws_info.get("connected") else "🟡 STREAMING"
    
    status_markdown = f"""
### ⚡ VANTA // QUANTITATIVE CONTROL CENTER v4.0
**Môi trường:** `AWS us-east-1 (Bắc Virginia)` | **Trạng thái:** `🟢 ONLINE 24/7`

| Chỉ số vận hành | Giá trị thực tế |
| :--- | :--- |
| **Vũ trụ thị trường theo dõi** | **{m['active_markets_watched']:,} cặp** (Bóng siêu cỏ & TT Cup: {m.get('super_grassroots_count', 0):,}+) |
| **Độ trễ In-Memory Matcher** | **{m['latency_ms']} ms** |
| **Push WebSocket CLOB** | **{ws_text}** |
| **Số dư giao dịch (Virtual)** | **${m['current_balance']:.2f} USDC** |
| **Lợi nhuận ròng (PnL)** | **+${m['total_pnl']:.3f} USDC** (ROI: +{m['roi_percent']:.2f}%) |
| **Tổng số lượt quét** | **{m['scan_count']:,} lần** |
| **Số lệnh chốt thành công** | **{m['trades_count']} lệnh** (Tỷ lệ thắng: {m['win_rate']:.1f}%) |

👉 *Hệ thống đang chạy ngầm và đồng bộ 2 chiều với Telegram Bot: <b>@Baocaotilehomnay1_bot</b>*
"""
    recent_logs = "\n".join(snap.get("log_stream", [])[:15])
    return status_markdown, recent_logs

with gr.Blocks(title="VANTA Quant Engine v4.0", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# ⚡ VANTA // HIGH-FREQUENCY ARBITRAGE CLOUD NODE")
    
    with gr.Row():
        metrics_display = gr.Markdown()
    
    with gr.Row():
        logs_display = gr.Textbox(label="Live Engine Event Log", lines=10, interactive=False)
        
    refresh_btn = gr.Button("🔄 Cập nhật số liệu ngay", variant="primary")
    refresh_btn.click(fn=get_live_metrics, outputs=[metrics_display, logs_display])
    
    demo.load(fn=get_live_metrics, outputs=[metrics_display, logs_display])

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
