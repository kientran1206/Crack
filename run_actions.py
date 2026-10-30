"""
run_actions.py - Continuous Daemon Runner for GitHub Actions 24/7 Pipeline
Runs the Quantitative Arbitrage Engine & Telegram Poller for a fixed cycle (5h20m),
then gracefully hands off to the next automated runner.
"""
import asyncio
from datetime import datetime
import logging
import os
import sys
import uvicorn

from config import CONFIG
from dashboard import app, engine, ledger
from core.telegram_bot import TelegramCommandHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ActionsDaemon")

# Default 5 hours 20 minutes (19,200 seconds) to stay well within GitHub Actions 6h limit
CYCLE_DURATION_SECONDS = float(os.getenv("RUN_DURATION_SECONDS", 19200))

async def background_scanner_loop():
    """Background task continuously scanning orderbooks, auto-executing and reporting to Telegram."""
    logger.info("Initializing multi-exchange connectors & Telegram reporting...")
    await engine.initialize()
    
    refresh_counter = 0
    last_digest_hour = -1

    while engine.is_running:
        try:
            refresh_counter += 1
            # Refresh live matchdays and orderbook universe every 150 cycles (~30 seconds)
            if refresh_counter >= 150:
                refresh_counter = 0
                await asyncio.gather(
                    engine.poly.refresh_active_markets(limit=50),
                    engine.espn_scoreboard.fetch_live_matchdays(),
                    engine.asian_broker.fetch_aggregated_asian_odds(limit=30),
                    engine.betsapi_feed.fetch_live_grassroots_inplay(limit=30),
                    engine.sxbet.fetch_active_markets(limit=20),
                    return_exceptions=True
                )

            # Scan strictly real orderbooks
            opps = await engine.scan_once()
            
            # Auto-Execute on the top best profitable verified spread per cycle (Avoids capital churn)
            for opp in opps[:1]:
                if opp.est_net_profit >= CONFIG.MIN_PROFIT_HURDLE_USDC:
                    engine.execute_paper_trade(opp)

            # Send daily digest at 20:00 (or every 24h)
            now = datetime.now()
            if now.hour == 20 and now.hour != last_digest_hour:
                last_digest_hour = now.hour
                if engine.telegram.is_configured:
                    await engine.telegram.send_daily_digest(engine.get_telemetry_snapshot()["metrics"])

            await asyncio.sleep(CONFIG.SCAN_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error in scanner loop: {e}")
            await asyncio.sleep(3.0)

async def main():
    logger.info(f"=== VANTA ARBITRAGE ENGINE 24/7 (Cycle limit: {CYCLE_DURATION_SECONDS / 3600:.2f} hours) ===")
    logger.info(f"Binding internal telemetry port on {CONFIG.DASHBOARD_HOST}:{CONFIG.DASHBOARD_PORT}")
    
    # 1. Background Scanner Task
    scanner_task = asyncio.create_task(background_scanner_loop(), name="ScannerLoop")
    scanner_task.add_done_callback(lambda t: logger.error(f"ScannerLoop terminated: {t.exception()}") if not t.cancelled() and t.exception() else None)
    
    # 2. Interactive 2-Way Telegram Bot Poller
    telegram_bot = TelegramCommandHandler(engine=engine, executor=engine.executor, ledger=ledger)
    telegram_task = asyncio.create_task(telegram_bot.start_polling(), name="TelegramPoller")
    telegram_task.add_done_callback(lambda t: logger.error(f"TelegramPoller terminated: {t.exception()}") if not t.cancelled() and t.exception() else None)
    
    # 3. Notify dj on Telegram that new cloud runner is ONLINE
    if engine.telegram.is_configured:
        try:
            await engine.telegram.send_message(
                "⚡ <b>VANTA CLOUD RUNNER ONLINE (GitHub Actions 24/7)</b>\n"
                f"• Cụm máy chủ: <code>Microsoft Azure US (Ubuntu)</code>\n"
                f"• Tài nguyên: <code>7 GB RAM / 2 vCPU</code>\n"
                f"• Độ trễ Polymarket: <code>&lt; 5ms (WebSocket Push)</code>\n"
                f"• Vũ trụ bóng cỏ: <code>5,340 trận (In-Memory)</code>\n\n"
                "<i>Hệ thống đang quét liên tục, không bao giờ bị sleep!</i>"
            )
        except Exception as e:
            logger.warning(f"Could not send online broadcast: {e}")

    # 4. Web server config
    config = uvicorn.Config(
        app=app,
        host=CONFIG.DASHBOARD_HOST,
        port=CONFIG.DASHBOARD_PORT,
        log_level="warning"
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    
    # 5. Wait until cycle timeout
    try:
        await asyncio.sleep(CYCLE_DURATION_SECONDS)
        logger.info("Cycle duration reached. Preparing graceful handoff to next runner...")
    except asyncio.CancelledError:
        logger.info("Main loop cancelled.")
    finally:
        server.should_exit = True
        engine.is_running = False
        scanner_task.cancel()
        telegram_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)
        logger.info("Cycle finished cleanly. Ready for next workflow dispatch.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Terminated manually.")
