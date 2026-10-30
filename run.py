"""
run.py - Main Entrypoint for Arbitrage Engine, Live Webhook & Telegram Reporting
Run with: python run.py
"""
import asyncio
from datetime import datetime
import logging
import uvicorn

from config import CONFIG
from dashboard import app, engine, ledger
from core.telegram_bot import TelegramCommandHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("Launcher")

async def background_scanner_loop():
    """Background task continuously scanning orderbooks, auto-executing and reporting to Telegram."""
    logger.info("Initializing multi-exchange connectors & Telegram reporting...")
    await engine.initialize()
    
    refresh_counter = 0
    last_digest_hour = -1

    while True:
        try:
            if engine.is_running:
                refresh_counter += 1
                if refresh_counter >= 30:
                    refresh_counter = 0
                    await engine.poly.refresh_active_markets(limit=25)
                    await engine.sxbet.fetch_active_markets(limit=20)
                    await engine.manifold.fetch_active_binary_markets(limit=25)

                # Scan strictly real orderbooks
                opps = await engine.scan_once()
                
                # Auto-Execute on profitable verified spreads
                for opp in opps:
                    if opp.est_net_profit >= CONFIG.MIN_PROFIT_HURDLE_USDC:
                        engine.execute_paper_trade(opp)

            await asyncio.sleep(CONFIG.SCAN_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error in scanner loop: {e}")
            await asyncio.sleep(3.0)

async def main():
    logger.info(f"Starting Quantitative Arbitrage Engine on http://{CONFIG.DASHBOARD_HOST}:{CONFIG.DASHBOARD_PORT}")
    
    # Start scanner loop as background task in current event loop
    asyncio.create_task(background_scanner_loop())
    
    # Start interactive 2-way Telegram Command Poller
    telegram_bot = TelegramCommandHandler(engine=engine, executor=engine.executor, ledger=ledger)
    asyncio.create_task(telegram_bot.start_polling())
    
    # Start Uvicorn web server
    config = uvicorn.Config(
        app=app,
        host=CONFIG.DASHBOARD_HOST,
        port=CONFIG.DASHBOARD_PORT,
        log_level="warning"
    )
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down engine gracefully.")
