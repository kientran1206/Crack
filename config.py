"""
config.py - Quantitative Arbitrage Engine Configuration
"""
from dataclasses import dataclass
from decimal import Decimal
import os
from pathlib import Path

# Load .env natively without external dependencies
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    with open(_env_path, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())


@dataclass
class EngineConfig:
    # Portfolio & Risk Limits (Optimized for Micro-Bankroll Bootstrap $10)
    STARTING_BALANCE_USDC: Decimal = Decimal(os.getenv("STARTING_BALANCE_USDC", "10.00"))
    MAX_RISK_PER_TRADE_PCT: Decimal = Decimal("0.05")  # 5% max risk per trade ($0.50 on $10)
    MIN_TRADE_NOTIONAL_USDC: Decimal = Decimal("0.20") # Micro-ticket minimum: 20 cents
    MIN_PROFIT_HURDLE_USDC: Decimal = Decimal("0.005")
    MIN_SPREAD_PCT: Decimal = Decimal("1.0")
    
    # Cost & Execution Mechanics (Zero-Gas Priority Routing)
    POLYGON_GAS_PER_TX_USDC: Decimal = Decimal("0.005")
    TAKER_FEE_FACTOR: Decimal = Decimal("0.02")
    ZERO_GAS_PRIORITY_ONLY: bool = True  # Strictly route through Maker 0% Gas (Limitless) or Gasless Relayer (Polymarket)
    
    # Web3 & Live Mode Configuration
    DRY_RUN: bool = os.getenv("DRY_RUN", "True").lower() in ("true", "1", "yes")
    PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
    COLD_WALLET_ADDRESS: str = os.getenv("COLD_WALLET_ADDRESS", "")
    MIN_POL_GAS_RESERVE: Decimal = Decimal("0.05")
    
    # Telegram Automated Reporting & Command Control
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_USER_ID: int = int(os.getenv("TELEGRAM_USER_ID", "1423475386"))
    
    # Quantitative Risk & Legging Safeguards
    MIN_GROSS_SPREAD_PCT: Decimal = Decimal("1.5")
    MIN_PROFIT_HURDLE_USDC: Decimal = Decimal("0.02")
    TIMEOUT_LEGGING_MS: int = 1200
    
    # Network Endpoints
    POLYMARKET_GAMMA_API: str = "https://gamma-api.polymarket.com"
    POLYMARKET_CLOB_REST: str = "https://clob.polymarket.com"
    POLYMARKET_CLOB_WS: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    AZURO_ONCHAIN_FEED_API: str = "https://api.onchainfeed.org/api/v1/public"
    
    # Server / Dashboard
    DASHBOARD_HOST: str = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    DASHBOARD_PORT: int = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8888")))
    SCAN_INTERVAL_SECONDS: float = float(os.getenv("SCAN_INTERVAL_SECONDS", "0.2"))

CONFIG = EngineConfig()
