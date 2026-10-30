"""
prediction_bot/limitless_cli.py
Giao diện dòng lệnh (CLI) chạy thử nghiệm giám sát và săn kèo Limitless Exchange thời gian thực.
"""

import asyncio
from decimal import Decimal
import logging
import sys

from prediction_bot.connectors.limitless import LimitlessConnector, LimitlessSignal
from prediction_bot.core.limitless_engine import LimitlessArbitrageWatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("limitless_cli")


def on_opportunity_detected(sig: LimitlessSignal):
    print("\n" + "="*70)
    print("🔥 [PHÁT HIỆN KÈO THƠM LIMITLESS - LATE CYCLE CONVERGENCE]")
    print(f"📌 Thị trường: {sig.market_slug} ({sig.asset})")
    print(f"🎯 Mốc Strike: ${sig.strike_price} | Giá Spot hiện tại: ${sig.spot_price}")
    print(f"📏 Khoảng cách an toàn (Buffer): ${sig.buffer_usd:+.2f} ({sig.buffer_pct:+.2f}%)")
    print(f"⏳ Thời gian còn lại: {sig.seconds_to_expiry} giây ({sig.seconds_to_expiry//60} phút)")
    print(f"🚀 Cửa cược khuyên dùng: {sig.recommended_side}")
    print(f"💵 Giá share mua vào: {sig.share_price} USDC")
    print(f"📈 Lợi nhuận kỳ vọng (ROI): +{sig.expected_roi_pct:.2f}%")
    print(f"💰 Vốn giải ngân (30% bankroll): ${sig.allocated_usd:.2f} USDC")
    print("="*70 + "\n")


async def main():
    print("""
    ===============================================================
    🚀 ANTIGRAVITY LIMITLESS ARBITRAGE WATCHER (BASE L2)
    - Tối ưu cho tài khoản vốn nhỏ: $10.00 USDC
    - Cơ chế: Săn lệch giá nến 1H ở 5 phút cuối (Late-Cycle)
    - Giao thức: Ký EIP-712 off-chain (Maker 0% Gas)
    - Kết nối song song: Binance WebSocket (Spot) + Limitless API
    ===============================================================
    """)
    connector = LimitlessConnector(dry_run=True)
    watcher = LimitlessArbitrageWatcher(
        connector=connector,
        bankroll_usdc=Decimal("10.00"),
        on_signal_callback=on_opportunity_detected
    )

    try:
        await watcher.run_binance_stream()
    except KeyboardInterrupt:
        logger.info("Đang dừng watcher...")
        watcher.stop()


if __name__ == "__main__":
    asyncio.run(main())
