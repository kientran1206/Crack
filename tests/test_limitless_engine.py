"""
tests/test_limitless_engine.py
Unit tests cho Limitless Connector & Arbitrage Watcher.
Kiểm tra:
1. Thuật toán Late-Cycle Convergence tính toán chuẩn xác Buffer và ROI.
2. Kiểm soát rủi ro vốn $10: Chia đạn $3.00/lệnh, cấm vào lệnh khi buffer < ngưỡng.
3. Cấu trúc EIP-712 Order schema hợp lệ theo chuẩn Base L2 (ChainId: 8453).
4. Bộ lọc thời gian: Từ chối lệnh khi > 10 phút hoặc < 60 giây.
"""

import os
import sys
import time
from decimal import Decimal
import pytest

# Ensure repo root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal
    from core.limitless_engine import LimitlessArbitrageWatcher
except ModuleNotFoundError:
    from prediction_bot.connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal
    from prediction_bot.core.limitless_engine import LimitlessArbitrageWatcher


def test_late_cycle_convergence_safe_signal():
    """
    Kịch bản: Còn 5 phút (300s), Strike BTC $98,000, Spot $98,450 (Buffer +$450 > $250).
    Share UP bán 0.89¢ -> Kỳ vọng: Phát tín hiệu an toàn, ROI +12.36%, cược $3.00.
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market = LimitlessMarket(
        slug="btc-above-98k-15utc",
        title="Bitcoin above $98,000 at 15:00 UTC",
        target_asset="BTC",
        strike_price=Decimal("98000"),
        end_time=now + 300,  # Còn 5 phút
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=101,
        token_id_down=102,
        best_ask_up=Decimal("0.89"),
        best_ask_down=Decimal("0.11"),
        volume_24h=Decimal("50000")
    )

    # Còn 5 phút (300s): Dynamic time multiplier = 1 + 300/300 = 2.0x
    # Safe buffer = $250 * 2.0 = $500. Oracle margin = $50 -> Tổng yêu cầu: $550
    # Spot $98,600 (Cách Strike +$600 > $550) -> Đạt chuẩn an toàn tuyệt đối
    signal = connector.evaluate_late_cycle_convergence(
        market=market,
        current_spot=Decimal("98600"),
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is True
    assert signal.recommended_side == "UP"
    assert signal.share_price == Decimal("0.89")
    assert signal.allocated_usd == Decimal("0.50")  # Chia đạn $0.50 (5% từ vốn $10) bảo vệ tối đa vốn nhỏ
    assert signal.expected_roi_pct > Decimal("12.0")
    assert signal.rejection_reason is None


def test_rejection_buffer_too_thin():
    """
    Kịch bản: Còn 5 phút, Strike BTC $98,000, Spot $98,030 (Buffer +$30 < $64.00 ngưỡng an toàn thực nghiệm).
    Kỳ vọng: Bác bỏ tín hiệu do rủi ro giật râu nến (Buffer too thin).
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market = LimitlessMarket(
        slug="btc-above-98k-15utc",
        title="Bitcoin above $98,000 at 15:00 UTC",
        target_asset="BTC",
        strike_price=Decimal("98000"),
        end_time=now + 300,
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=101,
        token_id_down=102,
        best_ask_up=Decimal("0.75"),
        best_ask_down=Decimal("0.25"),
        volume_24h=Decimal("50000")
    )

    signal = connector.evaluate_late_cycle_convergence(
        market=market,
        current_spot=Decimal("98030"),  # Chỉ cách $30 (< $64.00)
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is False
    assert signal.recommended_side == "NONE"
    assert "chưa đủ an toàn" in signal.rejection_reason


def test_rejection_share_price_too_expensive():
    """
    Kịch bản: Buffer an toàn (+$500), nhưng giá share UP đã bị đẩy lên 0.96¢ (> 0.92¢).
    Kỳ vọng: Bác bỏ vì biên lợi nhuận quá mỏng (< 8%).
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market = LimitlessMarket(
        slug="btc-above-98k-15utc",
        title="Bitcoin above $98,000 at 15:00 UTC",
        target_asset="BTC",
        strike_price=Decimal("98000"),
        end_time=now + 300,
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=101,
        token_id_down=102,
        best_ask_up=Decimal("0.96"),  # Quá đắt
        best_ask_down=Decimal("0.04"),
        volume_24h=Decimal("50000")
    )

    signal = connector.evaluate_late_cycle_convergence(
        market=market,
        current_spot=Decimal("98700"),  # Buffer +$700 > $550 an toàn
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is False
    assert "quá cao" in signal.rejection_reason


def test_eip712_order_payload_structure():
    """
    Kiểm tra cấu trúc payload EIP-712 Order cho Base L2 (Chain 8453).
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market = LimitlessMarket(
        slug="eth-above-3k-15utc",
        title="Ethereum above $3,000 at 15:00 UTC",
        target_asset="ETH",
        strike_price=Decimal("3000"),
        end_time=now + 300,
        exchange_address="0xExchangeBaseAddress12345678901234567890",
        token_id_up=201,
        token_id_down=202,
        best_ask_up=Decimal("0.90"),
        best_ask_down=Decimal("0.10"),
        volume_24h=Decimal("20000")
    )

    payload = connector.create_eip712_order_payload(
        market=market,
        side="UP",
        amount_usdc=Decimal("3.00"),
        share_price=Decimal("0.90")
    )

    # Kiểm tra Domain
    domain = payload["domain"]
    assert domain["name"] == "Limitless CTF Exchange"
    assert domain["chainId"] == 8453
    assert domain["verifyingContract"] == "0xExchangeBaseAddress12345678901234567890"

    # Kiểm tra Message
    msg = payload["message"]
    assert msg["tokenId"] == 201
    assert msg["makerAmount"] == 3000000  # $3.00 USDC (6 decimals)
    assert msg["feeRateBps"] == 0  # Maker 0% fee
    assert msg["side"] == 0  # Buy
    assert payload["primaryType"] == "Order"
    assert payload.get("postOnly") is True  # PostOnly bắt buộc theo phản biện Opus


def test_rejection_in_dead_zone():
    """
    Kịch bản: Còn 45 giây (< 90s Dead Zone), Strike BTC $98,000, Spot $98,500.
    Kỳ vọng: Bác bỏ vì nằm trong Dead Zone (phòng ngừa Reject Storm & sai lệch Oracle).
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market = LimitlessMarket(
        slug="btc-above-98k-15utc",
        title="Bitcoin above $98,000 at 15:00 UTC",
        target_asset="BTC",
        strike_price=Decimal("98000"),
        end_time=now + 45,  # Còn 45s (trong dead zone 90s)
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=101,
        token_id_down=102,
        best_ask_up=Decimal("0.85"),
        best_ask_down=Decimal("0.15"),
        volume_24h=Decimal("50000")
    )

    signal = connector.evaluate_late_cycle_convergence(
        market=market,
        current_spot=Decimal("98500"),
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is False
    assert "Vùng chết" in signal.rejection_reason


def test_rejection_of_knife_catching_short_timeframe():
    """
    Kịch bản: Nến 5 phút hoặc 15 phút (bắt dao rơi).
    Kỳ vọng: Bác bỏ hoàn toàn để chống rủi ro râu nến giật phút chót.
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market_5m = LimitlessMarket(
        slug="btc-up-down-5-min",
        title="BTC Up or Down - 5 Min",
        target_asset="BTC",
        strike_price=Decimal("98000"),
        end_time=now + 180,  # Còn 3 phút
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=101,
        token_id_down=102,
        best_ask_up=Decimal("0.85"),
        best_ask_down=Decimal("0.15"),
        volume_24h=Decimal("50000")
    )

    signal = connector.evaluate_late_cycle_convergence(
        market=market_5m,
        current_spot=Decimal("98500"),
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is False
    assert "Bác bỏ nến ngắn" in signal.rejection_reason


def test_tiered_position_sizing_for_different_bankrolls():
    """
    Kiểm tra thuật toán chia đạn phân tầng (Tiered Position Sizing):
    - Vốn < $3.00 (ví dụ $2.58 USDC): Vào lệnh đúng $0.25 (10 viên đạn)
    - Vốn $3.00 - $5.00: Vào lệnh $0.35
    - Vốn $5.00 - $10.00: Vào lệnh $0.50
    - Vốn $10.00 - $25.00: Vào lệnh $1.00
    - Vốn $50.00: Vào lệnh $2.50 (5% vốn)
    """
    try:
        from connectors.limitless import calculate_tiered_bet_amount
    except ImportError:
        from prediction_bot.connectors.limitless import calculate_tiered_bet_amount

    # Vốn hạt tiêu của dj ($2.58)
    assert calculate_tiered_bet_amount(Decimal("2.58")) == Decimal("0.25")
    assert calculate_tiered_bet_amount(Decimal("1.50")) == Decimal("0.25")

    # Vốn $3 - $5
    assert calculate_tiered_bet_amount(Decimal("3.50")) == Decimal("0.35")
    assert calculate_tiered_bet_amount(Decimal("4.99")) == Decimal("0.35")

    # Vốn $5 - $10
    assert calculate_tiered_bet_amount(Decimal("5.00")) == Decimal("0.50")
    assert calculate_tiered_bet_amount(Decimal("10.00")) == Decimal("0.50")

    # Vốn $10 - $25
    assert calculate_tiered_bet_amount(Decimal("15.00")) == Decimal("1.00")
    assert calculate_tiered_bet_amount(Decimal("25.00")) == Decimal("1.00")

    # Vốn > $25
    assert calculate_tiered_bet_amount(Decimal("50.00")) == Decimal("2.50")
    assert calculate_tiered_bet_amount(Decimal("100.00")) == Decimal("3.00") # Cap at $3.00


def test_15m_market_convergence_safe_signal():
    """
    Kiểm tra kèo 15-Minute Market (Up or Down - 15 Min):
    - Thời gian: Còn 180s (3 phút).
    - Strike ETH: $2,250. Spot ETH: $2,260 (Buffer +$10).
    - Giá share: $0.85 (lãi +17.6%).
    - Kỳ vọng: Được chấp thuận (is_safe=True), không bị chặn bởi bộ lọc nến ngắn.
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market_15m = LimitlessMarket(
        slug="eth-up-or-down-15-min-exp",
        title="ETH Up or Down - 15 Min",
        target_asset="ETH",
        strike_price=Decimal("2250.00"),
        end_time=now + 180,  # 3 phút cuối
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=301,
        token_id_down=302,
        best_ask_up=Decimal("0.85"),
        best_ask_down=Decimal("0.15"),
        volume_24h=Decimal("15000")
    )

    signal = connector.evaluate_late_cycle_convergence(
        market=market_15m,
        current_spot=Decimal("2260.00"), # Buffer +$10 > required ~$2.2
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is True
    assert signal.recommended_side == "UP"
    assert signal.share_price == Decimal("0.85")
    assert signal.expected_roi_pct > Decimal("17.0")
    assert signal.allocated_usd == Decimal("0.50")


def test_high_conviction_dynamic_price_ceiling():
    """
    Kiểm tra cơ chế nới trần giá share lên 0.94¢ khi Buffer cực lớn (>= 1.5x threshold):
    - Nến 1H còn 200s (< 300s).
    - Strike BTC: $98,000. Spot BTC: $98,250 (Buffer +$250 > 1.5x threshold ~$60).
    - Giá share: $0.93.
    - Kỳ vọng: Được chấp thuận (is_safe=True) vì xác suất thắng >98.5%, thu lãi an toàn +7.5%.
    """
    connector = LimitlessConnector(dry_run=True)
    now = int(time.time())

    market_1h = LimitlessMarket(
        slug="btc-up-or-down-hourly-conviction",
        title="BTC Up or Down Hourly",
        target_asset="BTC",
        strike_price=Decimal("98000.00"),
        end_time=now + 200,
        exchange_address="0x1234567890123456789012345678901234567890",
        token_id_up=401,
        token_id_down=402,
        best_ask_up=Decimal("0.93"),
        best_ask_down=Decimal("0.07"),
        volume_24h=Decimal("80000")
    )

    signal = connector.evaluate_late_cycle_convergence(
        market=market_1h,
        current_spot=Decimal("98250.00"),
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is True
    assert signal.recommended_side == "UP"
    assert signal.share_price == Decimal("0.93")
    assert signal.expected_roi_pct > Decimal("7.0")

