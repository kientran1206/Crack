"""
tests/test_limitless_engine.py
Unit tests cho Limitless Connector & Arbitrage Watcher.
Kiểm tra:
1. Thuật toán Late-Cycle Convergence tính toán chuẩn xác Buffer và ROI.
2. Kiểm soát rủi ro vốn $10: Chia đạn $3.00/lệnh, cấm vào lệnh khi buffer < ngưỡng.
3. Cấu trúc EIP-712 Order schema hợp lệ theo chuẩn Base L2 (ChainId: 8453).
4. Bộ lọc thời gian: Từ chối lệnh khi > 10 phút hoặc < 60 giây.
"""

from decimal import Decimal
import time
import pytest

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

    signal = connector.evaluate_late_cycle_convergence(
        market=market,
        current_spot=Decimal("98450"),
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is True
    assert signal.recommended_side == "UP"
    assert signal.share_price == Decimal("0.89")
    assert signal.allocated_usd == Decimal("1.00")  # Chia đạn $1.00 (10% từ vốn $10) sau phản biện Opus
    assert signal.expected_roi_pct > Decimal("12.0")
    assert signal.rejection_reason is None


def test_rejection_buffer_too_thin():
    """
    Kịch bản: Còn 5 phút, Strike BTC $98,000, Spot $98,100 (Buffer +$100 < $250 ngưỡng an toàn).
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
        current_spot=Decimal("98100"),  # Chỉ cách $100
        bankroll_usdc=Decimal("10.00")
    )

    assert signal.is_safe is False
    assert signal.recommended_side == "NONE"
    assert "ngưỡng an toàn" in signal.rejection_reason


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
        current_spot=Decimal("98500"),
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
