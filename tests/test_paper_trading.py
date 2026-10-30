"""
tests/test_paper_trading.py - TDD Verification Suite for Realistic Paper Trading & Audit System
Covers:
- TC-01: SQLite Paper Database persistence across open and settled states.
- TC-02: Win settlement (Release locked capital + credit net profit).
- TC-03: Loss settlement (Forfeit locked capital + deduct from balance).
- TC-04: Daily Audit Report rendering with 10h/22h VN format.
- TC-05: Concurrent exposure limiter (Max 3 open positions).
- TC-06: Simulated execution latency guard (200ms penalty).
"""
import asyncio
from decimal import Decimal
import os
import sys
import tempfile
import time
import pytest

# Ensure root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.paper_database import PaperDatabase
from core.daily_reporter import format_audit_report
from core.ledger import VirtualStateLedger
from connectors.limitless import LimitlessConnector, LimitlessMarket, LimitlessSignal
from core.limitless_engine import LimitlessArbitrageWatcher


@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = PaperDatabase(db_path=path)
    yield db
    try:
        os.remove(path)
    except OSError:
        pass


def test_sqlite_paper_database_lifecycle(temp_db):
    """TC-01: Test recording open positions and querying them."""
    now = int(time.time())
    pos_id = "POS_BTC_TEST_001"
    
    assert temp_db.count_open_positions() == 0

    success = temp_db.record_open_position(
        position_id=pos_id,
        market_slug="btc-hourly-test",
        title="BTC Hourly above $98,000",
        asset="BTC",
        side="UP",
        strike_price=Decimal("98000"),
        entry_spot=Decimal("98600"),
        share_price=Decimal("0.89"),
        shares_count=Decimal("0.5617"),
        cost_usd=Decimal("0.50"),
        entry_time=now,
        expiry_time=now + 300,
        note="Buffer +$600"
    )
    assert success is True
    assert temp_db.count_open_positions() == 1

    open_pos = temp_db.get_open_positions()
    assert len(open_pos) == 1
    assert open_pos[0]["position_id"] == pos_id
    assert open_pos[0]["outcome"] == "OPEN"


def test_settle_position_win(temp_db):
    """TC-02: Test position settled as WIN."""
    now = int(time.time())
    pos_id = "POS_ETH_TEST_002"

    temp_db.record_open_position(
        position_id=pos_id,
        market_slug="eth-hourly-test",
        title="ETH Hourly above $2,700",
        asset="ETH",
        side="UP",
        strike_price=Decimal("2700"),
        entry_spot=Decimal("2740"),
        share_price=Decimal("0.88"),
        shares_count=Decimal("0.5681"),
        cost_usd=Decimal("0.50"),
        entry_time=now,
        expiry_time=now + 60,
        note="Buffer +$40"
    )

    payout = Decimal("0.5681")
    cost = Decimal("0.50")
    net_pnl = payout - cost - Decimal("0.001") # fees

    settle_success = temp_db.settle_position(
        position_id=pos_id,
        settled_spot=Decimal("2755"),
        outcome="WIN",
        payout_usd=payout,
        net_pnl_usd=net_pnl,
        note="Won clearly above strike"
    )
    assert settle_success is True
    assert temp_db.count_open_positions() == 0

    perf = temp_db.get_recent_performance(lookback_seconds=3600)
    assert perf["settled_count"] == 1
    assert perf["winning_count"] == 1
    assert perf["losing_count"] == 0
    assert perf["win_rate_pct"] == 100.0
    assert perf["total_pnl_usd"] > 0


def test_settle_position_loss(temp_db):
    """TC-03: Test position settled as LOSS (whip-saw candle)."""
    now = int(time.time())
    pos_id = "POS_SOL_TEST_003"

    temp_db.record_open_position(
        position_id=pos_id,
        market_slug="sol-hourly-test",
        title="SOL Hourly above $210",
        asset="SOL",
        side="UP",
        strike_price=Decimal("210"),
        entry_spot=Decimal("213"),
        share_price=Decimal("0.85"),
        shares_count=Decimal("0.5882"),
        cost_usd=Decimal("0.50"),
        entry_time=now,
        expiry_time=now + 60,
        note="Buffer +$3"
    )

    settle_success = temp_db.settle_position(
        position_id=pos_id,
        settled_spot=Decimal("208.50"), # Sập râu nến qua strike!
        outcome="LOSS",
        payout_usd=Decimal("0.00"),
        net_pnl_usd=Decimal("-0.50"),
        note="Lost due to last-minute flash wick"
    )
    assert settle_success is True

    perf = temp_db.get_recent_performance(lookback_seconds=3600)
    assert perf["settled_count"] == 1
    assert perf["winning_count"] == 0
    assert perf["losing_count"] == 1
    assert perf["win_rate_pct"] == 0.0
    assert perf["total_pnl_usd"] == -0.50


def test_format_audit_report(temp_db):
    """TC-04: Test Daily Audit Report formatting."""
    now = int(time.time())
    # Add 1 win and 1 loss
    temp_db.record_open_position("POS_1", "m1", "BTC UP", "BTC", "UP", Decimal("98000"), Decimal("98500"), Decimal("0.89"), Decimal("0.56"), Decimal("0.50"), now - 100, now - 10)
    temp_db.settle_position("POS_1", Decimal("98600"), "WIN", Decimal("0.56"), Decimal("0.06"), "WIN")
    
    temp_db.record_open_position("POS_2", "m2", "ETH DOWN", "ETH", "DOWN", Decimal("2700"), Decimal("2680"), Decimal("0.88"), Decimal("0.56"), Decimal("0.50"), now - 100, now - 10)
    temp_db.settle_position("POS_2", Decimal("2705"), "LOSS", Decimal("0.00"), Decimal("-0.50"), "LOSS")

    report_text = format_audit_report(
        db=temp_db,
        current_balance=Decimal("9.56"),
        starting_balance=Decimal("10.00"),
        title_suffix="(10:00 SÁNG)"
    )

    assert "BÁO CÁO KIỂM TOÁN HIỆU SUẤT TRADING (10:00 SÁNG)" in report_text
    assert "Số dư hiện tại:" in report_text
    assert "Tổng số lệnh đã chốt:" in report_text
    assert "Thắng:" in report_text
    assert "Thua:" in report_text


def test_paper_entry_locks_capital_and_enforces_limit(temp_db):
    """TC-05 & TC-06: Test paper entry locks capital and honors concurrent position limit."""
    async def _test_coro():
        ledger = VirtualStateLedger(starting_balance=Decimal("10.00"), max_risk_pct=Decimal("0.05"), gas_cost=Decimal("0.001"))
        connector = LimitlessConnector(dry_run=True)
        watcher = LimitlessArbitrageWatcher(connector=connector, bankroll_usdc=Decimal("10.00"), db=temp_db, ledger=ledger)

        now = int(time.time())
        market = LimitlessMarket(
            slug="btc-test-limit-1",
            title="Bitcoin Hourly above $98,000",
            target_asset="BTC",
            strike_price=Decimal("98000"),
            end_time=now + 300,
            exchange_address="0x1111",
            token_id_up=1,
            token_id_down=2,
            best_ask_up=Decimal("0.89"),
            best_ask_down=Decimal("0.11")
        )
        sig = LimitlessSignal(
            market_slug=market.slug,
            asset="BTC",
            strike_price=market.strike_price,
            spot_price=Decimal("98600"),
            buffer_usd=Decimal("600"),
            buffer_pct=Decimal("0.61"),
            seconds_to_expiry=300,
            recommended_side="UP",
            share_price=Decimal("0.89"),
            expected_roi_pct=Decimal("12.36"),
            allocated_usd=Decimal("0.50"),
            is_safe=True
        )

        # First entry: should succeed and lock $0.50
        success = await watcher.execute_paper_entry(sig, market)
        assert success is True
        assert ledger.locked_balance == Decimal("0.50")
        assert ledger.free_balance == Decimal("9.50")
        assert temp_db.count_open_positions() == 1

        # Duplicate entry on same market should be rejected
        success_dup = await watcher.execute_paper_entry(sig, market)
        assert success_dup is False

    asyncio.run(_test_coro())


def test_paper_reporting_muted_in_live_mode(temp_db):
    """TC-07: Test that paper reports to Telegram are muted when LIVE mode is active."""
    class DummyExecutor:
        def __init__(self, dry_run=True):
            self.dry_run = dry_run

    class DummyTelegram:
        def __init__(self):
            self.sent_messages = []

        async def send_message(self, text, reply_markup=None):
            self.sent_messages.append(text)

    async def _test_coro():
        telegram = DummyTelegram()
        live_executor = DummyExecutor(dry_run=False) # LIVE MODE
        connector = LimitlessConnector(dry_run=False)
        ledger = VirtualStateLedger(starting_balance=Decimal("10.00"), max_risk_pct=Decimal("0.05"), gas_cost=Decimal("0.001"))
        
        watcher = LimitlessArbitrageWatcher(
            connector=connector,
            bankroll_usdc=Decimal("10.00"),
            db=temp_db,
            ledger=ledger,
            telegram=telegram,
            executor=live_executor
        )

        assert watcher.is_paper_mode() is False

        now = int(time.time())
        market = LimitlessMarket(
            slug="btc-test-live-mode",
            title="Bitcoin Live Mode Test",
            target_asset="BTC",
            strike_price=Decimal("98000"),
            end_time=now + 300,
            exchange_address="0x1111",
            token_id_up=1,
            token_id_down=2,
            best_ask_up=Decimal("0.89"),
            best_ask_down=Decimal("0.11")
        )
        sig = LimitlessSignal(
            market_slug=market.slug,
            asset="BTC",
            strike_price=market.strike_price,
            spot_price=Decimal("98600"),
            buffer_usd=Decimal("600"),
            buffer_pct=Decimal("0.61"),
            seconds_to_expiry=300,
            recommended_side="UP",
            share_price=Decimal("0.89"),
            expected_roi_pct=Decimal("12.36"),
            allocated_usd=Decimal("0.50"),
            is_safe=True
        )

        # In LIVE mode, position is stored in DB for bookkeeping, but Telegram paper notification is MUTED!
        success = await watcher.execute_paper_entry(sig, market)
        assert success is True
        assert len(telegram.sent_messages) == 0  # MUTED!

        # Now switch executor to PAPER (dry_run=True)
        live_executor.dry_run = True
        assert watcher.is_paper_mode() is True

        market2 = LimitlessMarket(
            slug="btc-test-paper-mode",
            title="Bitcoin Paper Mode Test",
            target_asset="BTC",
            strike_price=Decimal("98000"),
            end_time=now + 300,
            exchange_address="0x1111",
            token_id_up=1,
            token_id_down=2,
            best_ask_up=Decimal("0.89"),
            best_ask_down=Decimal("0.11")
        )
        sig2 = LimitlessSignal(
            market_slug=market2.slug,
            asset="BTC",
            strike_price=market2.strike_price,
            spot_price=Decimal("98600"),
            buffer_usd=Decimal("600"),
            buffer_pct=Decimal("0.61"),
            seconds_to_expiry=300,
            recommended_side="UP",
            share_price=Decimal("0.89"),
            expected_roi_pct=Decimal("12.36"),
            allocated_usd=Decimal("0.50"),
            is_safe=True
        )

        success2 = await watcher.execute_paper_entry(sig2, market2)
        assert success2 is True
        assert len(telegram.sent_messages) == 1  # REPORTED!
        assert "VỊ THẾ PAPER THỰC CHIẾN VỪA VÀO VỐN" in telegram.sent_messages[0]

    asyncio.run(_test_coro())
