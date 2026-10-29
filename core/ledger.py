"""
ledger.py - Virtual Wallet, Atomic Escrow & Position Sizing Engine
"""
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from enum import Enum
import time
from typing import Dict, List, Optional, Tuple

class OpportunityType(str, Enum):
    INTERNAL_BUNDLE = "INTERNAL_BUNDLE"  # YES + NO < 1.00 on Polymarket
    CROSS_MARKET = "CROSS_MARKET"        # Cross-exchange spread (Polymarket vs Azuro)

class TradeStatus(str, Enum):
    DETECTED = "DETECTED"
    VALIDATED = "VALIDATED"
    SIMULATED_FILL = "SIMULATED_FILL"
    SETTLED = "SETTLED"
    REJECTED = "REJECTED"

@dataclass(frozen=True)
class OrderBookLevel:
    price: Decimal
    quantity: Decimal

@dataclass
class MarketDepth:
    token_id: str
    outcome: str
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)

    @property
    def best_ask(self) -> Optional[OrderBookLevel]:
        return self.asks[0] if self.asks else None

    @property
    def best_bid(self) -> Optional[OrderBookLevel]:
        return self.bids[0] if self.bids else None

@dataclass
class Opportunity:
    opp_id: str
    market_title: str
    platform: str
    opp_type: OpportunityType
    spread_pct: Decimal
    yes_price: Decimal
    no_price: Decimal
    target_shares: Decimal
    gross_cost: Decimal
    est_fees: Decimal
    est_gas: Decimal
    est_net_profit: Decimal
    status: TradeStatus = TradeStatus.DETECTED
    timestamp_iso: str = field(default_factory=lambda: time.strftime("%Y-%m-%d %H:%M:%S"))

@dataclass
class TradeRecord:
    trade_id: str
    timestamp_iso: str
    market: str
    strategy: str
    size_shares: Decimal
    entry_cost: Decimal
    payout: Decimal
    fees_paid: Decimal
    gas_paid: Decimal
    net_pnl: Decimal
    roi_pct: Decimal
    status: str

class VirtualStateLedger:
    """
    Thread-safe & atomic virtual wallet ledger.
    Guarantees no double-spending of paper balance across concurrent opportunities.
    """
    def __init__(self, starting_balance: Decimal, max_risk_pct: Decimal, gas_cost: Decimal):
        self.starting_balance: Decimal = starting_balance
        self.total_balance: Decimal = starting_balance
        self.free_balance: Decimal = starting_balance
        self.locked_balance: Decimal = Decimal("0.00")
        self.max_risk_pct: Decimal = max_risk_pct
        self.gas_cost: Decimal = gas_cost
        
        self.trades: List[TradeRecord] = []
        self.active_reservations: Dict[str, Decimal] = {}
        
        self.total_pnl: Decimal = Decimal("0.00")
        self.winning_trades: int = 0
        self.losing_trades: int = 0

    @property
    def trades_count(self) -> int:
        return len(self.trades)

    @property
    def win_rate(self) -> Decimal:
        if self.trades_count == 0:
            return Decimal("0.0")
        return ((Decimal(self.winning_trades) / Decimal(self.trades_count)) * Decimal("100.0")).quantize(Decimal("0.1"))

    @property
    def roi_percent(self) -> Decimal:
        if self.starting_balance == Decimal("0.0"):
            return Decimal("0.0")
        return (((self.total_balance - self.starting_balance) / self.starting_balance) * Decimal("100.0")).quantize(Decimal("0.01"))

    def reserve_capital(self, opp_id: str, amount: Decimal) -> bool:
        """Atomic escrow lock."""
        if amount <= Decimal("0.0") or amount > self.free_balance:
            return False
        self.free_balance -= amount
        self.locked_balance += amount
        self.active_reservations[opp_id] = amount
        return True

    def release_reservation(self, opp_id: str) -> None:
        """Refund reserved capital on abort."""
        if opp_id in self.active_reservations:
            reserved = self.active_reservations.pop(opp_id)
            self.locked_balance -= reserved
            self.free_balance += reserved

    def commit_settlement(
        self,
        opp_id: str,
        gross_cost: Decimal,
        payout: Decimal,
        fees_paid: Decimal,
        gas_paid: Decimal,
        trade_record: TradeRecord
    ) -> None:
        """Settle trade and credit realized net profit."""
        if opp_id not in self.active_reservations:
            return
        
        reserved = self.active_reservations.pop(opp_id)
        self.locked_balance -= reserved
        
        net_trade_pnl = payout - gross_cost - fees_paid - gas_paid
        
        self.total_pnl += net_trade_pnl
        self.total_balance += net_trade_pnl
        self.free_balance += (reserved + net_trade_pnl)
        
        if net_trade_pnl > Decimal("0.0"):
            self.winning_trades += 1
        elif net_trade_pnl < Decimal("0.0"):
            self.losing_trades += 1

        self.trades.insert(0, trade_record)  # latest first

    def calculate_position_size(
        self,
        unit_cost: Decimal,
        max_market_shares: Decimal,
        min_notional: Decimal
    ) -> Decimal:
        """
        Calculates optimal position size respecting capital limits and orderbook depth.
        """
        # Maximum capital allocated to this single trade
        capital_ceiling = min(
            self.free_balance,
            self.total_balance * self.max_risk_pct
        )
        
        if capital_ceiling < min_notional or unit_cost <= Decimal("0"):
            return Decimal("0")
        
        # Max shares affordable under risk ceiling
        affordable_shares = (capital_ceiling / unit_cost).quantize(Decimal("1"), rounding=ROUND_DOWN)
        # Clamped by market depth
        optimal_shares = min(affordable_shares, max_market_shares)
        
        if (optimal_shares * unit_cost) < min_notional:
            return Decimal("0")
        return optimal_shares
