"""
core/paper_database.py - Persistent SQLite Paper Trading Ledger & Audit Database
Guarantees zero data loss across process restarts, runner respawns, and daily reports.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import logging
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List, Optional

from config import CONFIG

logger = logging.getLogger("PaperDatabase")

# Vietnam Timezone (UTC+7)
VN_TZ = timezone(timedelta(hours=7))


class PaperDatabase:
    """
    SQLite thread-safe repository for all simulated and live positions.
    """
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or CONFIG.SQLITE_DB_PATH
        # Ensure parent directory exists if specified
        parent = Path(self.db_path).parent
        if parent and str(parent) != ".":
            parent.mkdir(parents=True, exist_ok=True)
            
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS paper_positions (
                    position_id TEXT PRIMARY KEY,
                    market_slug TEXT NOT NULL,
                    title TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    side TEXT NOT NULL,
                    strike_price REAL NOT NULL,
                    entry_spot REAL NOT NULL,
                    share_price REAL NOT NULL,
                    shares_count REAL NOT NULL,
                    cost_usd REAL NOT NULL,
                    entry_time INTEGER NOT NULL,
                    expiry_time INTEGER NOT NULL,
                    settled_time INTEGER,
                    settled_spot REAL,
                    outcome TEXT NOT NULL DEFAULT 'OPEN',
                    payout_usd REAL DEFAULT 0.0,
                    net_pnl_usd REAL DEFAULT 0.0,
                    note TEXT
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_paper_status_expiry 
                ON paper_positions(outcome, expiry_time)
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS paper_daily_audits (
                    audit_id TEXT PRIMARY KEY,
                    date_str TEXT NOT NULL,
                    report_hour INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    starting_balance REAL NOT NULL,
                    current_balance REAL NOT NULL,
                    total_trades INTEGER NOT NULL,
                    winning_trades INTEGER NOT NULL,
                    losing_trades INTEGER NOT NULL,
                    win_rate_pct REAL NOT NULL,
                    net_pnl_usd REAL NOT NULL,
                    roi_pct REAL NOT NULL,
                    max_drawdown_usd REAL NOT NULL,
                    report_text TEXT NOT NULL
                )
            """)
            conn.commit()

    def record_open_position(
        self,
        position_id: str,
        market_slug: str,
        title: str,
        asset: str,
        side: str,
        strike_price: Decimal,
        entry_spot: Decimal,
        share_price: Decimal,
        shares_count: Decimal,
        cost_usd: Decimal,
        entry_time: int,
        expiry_time: int,
        note: str = ""
    ) -> bool:
        """Record a newly opened position with escrowed capital."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO paper_positions (
                        position_id, market_slug, title, asset, side,
                        strike_price, entry_spot, share_price, shares_count,
                        cost_usd, entry_time, expiry_time, outcome, note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
                """, (
                    position_id, market_slug, title, asset, side,
                    float(strike_price), float(entry_spot), float(share_price), float(shares_count),
                    float(cost_usd), entry_time, expiry_time, note
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error("Failed to record open position: %s", e)
            return False

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Retrieve all currently active positions awaiting settlement."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM paper_positions WHERE outcome = 'OPEN' ORDER BY expiry_time ASC")
            return [dict(row) for row in cursor.fetchall()]

    def count_open_positions(self) -> int:
        """Count active positions for concurrent exposure limit."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM paper_positions WHERE outcome = 'OPEN'")
            return cursor.fetchone()[0]

    def settle_position(
        self,
        position_id: str,
        settled_spot: Decimal,
        outcome: str,  # 'WIN' or 'LOSS'
        payout_usd: Decimal,
        net_pnl_usd: Decimal,
        note: str = ""
    ) -> bool:
        """Settle a position upon candle expiry using live Oracle settlement data."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE paper_positions
                    SET settled_time = ?,
                        settled_spot = ?,
                        outcome = ?,
                        payout_usd = ?,
                        net_pnl_usd = ?,
                        note = ?
                    WHERE position_id = ?
                """, (
                    int(time.time()),
                    float(settled_spot),
                    outcome,
                    float(payout_usd),
                    float(net_pnl_usd),
                    note,
                    position_id
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error("Failed to settle position %s: %s", position_id, e)
            return False

    def get_recent_performance(self, lookback_seconds: int = 86400) -> Dict[str, Any]:
        """Aggregate performance metrics over a rolling period (default 24h)."""
        cutoff = int(time.time()) - lookback_seconds
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM paper_positions 
                WHERE (entry_time >= ? OR settled_time >= ?)
                ORDER BY entry_time DESC
            """, (cutoff, cutoff))
            rows = [dict(r) for r in cursor.fetchall()]

        total_settled = [r for r in rows if r["outcome"] in ("WIN", "LOSS")]
        winning = [r for r in total_settled if r["outcome"] == "WIN"]
        losing = [r for r in total_settled if r["outcome"] == "LOSS"]
        open_pos = [r for r in rows if r["outcome"] == "OPEN"]

        total_pnl = sum(Decimal(str(r["net_pnl_usd"])) for r in total_settled)
        total_cost = sum(Decimal(str(r["cost_usd"])) for r in total_settled)
        
        win_rate = (len(winning) / len(total_settled) * 100) if total_settled else 0.0

        # Calculate max drawdown
        running_pnl = Decimal("0")
        peak_pnl = Decimal("0")
        max_dd = Decimal("0")
        for r in reversed(total_settled):
            running_pnl += Decimal(str(r["net_pnl_usd"]))
            if running_pnl > peak_pnl:
                peak_pnl = running_pnl
            dd = peak_pnl - running_pnl
            if dd > max_dd:
                max_dd = dd

        return {
            "total_records": len(rows),
            "open_count": len(open_pos),
            "settled_count": len(total_settled),
            "winning_count": len(winning),
            "losing_count": len(losing),
            "win_rate_pct": win_rate,
            "total_pnl_usd": float(total_pnl),
            "total_volume_usd": float(total_cost),
            "max_drawdown_usd": float(max_dd),
            "open_positions": open_pos,
            "recent_settled": total_settled[:15]
        }

    def record_daily_audit(
        self,
        audit_id: str,
        date_str: str,
        report_hour: int,
        starting_balance: float,
        current_balance: float,
        perf: Dict[str, Any],
        report_text: str
    ):
        """Persist generated audit report for traceability."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO paper_daily_audits (
                        audit_id, date_str, report_hour, timestamp,
                        starting_balance, current_balance, total_trades,
                        winning_trades, losing_trades, win_rate_pct,
                        net_pnl_usd, roi_pct, max_drawdown_usd, report_text
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    audit_id, date_str, report_hour, int(time.time()),
                    starting_balance, current_balance, perf["settled_count"],
                    perf["winning_count"], perf["losing_count"], perf["win_rate_pct"],
                    perf["total_pnl_usd"],
                    ((current_balance - starting_balance) / starting_balance * 100) if starting_balance > 0 else 0.0,
                    perf["max_drawdown_usd"], report_text
                ))
                conn.commit()
        except Exception as e:
            logger.error("Failed to persist daily audit: %s", e)
