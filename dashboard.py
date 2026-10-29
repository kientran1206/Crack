"""
dashboard.py - FastAPI + Server-Sent Events (SSE) Real-Time Control & Telemetry Dashboard
"""
import asyncio
from datetime import datetime
from decimal import Decimal
import json
import logging
from typing import AsyncGenerator
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from config import CONFIG
from core.ledger import VirtualStateLedger
from core.engine import QuantitativeArbitrageEngine

logger = logging.getLogger("Dashboard")

# Initialize Ledger & Engine
ledger = VirtualStateLedger(
    starting_balance=CONFIG.STARTING_BALANCE_USDC,
    max_risk_pct=CONFIG.MAX_RISK_PER_TRADE_PCT,
    gas_cost=CONFIG.POLYGON_GAS_PER_TX_USDC
)
engine = QuantitativeArbitrageEngine(ledger=ledger)

app = FastAPI(title="Prediction Market Arbitrage Dashboard")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>VANTA // QUANTITATIVE ARBITRAGE TERMINAL</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&display=swap');
    body { font-family: 'JetBrains Mono', monospace; background-color: #07090e; color: #cbd5e1; }
    .glow-green { box-shadow: 0 0 15px rgba(16, 185, 129, 0.2); }
    .glow-blue { box-shadow: 0 0 15px rgba(56, 189, 248, 0.2); }
    .glow-amber { box-shadow: 0 0 15px rgba(245, 158, 11, 0.2); }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0b0f17; }
    ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #334155; }
  </style>
</head>
<body class="p-4 md:p-6 min-h-screen">
  <div class="max-w-7xl mx-auto space-y-6">

    <!-- TOP BAR -->
    <header class="flex flex-col md:flex-row justify-between items-start md:items-center bg-[#0d121d] border border-slate-800 p-4 rounded-xl">
      <div class="flex items-center space-x-3">
        <div class="w-3 h-3 bg-emerald-400 rounded-full animate-ping"></div>
        <div>
          <h1 class="text-xl font-extrabold tracking-tight text-white flex items-center space-x-2">
            <span class="text-emerald-400">[V]</span>
            <span>QUANTITATIVE ARBITRAGE ENGINE</span>
            <span class="text-xs px-2 py-0.5 bg-emerald-950/80 text-emerald-400 border border-emerald-800/60 rounded">PAPER TRADING LIVE</span>
          </h1>
          <p class="text-xs text-slate-400 mt-0.5">Polymarket CLOB & Azuro Liquidity Protocol // Auto-Execution & Capital Sizing</p>
        </div>
      </div>
      
      <div class="flex items-center space-x-4 mt-3 md:mt-0 text-xs">
        <div class="flex items-center space-x-2 bg-slate-900 px-3 py-1.5 rounded-lg border border-slate-800">
          <span class="text-slate-400">LATENCY:</span>
          <span id="latency" class="font-bold text-emerald-400">0.0 ms</span>
        </div>
        <div class="flex items-center space-x-2 bg-slate-900 px-3 py-1.5 rounded-lg border border-slate-800">
          <span class="text-slate-400">SCANS:</span>
          <span id="scan-count" class="font-bold text-sky-400">0</span>
        </div>
        <div class="flex items-center space-x-2 bg-slate-900 px-3 py-1.5 rounded-lg border border-slate-800">
          <span class="w-2 h-2 rounded-full bg-emerald-500"></span>
          <span id="sse-status" class="font-semibold text-emerald-400">STREAMING</span>
        </div>
      </div>
    </header>

    <!-- METRICS GRID -->
    <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
      <!-- Card 1 -->
      <div class="bg-[#0d121d] border border-slate-800/80 p-3.5 rounded-xl">
        <div class="text-[11px] font-semibold text-slate-400">VÍ ẢO (VIRTUAL BALANCE)</div>
        <div id="balance" class="text-xl md:text-2xl font-black text-white mt-1">$10.00</div>
        <div class="text-[10px] text-slate-500 mt-0.5">Free: <span id="free-bal" class="text-slate-300 font-semibold">$10.00</span></div>
      </div>
      <!-- Card 2 -->
      <div class="bg-[#0d121d] border border-slate-800/80 p-3.5 rounded-xl glow-green">
        <div class="text-[11px] font-semibold text-slate-400">LÃI RÒNG (TOTAL P&L)</div>
        <div id="pnl" class="text-xl md:text-2xl font-black text-emerald-400 mt-1">+$0.000</div>
        <div class="text-[10px] text-emerald-500/80 mt-0.5">ROI: <span id="roi" class="font-bold">0.00%</span></div>
      </div>
      <!-- Card 3 -->
      <div class="bg-[#0d121d] border border-slate-800/80 p-3.5 rounded-xl">
        <div class="text-[11px] font-semibold text-slate-400">LỆNH ĐÃ KHỚP</div>
        <div id="trades-count" class="text-xl md:text-2xl font-black text-sky-400 mt-1">0</div>
        <div class="text-[10px] text-slate-500 mt-0.5">Tỉ lệ thắng: <span id="win-rate" class="text-sky-300 font-bold">100%</span></div>
      </div>
      <!-- Card 4 -->
      <div class="bg-[#0d121d] border border-slate-800/80 p-3.5 rounded-xl">
        <div class="text-[11px] font-semibold text-slate-400">THỊ TRƯỜNG THEO DÕI</div>
        <div id="markets-count" class="text-xl md:text-2xl font-black text-indigo-400 mt-1">0</div>
        <div class="text-[10px] text-slate-500 mt-0.5">Polymarket + Azuro</div>
      </div>
      <!-- Card 5 -->
      <div class="bg-[#0d121d] border border-slate-800/80 p-3.5 rounded-xl glow-amber">
        <div class="text-[11px] font-semibold text-slate-400">KÈO LỆCH PHÁT HIỆN</div>
        <div id="opps-count" class="text-xl md:text-2xl font-black text-amber-400 mt-1">0</div>
        <div class="text-[10px] text-amber-500/80 mt-0.5">Sẵn sàng vào lệnh</div>
      </div>
      <!-- Card 6: Quick Controls -->
      <div class="bg-[#0d121d] border border-slate-800/80 p-3 rounded-xl flex flex-col justify-between">
        <div class="text-[11px] font-semibold text-slate-400">ĐIỀU KHIỂN BOT</div>
        <div class="flex space-x-1.5 mt-1">
          <button id="btn-toggle" onclick="toggleEngine()" class="flex-1 py-1 px-2 bg-emerald-600 hover:bg-emerald-500 text-black font-bold text-xs rounded transition">
            PAUSE
          </button>
          <button onclick="resetBalance()" class="py-1 px-2 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs rounded transition" title="Reset về $10">
            RESET
          </button>
        </div>
      </div>
    </div>

    <!-- MAIN TWO-COLUMN SECTION -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

      <!-- LEFT: ACTIVE ARBITRAGE OPPORTUNITIES (7 COLS) -->
      <div class="lg:col-span-7 bg-[#0d121d] border border-slate-800 rounded-xl p-4 flex flex-col h-[480px]">
        <div class="flex justify-between items-center pb-3 border-b border-slate-800/80 mb-3">
          <div class="flex items-center space-x-2">
            <span class="w-2 h-2 bg-amber-400 rounded-full"></span>
            <h2 class="text-sm font-bold text-slate-200">KÈO CHÊNH LỆCH ĐANG MỞ (LIVE ARBITRAGE)</h2>
          </div>
          <span class="text-[11px] text-slate-500">Auto-Refreshed via CLOB</span>
        </div>

        <div class="flex-1 overflow-y-auto pr-1">
          <table class="w-full text-left text-xs">
            <thead class="text-slate-400 border-b border-slate-800/60 sticky top-0 bg-[#0d121d]">
              <tr>
                <th class="pb-2 font-medium">THỊ TRƯỜNG</th>
                <th class="pb-2 font-medium">SÀN / LOẠI</th>
                <th class="pb-2 font-medium">GIÁ CẶP</th>
                <th class="pb-2 font-medium">CHÊNH LỆCH</th>
                <th class="pb-2 font-medium text-right">LÃI DỰ KIẾN</th>
              </tr>
            </thead>
            <tbody id="opps-body" class="divide-y divide-slate-800/50 text-slate-300">
              <tr>
                <td colspan="5" class="py-12 text-center text-slate-600">Đang quét sổ lệnh từ Polymarket & Azuro...</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- RIGHT: EXECUTED PAPER TRADES LEDGER (5 COLS) -->
      <div class="lg:col-span-5 bg-[#0d121d] border border-slate-800 rounded-xl p-4 flex flex-col h-[480px]">
        <div class="flex justify-between items-center pb-3 border-b border-slate-800/80 mb-3">
          <div class="flex items-center space-x-2">
            <span class="w-2 h-2 bg-emerald-400 rounded-full"></span>
            <h2 class="text-sm font-bold text-slate-200">SỔ CÁI KHỚP LỆNH ẢO (PAPER TRADES)</h2>
          </div>
          <span class="text-[11px] text-emerald-400 font-semibold" id="recent-trade-status">Auto-Compound ON</span>
        </div>

        <div class="flex-1 overflow-y-auto pr-1">
          <table class="w-full text-left text-xs">
            <thead class="text-slate-400 border-b border-slate-800/60 sticky top-0 bg-[#0d121d]">
              <tr>
                <th class="pb-2 font-medium">GIỜ</th>
                <th class="pb-2 font-medium">KÈO</th>
                <th class="pb-2 font-medium">VỐN VÀO</th>
                <th class="pb-2 font-medium text-right">TIỀN LỜI</th>
              </tr>
            </thead>
            <tbody id="trades-body" class="divide-y divide-slate-800/50 text-slate-300">
              <tr>
                <td colspan="4" class="py-12 text-center text-slate-600">Chưa có giao dịch. Bot sẽ tự khớp khi có kèo đủ tiêu chuẩn.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

    </div>

    <!-- TERMINAL LOG FEED -->
    <div class="bg-[#0b0f17] border border-slate-800/90 rounded-xl p-4">
      <div class="flex justify-between items-center pb-2.5 border-b border-slate-800 text-xs">
        <div class="flex items-center space-x-2 text-slate-400">
          <span class="text-emerald-400 font-bold">&gt;_</span>
          <span class="font-bold text-slate-200">SYSTEM TELEMETRY LOG</span>
        </div>
        <span class="text-[11px] text-slate-500">Event Stream: Real-Time</span>
      </div>
      <div id="log-feed" class="mt-2.5 h-36 overflow-y-auto font-mono text-xs space-y-1 text-slate-400 pr-1">
        <div>[00:00:00] Engine initiated. Connecting to decentralized liquidity streams...</div>
      </div>
    </div>

  </div>

  <script>
    let evtSource = null;
    let isRunning = true;

    function connectSSE() {
      evtSource = new EventSource('/telemetry/stream');
      
      evtSource.onopen = () => {
        document.getElementById('sse-status').innerText = 'STREAMING';
        document.getElementById('sse-status').className = 'font-semibold text-emerald-400';
      };

      evtSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          updateDashboard(data);
        } catch (e) {
          console.error("Parse error:", e);
        }
      };

      evtSource.onerror = () => {
        document.getElementById('sse-status').innerText = 'RECONNECTING...';
        document.getElementById('sse-status').className = 'font-semibold text-amber-400';
      };
    }

    function updateDashboard(data) {
      const m = data.metrics;
      document.getElementById('balance').innerText = '$' + m.current_balance.toFixed(2);
      document.getElementById('free-bal').innerText = '$' + m.free_balance.toFixed(2);
      
      const pnlEl = document.getElementById('pnl');
      const sign = m.total_pnl >= 0 ? '+' : '';
      pnlEl.innerText = sign + '$' + m.total_pnl.toFixed(3);
      pnlEl.className = 'text-xl md:text-2xl font-black mt-1 ' + (m.total_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400');
      
      document.getElementById('roi').innerText = sign + m.roi_percent.toFixed(2) + '%';
      document.getElementById('trades-count').innerText = m.trades_count;
      document.getElementById('win-rate').innerText = m.win_rate.toFixed(1) + '%';
      document.getElementById('markets-count').innerText = m.active_markets_watched;
      document.getElementById('opps-count').innerText = data.active_opportunities.length;
      document.getElementById('latency').innerText = m.latency_ms.toFixed(1) + ' ms';
      document.getElementById('scan-count').innerText = m.scan_count;

      // Update Opportunities Table
      const oppsBody = document.getElementById('opps-body');
      if (data.active_opportunities.length === 0) {
        oppsBody.innerHTML = '<tr><td colspan="5" class="py-8 text-center text-slate-600">Đang lắng nghe sổ lệnh... Chưa có kèo chênh lệch đạt ngưỡng > 1.0%</td></tr>';
      } else {
        oppsBody.innerHTML = data.active_opportunities.map(o => `
          <tr class="hover:bg-slate-900/40 transition">
            <td class="py-2.5 pr-2 font-medium text-slate-200 truncate max-w-[200px]" title="${o.market_title}">
              ${o.market_title}
            </td>
            <td class="py-2.5 pr-2 text-slate-400">
              <span class="px-1.5 py-0.5 rounded text-[10px] bg-slate-800 border border-slate-700 text-sky-400">
                ${o.platform.split(' ')[0]}
              </span>
            </td>
            <td class="py-2.5 pr-2 font-mono text-slate-300">
              $${o.yes_price.toFixed(3)} + $${o.no_price.toFixed(3)} = <span class="font-bold text-amber-400">$${(o.yes_price + o.no_price).toFixed(3)}</span>
            </td>
            <td class="py-2.5 pr-2 font-mono text-emerald-400 font-bold">
              +${o.spread_percent.toFixed(2)}%
            </td>
            <td class="py-2.5 text-right font-mono text-emerald-400 font-bold">
              +$${o.est_net_profit.toFixed(3)}
            </td>
          </tr>
        `).join('');
      }

      // Update Executed Trades Table
      const tradesBody = document.getElementById('trades-body');
      if (data.trade_history.length === 0) {
        tradesBody.innerHTML = '<tr><td colspan="4" class="py-8 text-center text-slate-600">Chưa có giao dịch. Đang chờ kích hoạt tự động.</td></tr>';
      } else {
        tradesBody.innerHTML = data.trade_history.map(t => `
          <tr class="hover:bg-slate-900/40 transition">
            <td class="py-2 pr-2 text-slate-400 font-mono text-[11px]">${t.timestamp.split(' ')[1] || t.timestamp}</td>
            <td class="py-2 pr-2 text-slate-200 truncate max-w-[140px]" title="${t.market}">${t.market}</td>
            <td class="py-2 pr-2 font-mono text-slate-300">$${t.entry_cost.toFixed(2)}</td>
            <td class="py-2 text-right font-mono text-emerald-400 font-bold">+$${t.net_pnl.toFixed(3)}</td>
          </tr>
        `).join('');
      }

      // Update Logs
      const logFeed = document.getElementById('log-feed');
      if (data.log_stream && data.log_stream.length > 0) {
        logFeed.innerHTML = data.log_stream.map(l => {
          let color = 'text-slate-400';
          if (l.includes('EXECUTED')) color = 'text-emerald-400 font-semibold';
          if (l.includes('Initializing')) color = 'text-sky-400';
          return `<div class="${color}">${l}</div>`;
        }).join('');
      }
    }

    async function toggleEngine() {
      const resp = await fetch('/api/toggle', { method: 'POST' });
      const res = await resp.json();
      isRunning = res.is_running;
      const btn = document.getElementById('btn-toggle');
      btn.innerText = isRunning ? 'PAUSE' : 'RESUME';
      btn.className = isRunning 
        ? 'flex-1 py-1 px-2 bg-emerald-600 hover:bg-emerald-500 text-black font-bold text-xs rounded transition'
        : 'flex-1 py-1 px-2 bg-amber-600 hover:bg-amber-500 text-black font-bold text-xs rounded transition';
    }

    async function resetBalance() {
      if (confirm('Khởi tạo lại ví ảo về mức mặc định $10.00?')) {
        await fetch('/api/reset', { method: 'POST' });
      }
    }

    window.onload = connectSSE;
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return DASHBOARD_HTML

@app.get("/telemetry/stream")
async def telemetry_stream(request: Request):
    """Server-Sent Events streaming telemetry snapshot to browser every 1.5 seconds."""
    async def event_generator() -> AsyncGenerator[str, None]:
        while True:
            if await request.is_disconnected():
                break
            try:
                data = engine.get_telemetry_snapshot()
                yield f"data: {json.dumps(data)}\n\n"
            except Exception as e:
                logger.error(f"SSE serialization error: {e}")
            await asyncio.sleep(1.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@app.post("/api/toggle")
async def toggle_engine():
    engine.is_running = not engine.is_running
    engine.log(f"User toggled engine state to: {'RUNNING' if engine.is_running else 'PAUSED'}")
    return {"is_running": engine.is_running}

@app.post("/api/reset")
async def reset_ledger():
    ledger.starting_balance = CONFIG.STARTING_BALANCE_USDC
    ledger.total_balance = CONFIG.STARTING_BALANCE_USDC
    ledger.free_balance = CONFIG.STARTING_BALANCE_USDC
    ledger.locked_balance = Decimal("0.00")
    ledger.total_pnl = Decimal("0.00")
    ledger.winning_trades = 0
    ledger.losing_trades = 0
    ledger.trades.clear()
    ledger.active_reservations.clear()
    engine.log(f"Virtual balance reset to initial ${CONFIG.STARTING_BALANCE_USDC:.2f} USDC.")
    return {"status": "ok"}
