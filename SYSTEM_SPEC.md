# VANTA // AUTONOMOUS QUANTITATIVE ARBITRAGE SYSTEM SPECIFICATION
Version: 2.1-PROD
Architecture: Event-Driven Multi-Protocol Scanner & Telegram Hot Wallet Custodian

## 1. Executive Summary
An autonomous, zero-cost, 100% headless algorithmic arbitrage and prediction market trading system.
Designed for a retail user ("dj") with small capital ($10-$100) seeking risk-free spread extraction without manual intervention.

## 2. Core Components
1. Multi-Exchange Connectors:
   - Polymarket (Polygon L2 CLOB + Gamma API)
   - SX Bet (Arbitrum P2P Sports Book / Exchange)
   - Manifold Markets (Public Social Binary Probability)
   - Azuro Protocol (EVM Smart Contract Sportsbook on Polygon)
2. Quantitative Arbitrage Engine:
   - Continuous L2 Orderbook Depth & VWAP slippage evaluation
   - Complimentary Arbitrage (Buy YES + Buy NO < $1.00 - Fees - Gas)
   - Cross-Market Parity Arbitrage (Discrepancy across matching event hashes)
3. Virtual & On-Chain Ledger:
   - Atomic Capital Escrow: Max 25% allocation per trade
   - In-memory Settlement & PnL tracking
4. Autonomous Web3 Hot Wallet Custody:
   - Locally generated Polygon keypair stored in `.env`
   - EIP-712 signing for Polymarket CLOB exchange orders
   - Direct ERC-20 (USDC) approval & Gnosis CTF `mergePositions`
5. Telegram Command & Control Interface:
   - Private Chat ID Hard-Lock (`1423475386`)
   - Interactive commands: `/start`, `/deposit`, `/wallet`, `/balance`, `/mode`, `/withdraw`, `/status`, `/help`
   - Automated push notifications on trade fill & daily 20:00 financial digests

## 3. Threat Model & Feasibility Questions for Opus Reviewers
1. Latency & Race Conditions:
   - With servers in AWS US-East and ping from SE Asia ~200ms, how viable is Maker Limit arbing vs Taker arbing?
2. Hot Wallet Security:
   - Security of storing the hot wallet private key in `.env` for small funds ($10-$50).
   - Safeguards against unauthorized withdrawals or Telegram webhook spoofing.
3. Slippage & Legging Risk:
   - What happens if Leg 1 (YES) fills on Polymarket, but Leg 2 (NO) slips or liquidity vanishes?
   - How to implement an automated unwinding / emergency stop loss rule.
