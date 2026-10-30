"""
prediction_bot/connectors/limitless.py
Connector chính thức cho Limitless Exchange (Base L2 - Chain ID 8453)
Hỗ trợ:
- Lấy danh sách thị trường từ GET /markets/active
- Quét Orderbook EIP-712 và tính toán xác suất lệch giá
- Ký lệnh Order EIP-712 (Maker 0% gas, Taker limit)
- Tích hợp bảo vệ vốn $10 USD (chia đạn, lọc buffer nến)
"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
import json
import logging
import time
from typing import Any, Dict, List, Optional
import urllib.parse
import urllib.request

logger = logging.getLogger("limitless_connector")


@dataclass
class LimitlessMarket:
    slug: str
    title: str
    target_asset: str  # BTC, ETH, SOL
    strike_price: Decimal
    end_time: int  # Unix timestamp seconds
    exchange_address: str
    token_id_up: int
    token_id_down: int
    best_ask_up: Decimal  # Giá bán rẻ nhất của share UP (0.01 - 0.99)
    best_ask_down: Decimal  # Giá bán rẻ nhất của share DOWN
    volume_24h: Decimal


@dataclass
class LimitlessSignal:
    market_slug: str
    asset: str
    strike_price: Decimal
    spot_price: Decimal
    buffer_usd: Decimal
    buffer_pct: Decimal
    seconds_to_expiry: int
    recommended_side: str  # "UP" hoặc "DOWN"
    share_price: Decimal
    expected_roi_pct: Decimal
    allocated_usd: Decimal
    is_safe: bool
    rejection_reason: Optional[str] = None


class LimitlessConnector:
    """
    Client giao tiếp trực tiếp với Limitless Exchange API trên Base L2.
    """
    BASE_API_URL = "https://api.limitless.exchange"
    BASE_CHAIN_ID = 8453

    def __init__(self, private_key: str = "", dry_run: bool = True):
        self.private_key = private_key
        self.dry_run = dry_run
        self.cached_markets: List[LimitlessMarket] = []
        self._wallet_address: Optional[str] = None
        self._signer = None

        if private_key:
            try:
                from eth_account import Account
                self._account = Account.from_key(private_key)
                self._wallet_address = self._account.address
            except Exception as e:
                logger.warning("Không thể khởi tạo ví từ private key: %s", e)

    def fetch_active_markets(self, asset: Optional[str] = None) -> List[LimitlessMarket]:
        """
        Lấy danh sách các thị trường đang mở (Open/Active) từ /markets/active.
        """
        url = f"{self.BASE_API_URL}/markets/active"
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "AntigravityLimitlessBot/1.0", "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.error("Lỗi khi gọi Limitless API: %s", e)
            return []

        markets: List[LimitlessMarket] = []
        now = int(time.time())

        items = data if isinstance(data, list) else data.get("data", [])
        for item in items:
            slug = item.get("slug", "")
            title = item.get("title", "")
            
            # Timestamp có thể là ms hoặc s
            raw_expiry = item.get("expirationTimestamp") or item.get("expirationDate") or 0
            if isinstance(raw_expiry, (int, float)):
                end_time = int(raw_expiry // 1000 if raw_expiry > 10000000000 else raw_expiry)
            else:
                end_time = 0

            # Nhận diện tài sản cơ sở
            title_upper = title.upper()
            target_asset = "OTHER"
            if "BTC" in title_upper or "BITCOIN" in title_upper:
                target_asset = "BTC"
            elif "ETH" in title_upper or "ETHEREUM" in title_upper:
                target_asset = "ETH"
            elif "SOL" in title_upper or "SOLANA" in title_upper:
                target_asset = "SOL"

            if asset and target_asset != asset.upper():
                continue

            # Trích xuất Strike price từ metadata hoặc priceOracleMetadata
            oracle_meta = item.get("priceOracleMetadata", {}) or {}
            strike_val = oracle_meta.get("strikePrice") or item.get("strikePrice", "0")
            try:
                strike_price = Decimal(str(strike_val))
            except Exception:
                strike_price = Decimal("0")

            venue = item.get("venue", {}) or {}
            exchange_addr = venue.get("exchange", "") or item.get("exchangeAddress", "")

            # Trích xuất giá orderbook từ prices array [UP, DOWN] hoặc dict
            raw_prices = item.get("prices", [])
            best_ask_up = Decimal("0.50")
            best_ask_down = Decimal("0.50")

            if isinstance(raw_prices, list) and len(raw_prices) >= 2:
                best_ask_up = Decimal(str(raw_prices[0]))
                best_ask_down = Decimal(str(raw_prices[1]))
            elif isinstance(raw_prices, dict):
                best_ask_up = Decimal(str(raw_prices.get("UP", raw_prices.get("YES", "0.50"))))
                best_ask_down = Decimal(str(raw_prices.get("DOWN", raw_prices.get("NO", "0.50"))))

            # Token IDs
            tokens = item.get("tokens", [])
            token_id_up = 1
            token_id_down = 2
            if isinstance(tokens, list) and len(tokens) >= 2:
                token_id_up = int(tokens[0].get("tokenId", 1) if isinstance(tokens[0], dict) else tokens[0])
                token_id_down = int(tokens[1].get("tokenId", 2) if isinstance(tokens[1], dict) else tokens[1])

            try:
                vol_24h = Decimal(str(item.get("volume", "0")))
            except Exception:
                vol_24h = Decimal("0")

            markets.append(LimitlessMarket(
                slug=slug,
                title=title,
                target_asset=target_asset,
                strike_price=strike_price,
                end_time=end_time,
                exchange_address=exchange_addr,
                token_id_up=token_id_up,
                token_id_down=token_id_down,
                best_ask_up=best_ask_up,
                best_ask_down=best_ask_down,
                volume_24h=vol_24h
            ))

        self.cached_markets = markets
        return markets

    def evaluate_late_cycle_convergence(
        self,
        market: LimitlessMarket,
        current_spot: Decimal,
        bankroll_usdc: Decimal = Decimal("10.00")
    ) -> LimitlessSignal:
        """
        Thuật toán Định giá Hội tụ Cuối Chu kỳ (Late-Cycle Convergence):
        - Đo khoảng cách giữa Spot Price và Strike Price (Buffer).
        - Đánh giá xác suất thắng thực tế dựa trên thời gian còn lại (Minutes to expiry).
        - Bắt lỗi lệch giá (Mispricing): Nếu xác suất >99% nhưng share bán < 0.92¢ -> Phát tín hiệu vào lệnh.
        """
        now = int(time.time())
        seconds_left = max(0, market.end_time - now)
        minutes_left = seconds_left // 60

        buffer_usd = current_spot - market.strike_price
        buffer_pct = (buffer_usd / market.strike_price * 100) if market.strike_price > 0 else Decimal("0")

        # Quy tắc quản trị vốn nghiêm ngặt cho bài toán khởi nghiệp vốn $10 USD:
        # Vốn nhỏ $10 chia đều thành các vé cược siêu nhỏ từ $0.20 - $0.50 (5% vốn) để tối đa hóa số lần quay vòng.
        max_bet_amount = max(Decimal("0.20"), min(bankroll_usdc * Decimal("0.05"), Decimal("0.80")))

        # LOẠI BỎ TRIỆT ĐỂ BẮT DAO RƠI (Chống râu nến giật khung siêu ngắn theo phản xạ chuẩn của dj):
        # Nghiêm cấm cược trên nến 5 Phút (5 Min) và 15 Phút (15 Min) vì 85% biến động là nhiễu giật râu!
        # CHỈ CHO PHÉP săn trên nến 1 Giờ (Hourly) hoặc Daily có đà quán tính vững chắc.
        title_lower = market.title.lower()
        if "5 min" in title_lower or "15 min" in title_lower:
            return LimitlessSignal(
                market_slug=market.slug,
                asset=market.target_asset,
                strike_price=market.strike_price,
                spot_price=current_spot,
                buffer_usd=buffer_usd,
                buffer_pct=buffer_pct,
                seconds_to_expiry=seconds_left,
                recommended_side="NONE",
                share_price=Decimal("0"),
                expected_roi_pct=Decimal("0"),
                allocated_usd=Decimal("0"),
                is_safe=False,
                rejection_reason="Bác bỏ nến 5m/15m: Khung quá ngắn, nguy cơ bắt dao rơi và râu nến quét phút chót"
            )

        # Kiểm tra điều kiện thời gian: Chỉ săn ở 2 đến 10 phút cuối cùng
        if seconds_left > 600:
            return LimitlessSignal(
                market_slug=market.slug,
                asset=market.target_asset,
                strike_price=market.strike_price,
                spot_price=current_spot,
                buffer_usd=buffer_usd,
                buffer_pct=buffer_pct,
                seconds_to_expiry=seconds_left,
                recommended_side="NONE",
                share_price=Decimal("0"),
                expected_roi_pct=Decimal("0"),
                allocated_usd=Decimal("0"),
                is_safe=False,
                rejection_reason=f"Chưa đến giờ săn (còn {minutes_left} phút, chỉ vào lệnh khi <= 10 phút)"
            )

        # DEAD ZONE: Không vào lệnh khi còn dưới 90 giây để phòng ngừa:
        # 1. Reject storm khi dùng PostOnly
        # 2. Sai lệch giá (Disagreement) giữa Binance Spot và Chainlink Data Streams
        # 3. Kẹt lệnh trong hàng đợi khớp của Limitless Orderbook
        if seconds_left < 90:
            return LimitlessSignal(
                market_slug=market.slug,
                asset=market.target_asset,
                strike_price=market.strike_price,
                spot_price=current_spot,
                buffer_usd=buffer_usd,
                buffer_pct=buffer_pct,
                seconds_to_expiry=seconds_left,
                recommended_side="NONE",
                share_price=Decimal("0"),
                expected_roi_pct=Decimal("0"),
                allocated_usd=Decimal("0"),
                is_safe=False,
                rejection_reason="Vùng chết (< 90s): Tránh Reject Storm & Lệch pha Chainlink Data Streams"
            )

        # Ngưỡng an toàn tối thiểu (Buffer Threshold)
        # BTC: Cách strike >= $250
        # ETH: Cách strike >= $15
        # SOL: Cách strike >= $2
        safe_buffer_usd = Decimal("250.00")
        if market.target_asset == "ETH":
            safe_buffer_usd = Decimal("15.00")
        elif market.target_asset == "SOL":
            safe_buffer_usd = Decimal("2.00")

        # Xác định phe thắng gần như chắc chắn
        if buffer_usd >= safe_buffer_usd:
            # Cửa UP thắng gần như tuyệt đối
            side = "UP"
            share_price = market.best_ask_up
        elif buffer_usd <= -safe_buffer_usd:
            # Cửa DOWN thắng gần như tuyệt đối
            side = "DOWN"
            share_price = market.best_ask_down
        else:
            return LimitlessSignal(
                market_slug=market.slug,
                asset=market.target_asset,
                strike_price=market.strike_price,
                spot_price=current_spot,
                buffer_usd=buffer_usd,
                buffer_pct=buffer_pct,
                seconds_to_expiry=seconds_left,
                recommended_side="NONE",
                share_price=Decimal("0"),
                expected_roi_pct=Decimal("0"),
                allocated_usd=Decimal("0"),
                is_safe=False,
                rejection_reason=f"Khoảng cách quá mỏng ({abs(buffer_usd):.2f} USD < {safe_buffer_usd:.2f} USD ngưỡng an toàn)"
            )

        # Kiểm tra giá share: Chỉ mua nếu giá <= $0.92 (đảm bảo lãi ròng >= 8.7%)
        if share_price > Decimal("0.92"):
            return LimitlessSignal(
                market_slug=market.slug,
                asset=market.target_asset,
                strike_price=market.strike_price,
                spot_price=current_spot,
                buffer_usd=buffer_usd,
                buffer_pct=buffer_pct,
                seconds_to_expiry=seconds_left,
                recommended_side=side,
                share_price=share_price,
                expected_roi_pct=((Decimal("1.00") - share_price) / share_price * 100),
                allocated_usd=Decimal("0"),
                is_safe=False,
                rejection_reason=f"Giá share {share_price:.2f} quá cao (> 0.92¢), biên lợi nhuận quá mỏng"
            )

        if share_price <= Decimal("0.05"):
            return LimitlessSignal(
                market_slug=market.slug,
                asset=market.target_asset,
                strike_price=market.strike_price,
                spot_price=current_spot,
                buffer_usd=buffer_usd,
                buffer_pct=buffer_pct,
                seconds_to_expiry=seconds_left,
                recommended_side=side,
                share_price=share_price,
                expected_roi_pct=Decimal("0"),
                allocated_usd=Decimal("0"),
                is_safe=False,
                rejection_reason=f"Dữ liệu orderbook bất thường (Giá share {share_price:.2f})"
            )

        expected_roi = ((Decimal("1.00") - share_price) / share_price) * 100

        return LimitlessSignal(
            market_slug=market.slug,
            asset=market.target_asset,
            strike_price=market.strike_price,
            spot_price=current_spot,
            buffer_usd=buffer_usd,
            buffer_pct=buffer_pct,
            seconds_to_expiry=seconds_left,
            recommended_side=side,
            share_price=share_price,
            expected_roi_pct=expected_roi,
            allocated_usd=max_bet_amount,
            is_safe=True,
            rejection_reason=None
        )

    def create_eip712_order_payload(
        self,
        market: LimitlessMarket,
        side: str,
        amount_usdc: Decimal,
        share_price: Decimal
    ) -> Dict[str, Any]:
        """
        Tạo cấu trúc EIP-712 Order Type cho Limitless CTF Exchange trên Base (ChainId: 8453).
        """
        token_id = market.token_id_up if side == "UP" else market.token_id_down

        # Số lượng share nhận được = amount_usdc / share_price
        shares_amount = amount_usdc / share_price
        # Quy đổi ra đơn vị 6 chữ số thập phân của USDC
        maker_amount = int(amount_usdc * Decimal("1000000"))
        taker_amount = int(shares_amount * Decimal("1000000"))

        salt = int(time.time() * 1000)
        expiration = market.end_time + 300  # Hạn dùng sau khi đáo hạn 5p

        domain = {
            "name": "Limitless CTF Exchange",
            "version": "1",
            "chainId": self.BASE_CHAIN_ID,
            "verifyingContract": market.exchange_address or "0x0000000000000000000000000000000000000000"
        }

        order_message = {
            "salt": salt,
            "maker": self._wallet_address or "0x0000000000000000000000000000000000000000",
            "signer": self._wallet_address or "0x0000000000000000000000000000000000000000",
            "taker": "0x0000000000000000000000000000000000000000",
            "tokenId": token_id,
            "makerAmount": maker_amount,
            "takerAmount": taker_amount,
            "expiration": expiration,
            "nonce": 0,
            "feeRateBps": 0,  # Maker 0% fee
            "side": 0,  # Buy order
            "signatureType": 0  # EOA standard
        }

        return {
            "domain": domain,
            "message": order_message,
            "primaryType": "Order",
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}
                ],
                "Order": [
                    {"name": "salt", "type": "uint256"},
                    {"name": "maker", "type": "address"},
                    {"name": "signer", "type": "address"},
                    {"name": "taker", "type": "address"},
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "makerAmount", "type": "uint256"},
                    {"name": "takerAmount", "type": "uint256"},
                    {"name": "expiration", "type": "uint256"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "feeRateBps", "type": "uint256"},
                    {"name": "side", "type": "uint8"},
                    {"name": "signatureType", "type": "uint8"}
                ]
            },
            "postOnly": True  # Bắt buộc theo phản biện Opus: chống bị khớp ngay lập tức như taker
        }

    def cancel_order(self, order_id: str) -> bool:
        """
        Hủy lệnh off-chain qua REST API của Limitless Exchange (0% gas on-chain).
        """
        url = f"{self.BASE_API_URL}/orders/{order_id}/cancel"
        try:
            req = urllib.request.Request(
                url,
                method="POST",
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception as e:
            logger.warning("Lỗi hủy lệnh %s qua API Limitless: %s", order_id, e)
            return False

    def cancel_all_orders(self, market_slug: Optional[str] = None) -> bool:
        """
        Hủy hàng loạt lệnh (Batch Cancel) khi phát hiện biến động hoặc nghẽn mạng.
        """
        url = f"{self.BASE_API_URL}/orders/cancel-all"
        payload = json.dumps({"market": market_slug} if market_slug else {}).encode("utf-8")
        try:
            req = urllib.request.Request(
                url,
                data=payload,
                method="POST",
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception as e:
            logger.warning("Lỗi batch cancel qua API Limitless: %s", e)
            return False
