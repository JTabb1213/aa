"""
Bybit trading client.

Implements the ExchangeClient interface for Bybit's v5 REST API.

Bybit v5 docs:
    https://bybit-exchange.github.io/docs/v5/order/create-order

Authentication:
    - X-BAPI-API-KEY header
    - X-BAPI-SIGN header (HMAC-SHA256)
    - X-BAPI-TIMESTAMP header
    - X-BAPI-RECV-WINDOW header
"""

import hashlib
import hmac
import json
import logging
import time
from typing import Dict, Optional

import aiohttp

import config
from execution.base import ExchangeClient
from execution.types import (
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical coin_id → Bybit symbol mapping
# ---------------------------------------------------------------------------
COIN_TO_BYBIT = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "binancecoin": "BNB",
    "tether": "USDT",
    "usd-coin": "USDC",
    "avalanche-2": "AVAX",
    "polkadot": "DOT",
    "chainlink": "LINK",
}

QUOTE_MAP = {"usd": "USDT", "usdt": "USDT"}


class BybitClient(ExchangeClient):
    """
    Bybit v5 REST trading client (spot).

    Requires BYBIT_API_KEY and BYBIT_API_SECRET in .env.
    """

    NAME = "bybit"
    BASE_URL = "https://api.bybit.com"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = COIN_TO_BYBIT.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}{q}"
        return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign(self, timestamp: str, params_str: str) -> dict:
        recv_window = "5000"
        pre_sign = f"{timestamp}{self._api_key}{recv_window}{params_str}"
        sig = hmac.new(
            self._api_secret.encode(),
            pre_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-SIGN": sig,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, body: dict = None) -> dict:
        timestamp = str(int(time.time() * 1000))
        body_str = json.dumps(body) if body else ""
        headers = self._sign(timestamp, body_str if method == "POST" else "")
        url = f"{self.BASE_URL}{path}"

        async with aiohttp.ClientSession() as session:
            if method == "POST":
                async with session.post(
                    url, headers=headers, data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()
            else:
                async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()

        if result.get("retCode", 0) != 0:
            raise Exception(
                f"Bybit API error {result.get('retCode')}: {result.get('retMsg')}"
            )
        return result.get("result", {})

    # ------------------------------------------------------------------
    # Live execution
    # ------------------------------------------------------------------

    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        symbol = self.get_exchange_symbol(request.coin_id, request.quote)
        if not symbol:
            return OrderResult(
                exchange=self.NAME, coin_id=request.coin_id,
                side=request.side, status=OrderStatus.REJECTED,
                error=f"Unknown symbol for {request.coin_id}/{request.quote}",
            )

        body: dict = {
            "category": "spot",
            "symbol": symbol,
            "side": "Buy" if request.side == OrderSide.BUY else "Sell",
            "orderType": "Market" if request.order_type == OrderType.MARKET else "Limit",
        }

        if request.order_type == OrderType.MARKET:
            if request.side == OrderSide.BUY:
                # Market buy: marketUnit=quoteCoin, qty = USD amount
                body["marketUnit"] = "quoteCoin"
                body["qty"] = str(round(request.size_usd, 2))
            else:
                if request.limit_price and request.limit_price > 0:
                    qty = request.size_usd / request.limit_price
                    body["qty"] = str(round(qty, 8))
                else:
                    return OrderResult(
                        exchange=self.NAME, coin_id=request.coin_id,
                        side=request.side, status=OrderStatus.REJECTED,
                        error="Market sell needs limit_price hint",
                    )
        else:
            if request.limit_price and request.limit_price > 0:
                qty = request.size_usd / request.limit_price
                body["qty"] = str(round(qty, 8))
                body["price"] = str(request.limit_price)

        result = await self._request("POST", "/v5/order/create", body)
        order_id = result.get("orderId", "")

        return OrderResult(
            exchange=self.NAME, coin_id=request.coin_id,
            side=request.side, status=OrderStatus.OPEN, order_id=order_id,
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        result = await self._request(
            "GET",
            f"/v5/order/realtime?category=spot&orderId={order_id}",
        )
        orders = result.get("list", [])
        if not orders:
            return OrderResult(
                exchange=self.NAME, coin_id="", side=OrderSide.BUY,
                status=OrderStatus.FAILED, error="Order not found",
            )
        order = orders[0]
        status_map = {
            "Filled": OrderStatus.FILLED,
            "PartiallyFilled": OrderStatus.PARTIAL,
            "New": OrderStatus.OPEN,
            "Cancelled": OrderStatus.CANCELLED,
            "Rejected": OrderStatus.REJECTED,
        }
        filled_qty = float(order.get("cumExecQty", 0))
        filled_value = float(order.get("cumExecValue", 0))
        fee = float(order.get("cumExecFee", 0))
        avg = filled_value / filled_qty if filled_qty > 0 else 0

        return OrderResult(
            exchange=self.NAME, coin_id="", side=OrderSide.BUY,
            status=status_map.get(order.get("orderStatus", ""), OrderStatus.FAILED),
            order_id=order_id, filled_qty=filled_qty,
            filled_price=avg, filled_usd=filled_value, fee=fee,
        )

    async def _execute_cancel_order(self, order_id: str) -> bool:
        try:
            await self._request("POST", "/v5/order/cancel", {
                "category": "spot", "orderId": order_id,
            })
            return True
        except Exception as e:
            logger.error(f"[bybit] cancel failed: {e}")
            return False

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/v5/account/wallet-balance?accountType=UNIFIED")
        balances = {}
        for account in result.get("list", []):
            for coin in account.get("coin", []):
                asset = coin.get("coin", "").lower()
                available = float(coin.get("availableToWithdraw", 0))
                if available > 0:
                    balances[asset] = available
        return balances
