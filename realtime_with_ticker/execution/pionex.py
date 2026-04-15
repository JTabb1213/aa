"""
Pionex trading client.

Implements the ExchangeClient interface for Pionex's REST API.

Pionex API docs:
    https://pionex-doc.gitbook.io/apidocs/rest-api/trading

Authentication:
    - PIONEX-KEY header
    - PIONEX-SIGNATURE header (HMAC-SHA256 of method+path+timestamp+body)
    - PIONEX-TIMESTAMP header
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
# Canonical coin_id → Pionex symbol mapping
# ---------------------------------------------------------------------------
# Hardcoded fallback (used if coin_aliases.json is missing/unreadable)
_PIONEX_FALLBACK: dict = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "avalanche-2": "AVAX",
    "litecoin": "LTC",
}

QUOTE_MAP = {"usd": "USDT", "usdt": "USDT"}


class PionexClient(ExchangeClient):
    """
    Pionex REST trading client.

    Requires PIONEX_API_KEY and PIONEX_API_SECRET in .env.
    """

    NAME = "pionex"
    BASE_URL = "https://api.pionex.com"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret
        self._coin_map = {**_PIONEX_FALLBACK, **self._load_coin_map_from_json("pionex")}

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = self._coin_map.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}_{q}"
        return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign(self, method: str, path: str, timestamp: str, body: str = "") -> dict:
        pre_sign = f"{method.upper()}{path}{timestamp}{body}"
        sig = hmac.new(
            self._api_secret.encode(),
            pre_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "PIONEX-KEY": self._api_key,
            "PIONEX-SIGNATURE": sig,
            "PIONEX-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, body: dict = None) -> dict:
        timestamp = str(int(time.time() * 1000))
        body_str = json.dumps(body) if body else ""
        headers = self._sign(method, path, timestamp, body_str)
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

        if not result.get("result", False):
            raise Exception(
                f"Pionex API error: {result.get('message', 'unknown')}"
            )
        return result.get("data", {})

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
            "symbol": symbol,
            "side": "BUY" if request.side == OrderSide.BUY else "SELL",
            "type": "MARKET" if request.order_type == OrderType.MARKET else "LIMIT",
        }

        if request.order_type == OrderType.MARKET:
            if request.side == OrderSide.BUY:
                body["quoteAmount"] = str(round(request.size_usd, 2))
            else:
                if request.limit_price and request.limit_price > 0:
                    qty = request.size_usd / request.limit_price
                    body["amount"] = str(round(qty, 8))
                else:
                    return OrderResult(
                        exchange=self.NAME, coin_id=request.coin_id,
                        side=request.side, status=OrderStatus.REJECTED,
                        error="Market sell needs limit_price hint",
                    )
        else:
            if request.limit_price and request.limit_price > 0:
                qty = request.size_usd / request.limit_price
                body["amount"] = str(round(qty, 8))
                body["price"] = str(request.limit_price)

        result = await self._request("POST", "/api/v1/trade/order", body)
        order_id = result.get("orderId", "")

        return OrderResult(
            exchange=self.NAME, coin_id=request.coin_id,
            side=request.side, status=OrderStatus.OPEN, order_id=order_id,
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        result = await self._request(
            "GET", f"/api/v1/trade/order?orderId={order_id}"
        )
        status_map = {
            "FILLED": OrderStatus.FILLED,
            "PARTIALLY_FILLED": OrderStatus.PARTIAL,
            "NEW": OrderStatus.OPEN,
            "CANCELED": OrderStatus.CANCELLED,
        }
        filled_qty = float(result.get("filledAmount", 0))
        filled_value = float(result.get("filledValue", 0))
        fee = float(result.get("fee", 0))
        avg = filled_value / filled_qty if filled_qty > 0 else 0

        return OrderResult(
            exchange=self.NAME, coin_id="", side=OrderSide.BUY,
            status=status_map.get(result.get("status", ""), OrderStatus.FAILED),
            order_id=order_id, filled_qty=filled_qty,
            filled_price=avg, filled_usd=filled_value, fee=fee,
        )

    async def _execute_cancel_order(self, order_id: str) -> bool:
        try:
            await self._request("POST", "/api/v1/trade/cancelOrder", {
                "orderId": order_id,
            })
            return True
        except Exception as e:
            logger.error(f"[pionex] cancel failed: {e}")
            return False

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/api/v1/account/balances")
        balances = {}
        for item in result.get("balances", []):
            asset = item.get("coin", "").lower()
            available = float(item.get("free", 0))
            if available > 0:
                balances[asset] = available
        return balances
