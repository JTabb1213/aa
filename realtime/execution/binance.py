"""
Binance.US trading client.

Implements the ExchangeClient interface for Binance.US REST API.

Binance.US docs:
    https://docs.binance.us/

Authentication:
    - X-MBX-APIKEY header
    - HMAC-SHA256 signature as query parameter
"""

import hashlib
import hmac
import logging
import time
from typing import Dict, Optional
from urllib.parse import urlencode

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
# Canonical coin_id → Binance symbol mapping
# ---------------------------------------------------------------------------
COIN_TO_BINANCE = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "binancecoin": "BNB",
    "tether": "USDT",
    "usd-coin": "USDC",
}

QUOTE_MAP = {
    "usd": "USD",
    "usdt": "USDT",
}


class BinanceClient(ExchangeClient):
    """
    Binance.US REST trading client.

    Requires BINANCE_API_KEY and BINANCE_API_SECRET in .env.
    """

    NAME = "binance"
    BASE_URL = "https://api.binance.us"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = COIN_TO_BINANCE.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}{q}"
        return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign_params(self, params: dict) -> dict:
        """Add timestamp + HMAC-SHA256 signature to query params."""
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query_string = urlencode(params)
        sig = hmac.new(
            self._api_secret.encode(),
            query_string.encode(),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = sig
        return params

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self._api_key}

    async def _request(self, method: str, path: str, params: dict = None) -> dict:
        """Make an authenticated request to Binance.US."""
        params = self._sign_params(params or {})
        url = f"{self.BASE_URL}{path}"

        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()
            else:
                async with session.post(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()

        if isinstance(result, dict) and result.get("code") and result["code"] < 0:
            raise Exception(
                f"Binance API error {result['code']}: {result.get('msg', 'unknown')}"
            )
        return result

    # ------------------------------------------------------------------
    # Live execution
    # ------------------------------------------------------------------

    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        symbol = self.get_exchange_symbol(request.coin_id, request.quote)
        if not symbol:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.REJECTED,
                error=f"Unknown symbol for {request.coin_id}/{request.quote}",
            )

        params: dict = {
            "symbol": symbol,
            "side": request.side.value.upper(),
        }

        if request.order_type == OrderType.MARKET:
            params["type"] = "MARKET"
            if request.side == OrderSide.BUY:
                # Market buy: use quoteOrderQty (spend exact USD)
                params["quoteOrderQty"] = str(round(request.size_usd, 2))
            else:
                # Market sell: need base quantity
                if request.limit_price and request.limit_price > 0:
                    qty = request.size_usd / request.limit_price
                    params["quantity"] = str(round(qty, 8))
                else:
                    return OrderResult(
                        exchange=self.NAME,
                        coin_id=request.coin_id,
                        side=request.side,
                        status=OrderStatus.REJECTED,
                        error="Market sell requires limit_price hint for volume calc",
                    )
        else:
            # Limit order
            params["type"] = "LIMIT"
            params["timeInForce"] = "GTC"
            if request.limit_price and request.limit_price > 0:
                qty = request.size_usd / request.limit_price
                params["quantity"] = str(round(qty, 8))
                params["price"] = str(request.limit_price)

        result = await self._request("POST", "/api/v3/order", params)
        order_id = str(result.get("orderId", ""))
        status_str = result.get("status", "NEW")

        # Binance returns fills inline for market orders
        fills = result.get("fills", [])
        filled_qty = sum(float(f.get("qty", 0)) for f in fills)
        filled_cost = sum(float(f.get("qty", 0)) * float(f.get("price", 0)) for f in fills)
        total_fee = sum(float(f.get("commission", 0)) for f in fills)
        avg_price = filled_cost / filled_qty if filled_qty > 0 else 0

        status_map = {
            "NEW": OrderStatus.OPEN,
            "FILLED": OrderStatus.FILLED,
            "PARTIALLY_FILLED": OrderStatus.PARTIAL,
            "CANCELED": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.CANCELLED,
        }

        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=status_map.get(status_str, OrderStatus.OPEN),
            order_id=order_id,
            filled_qty=filled_qty,
            filled_price=avg_price,
            filled_usd=filled_cost,
            fee=total_fee,
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        # Need symbol — store it from create, or query all open orders.
        # For now, use /api/v3/order with orderId  (caller must track symbol).
        raise NotImplementedError(
            "get_order requires symbol context — use coordinator tracking"
        )

    async def _execute_cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError(
            "cancel_order requires symbol context — use coordinator tracking"
        )

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/api/v3/account")
        balances = {}
        for item in result.get("balances", []):
            asset = item.get("asset", "").lower()
            free = float(item.get("free", 0))
            if free > 0:
                balances[asset] = free
        return balances
