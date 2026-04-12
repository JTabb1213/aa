"""
MEXC trading client.

Implements the ExchangeClient interface for MEXC's REST API.

MEXC uses a Binance-compatible REST API — the endpoint structure,
parameter names, and HMAC-SHA256 signing are virtually identical.

MEXC API docs:
    https://mexcdevelop.github.io/apidocs/spot_v3_en/#introduction

Authentication:
    - X-MEXC-APIKEY header (instead of X-MBX-APIKEY)
    - HMAC-SHA256 signature as query parameter (same logic as Binance)

Symbol format: BTCUSDT (concatenated, uppercase)
Taker fee:     0.10%
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
# Hardcoded fallback (used if coin_aliases.json is missing/unreadable)
# ---------------------------------------------------------------------------
_MEXC_FALLBACK: dict = {
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


class MexcClient(ExchangeClient):
    """
    MEXC REST trading client (Binance-compatible API).

    Requires MEXC_API_KEY and MEXC_API_SECRET in .env.
    """

    NAME = "mexc"
    BASE_URL = "https://api.mexc.com"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret
        self._coin_map = {**_MEXC_FALLBACK, **self._load_coin_map_from_json("mexc")}

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = self._coin_map.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}{q}"  # e.g. "BTCUSDT"
        return None

    # ------------------------------------------------------------------
    # Auth helpers (identical logic to Binance)
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
        return {"X-MEXC-APIKEY": self._api_key}

    async def _request(self, method: str, path: str, params: dict = None) -> dict:
        """Make an authenticated request to MEXC."""
        params = self._sign_params(params or {})
        url = f"{self.BASE_URL}{path}"

        async with aiohttp.ClientSession() as session:
            req_kwargs = dict(
                params=params,
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=10),
            )
            if method.upper() == "GET":
                async with session.get(url, **req_kwargs) as resp:
                    result = await resp.json()
            else:
                async with session.post(url, **req_kwargs) as resp:
                    result = await resp.json()

        if isinstance(result, dict) and result.get("code") and int(result["code"]) < 0:
            raise Exception(
                f"MEXC API error {result['code']}: {result.get('msg', 'unknown')}"
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
            "type": "MARKET",
        }

        if request.side == OrderSide.BUY:
            # Buy: specify quote amount (USDT)
            params["quoteOrderQty"] = str(request.size_usd)
        else:
            # Sell: specify base quantity (must be known)
            if request.quantity:
                params["quantity"] = str(request.quantity)
            else:
                return OrderResult(
                    exchange=self.NAME,
                    coin_id=request.coin_id,
                    side=request.side,
                    status=OrderStatus.REJECTED,
                    error="SELL order requires quantity",
                )

        try:
            result = await self._request("POST", "/api/v3/order", params)
        except Exception as e:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.FAILED,
                error=str(e),
            )

        filled_qty = float(result.get("executedQty", 0) or 0)
        filled_usd = float(result.get("cummulativeQuoteQty", 0) or 0)
        filled_price = filled_usd / filled_qty if filled_qty else 0.0
        status = (
            OrderStatus.FILLED
            if result.get("status") == "FILLED"
            else OrderStatus.OPEN
        )

        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=status,
            order_id=str(result.get("orderId", "")),
            filled_qty=filled_qty,
            filled_price=filled_price,
            filled_usd=filled_usd,
            fee=0.0,  # MEXC doesn't always return fee separately
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        raise NotImplementedError("get_order not yet implemented for MEXC")

    async def _execute_cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError("cancel_order not yet implemented for MEXC")

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/api/v3/account")
        balances: Dict[str, float] = {}
        for b in result.get("balances", []):
            asset = b.get("asset", "").lower()
            free = float(b.get("free", 0) or 0)
            if free > 0:
                balances[asset] = free
        return balances
