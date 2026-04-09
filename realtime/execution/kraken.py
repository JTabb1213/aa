"""
Kraken trading client.

Implements the ExchangeClient interface for Kraken's REST API.

Kraken REST trading docs:
    https://docs.kraken.com/rest/#tag/Trading

Authentication:
    - API-Key header
    - API-Sign header (HMAC-SHA512 of nonce + POST data, keyed with base64-decoded secret)

Symbol format:
    Kraken uses pairs like "XXBTZUSD" (v1) or "BTC/USD" (v2/websocket).
    For REST trading, we use the websocket name format.
"""

import base64
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
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
# Canonical coin_id → Kraken symbol mapping
# ---------------------------------------------------------------------------
# Kraken uses unique symbol names. This maps our canonical IDs to Kraken's
# REST trading symbols. Add more as you expand your tracked coins.
# ---------------------------------------------------------------------------
COIN_TO_KRAKEN = {
    "bitcoin": "XBT",
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


class KrakenClient(ExchangeClient):
    """
    Kraken REST trading client.

    Requires KRAKEN_API_KEY and KRAKEN_API_SECRET in .env.
    """

    NAME = "kraken"
    BASE_URL = "https://api.kraken.com"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usd") -> Optional[str]:
        base = COIN_TO_KRAKEN.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}/{q}"
        return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign(self, urlpath: str, data: dict) -> dict:
        """Generate signed headers for a private Kraken API call."""
        encoded = urllib.parse.urlencode(data)
        message = (str(data["nonce"]) + encoded).encode()
        sha256 = hashlib.sha256(message).digest()
        mac = hmac.new(
            base64.b64decode(self._api_secret),
            urlpath.encode() + sha256,
            hashlib.sha512,
        )
        return {
            "API-Key": self._api_key,
            "API-Sign": base64.b64encode(mac.digest()).decode(),
        }

    async def _private_request(self, endpoint: str, data: dict) -> dict:
        """Make an authenticated POST request to Kraken."""
        urlpath = f"/0/private/{endpoint}"
        data["nonce"] = str(int(time.time() * 1000))
        headers = self._sign(urlpath, data)

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}{urlpath}",
                data=data,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                result = await resp.json()

        if result.get("error"):
            raise Exception(f"Kraken API error: {result['error']}")
        return result.get("result", {})

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

        # Kraken expects volume in base currency units.
        # For market buys, we can use 'oflags=viqc' to specify volume in quote currency.
        data = {
            "pair": symbol,
            "type": request.side.value,
            "ordertype": request.order_type.value,
        }

        if request.order_type == OrderType.MARKET:
            if request.side == OrderSide.BUY:
                # Buy with USD amount using viqc (volume in quote currency)
                data["volume"] = str(request.size_usd)
                data["oflags"] = "viqc"
            else:
                # Sell: need to specify crypto volume — caller should set this
                # For now, estimate from size_usd and limit_price
                if request.limit_price and request.limit_price > 0:
                    data["volume"] = str(round(request.size_usd / request.limit_price, 8))
                else:
                    return OrderResult(
                        exchange=self.NAME,
                        coin_id=request.coin_id,
                        side=request.side,
                        status=OrderStatus.REJECTED,
                        error="Market sell requires limit_price hint for volume calc",
                    )
        else:
            data["price"] = str(request.limit_price)
            if request.limit_price and request.limit_price > 0:
                data["volume"] = str(round(request.size_usd / request.limit_price, 8))

        result = await self._private_request("AddOrder", data)
        order_id = result.get("txid", [None])[0]

        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=OrderStatus.OPEN,
            order_id=order_id,
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        result = await self._private_request("QueryOrders", {"txid": order_id})
        order_info = result.get(order_id, {})
        status_str = order_info.get("status", "unknown")

        status_map = {
            "closed": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "open": OrderStatus.OPEN,
            "pending": OrderStatus.PENDING,
        }

        vol_exec = float(order_info.get("vol_exec", 0))
        cost = float(order_info.get("cost", 0))
        fee = float(order_info.get("fee", 0))
        avg_price = cost / vol_exec if vol_exec > 0 else 0

        return OrderResult(
            exchange=self.NAME,
            coin_id="",  # would need reverse symbol lookup
            side=OrderSide.BUY,
            status=status_map.get(status_str, OrderStatus.FAILED),
            order_id=order_id,
            filled_qty=vol_exec,
            filled_price=avg_price,
            filled_usd=cost,
            fee=fee,
        )

    async def _execute_cancel_order(self, order_id: str) -> bool:
        try:
            await self._private_request("CancelOrder", {"txid": order_id})
            return True
        except Exception as e:
            logger.error(f"[kraken] cancel failed: {e}")
            return False

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._private_request("Balance", {})
        # Kraken returns keys like "ZUSD", "XXBT", etc.
        balances = {}
        key_map = {"ZUSD": "usd", "XXBT": "btc", "XETH": "eth", "SOL": "sol"}
        for k, v in result.items():
            clean = key_map.get(k, k.lower().lstrip("xz"))
            balances[clean] = float(v)
        return balances
