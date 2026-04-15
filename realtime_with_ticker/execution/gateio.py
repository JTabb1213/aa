"""
Gate.io trading client.

Implements the ExchangeClient interface for Gate.io's REST API v4.

Gate.io API docs:
    https://www.gate.io/docs/developers/apiv4/en/#create-an-order

Authentication:
    - KEY header: API key
    - Timestamp header: Unix timestamp in seconds
    - SIGN header: HMAC-SHA512 of the canonical request string

    Canonical string format:
        METHOD\n
        /api/v4/PATH\n
        QUERY_STRING\n
        HEX(SHA512(BODY))\n
        TIMESTAMP

Symbol format: BTC_USDT  (underscore separator)
Taker fee:     0.20%
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
# Hardcoded fallback (used if coin_aliases.json is missing/unreadable)
# ---------------------------------------------------------------------------
_GATEIO_FALLBACK: dict = {
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


class GateioClient(ExchangeClient):
    """
    Gate.io v4 REST trading client (spot).

    Requires GATEIO_API_KEY and GATEIO_API_SECRET in .env.
    """

    NAME = "gateio"
    BASE_URL = "https://api.gateio.ws"
    API_PREFIX = "/api/v4"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret
        self._coin_map = {**_GATEIO_FALLBACK, **self._load_coin_map_from_json("gateio")}

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = self._coin_map.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}_{q}"  # e.g. "BTC_USDT"
        return None

    # ------------------------------------------------------------------
    # Auth helpers (Gate.io v4 HMAC-SHA512)
    # ------------------------------------------------------------------

    def _sign(
        self,
        method: str,
        path: str,
        query_string: str = "",
        body: str = "",
    ) -> dict:
        """
        Generate Gate.io v4 authentication headers.

        Canonical string:
            METHOD\n/api/v4/PATH\nQUERY_STRING\nHEX(SHA512(BODY))\nTIMESTAMP
        """
        ts = str(int(time.time()))
        body_hash = hashlib.sha512(body.encode()).hexdigest()
        msg = f"{method.upper()}\n{self.API_PREFIX}{path}\n{query_string}\n{body_hash}\n{ts}"
        sig = hmac.new(
            self._api_secret.encode(),
            msg.encode(),
            hashlib.sha512,
        ).hexdigest()
        return {
            "KEY": self._api_key,
            "Timestamp": ts,
            "SIGN": sig,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        body: dict = None,
    ) -> dict:
        """Make an authenticated request to Gate.io v4."""
        query_string = ""
        if params:
            from urllib.parse import urlencode
            query_string = urlencode(params)

        body_str = json.dumps(body) if body else ""
        headers = self._sign(method, path, query_string, body_str)

        url = f"{self.BASE_URL}{self.API_PREFIX}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    result = await resp.json()
            else:
                async with session.post(
                    url,
                    headers=headers,
                    data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()

        if isinstance(result, dict) and result.get("label"):
            raise Exception(
                f"Gate.io API error [{result.get('label')}]: {result.get('message', 'unknown')}"
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

        order_body: dict = {
            "currency_pair": symbol,
            "type": "market",
            "side": request.side.value.lower(),  # "buy" or "sell"
            "time_in_force": "ioc",
        }

        if request.side == OrderSide.BUY:
            # Gate.io market buys: specify amount in QUOTE currency
            order_body["amount"] = str(request.size_usd)
        else:
            # Gate.io market sells: specify amount in BASE currency
            if request.quantity:
                order_body["amount"] = str(request.quantity)
            else:
                return OrderResult(
                    exchange=self.NAME,
                    coin_id=request.coin_id,
                    side=request.side,
                    status=OrderStatus.REJECTED,
                    error="SELL order requires quantity",
                )

        try:
            result = await self._request("POST", "/spot/orders", body=order_body)
        except Exception as e:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.FAILED,
                error=str(e),
            )

        filled_qty = float(result.get("amount", 0) or 0)
        filled_price = float(result.get("avg_deal_price", 0) or 0)
        filled_usd = filled_qty * filled_price
        fee = float(result.get("fee", 0) or 0)
        status_str = result.get("status", "")
        status = (
            OrderStatus.FILLED
            if status_str == "closed"
            else OrderStatus.OPEN
        )

        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=status,
            order_id=str(result.get("id", "")),
            filled_qty=filled_qty,
            filled_price=filled_price,
            filled_usd=filled_usd,
            fee=fee,
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        raise NotImplementedError("get_order not yet implemented for Gate.io")

    async def _execute_cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError("cancel_order not yet implemented for Gate.io")

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/spot/accounts")
        balances: Dict[str, float] = {}
        if isinstance(result, list):
            for b in result:
                currency = b.get("currency", "").lower()
                available = float(b.get("available", 0) or 0)
                if available > 0:
                    balances[currency] = available
        return balances
