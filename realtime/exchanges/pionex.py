"""
Pionex websocket connector.

Connects to Pionex's WebSocket API, subscribes to the ticker stream
for USDT pairs, and emits RawTick events onto the ingestion queue.

Pionex API docs:
    https://pionex-doc.gitbook.io/apidocs/websocket/market-data

Message format (ticker):
    Pionex uses a subscribe/push model. After subscribing to
    "SUBSCRIBE_TICKER", updates arrive as:
    {
        "topic": "TICKER",
        "data": {
            "symbol": "BTC_USDT",
            "close": "65432.10",
            "bid": "65432.00",
            "ask": "65433.00",
            "volume": "12345.67",
            ...
        },
        "timestamp": 1710000000000
    }

Note: Pionex uses "_" separated pairs (BTC_USDT).
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

import aiohttp
import websockets

import config
from core.models import RawTick
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


# ==============================================================================
# TEST MODE
# ==============================================================================
TEST_SYMBOLS_OVERRIDE = None
# TEST_SYMBOLS_OVERRIDE = [
#     "BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "ADA_USDT",
#     "DOGE_USDT", "AVAX_USDT", "DOT_USDT", "LINK_USDT", "LTC_USDT",
# ]
# ==============================================================================


class PionexConnector(BaseExchange):
    """
    Pionex exchange websocket connector.

    Lifecycle:
        1. Load symbols from coin_aliases.json (or fallback to API)
        2. Subscribe to ticker stream for all symbols
        3. Parse incoming messages → RawTick → ingestion queue
    """

    NAME = "pionex"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.PIONEX_WS_URL
        self._rest_url = config.PIONEX_REST_URL
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """Load trading symbols from coin_aliases.json (primary source)."""
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)

            symbols = []
            for entry in alias_data.get("assets", {}).values():
                pionex_sym = entry.get("exchange_symbols", {}).get("pionex")
                if pionex_sym:
                    symbols.append(f"{pionex_sym}_USDT")

            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[pionex] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
        except Exception as e:
            logger.warning(f"[pionex] Failed to load symbols from JSON: {e}")
        return []

    async def _fetch_symbols_from_api(self) -> List[str]:
        """Fetch trading symbols from Pionex REST API (fallback)."""
        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(self._rest_url, timeout=timeout) as resp:
                    data = await resp.json()

            symbols = []
            for item in data.get("data", {}).get("symbols", []):
                symbol = item.get("symbol", "")
                quote = item.get("quoteCurrency", "")
                enable = item.get("enable", False)

                if quote.upper() in self._quote_currencies and enable:
                    symbols.append(symbol)

            symbols = sorted(set(symbols))
            logger.info(f"[pionex] Fetched {len(symbols)} symbols from API (fallback)")
            return symbols
        except Exception as e:
            logger.error(f"[pionex] API fallback failed: {e}")
            return []

    async def _fetch_symbols(self) -> List[str]:
        """Get trading symbols. Priority: JSON → API → hardcoded."""
        symbols = self._load_symbols_from_json()
        if symbols:
            return symbols

        symbols = await self._fetch_symbols_from_api()
        if symbols:
            return symbols

        logger.warning("[pionex] Using hardcoded seed symbols (last resort)")
        return [
            "BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "ADA_USDT",
            "DOGE_USDT", "AVAX_USDT", "DOT_USDT", "LINK_USDT", "LTC_USDT",
        ]

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to Pionex WebSocket, subscribe to ticker stream,
        and emit RawTick events to the ingestion queue.
        """
        self._symbols = await self._fetch_symbols()

        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(f"[pionex] ⚠️  TEST MODE: using {len(self._symbols)} symbols")

        if not self._symbols:
            logger.warning("[pionex] No symbols — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[pionex] Subscribing to {len(self._symbols)} symbols")

        async with websockets.connect(self._ws_url, ping_interval=20) as ws:
            # Subscribe to each symbol's ticker
            for sym in self._symbols:
                subscribe_msg = json.dumps({
                    "op": "SUBSCRIBE",
                    "topic": "TICKER",
                    "symbol": sym,
                })
                await ws.send(subscribe_msg)

            logger.info("[pionex] Connected and subscribed")

            async for message in ws:
                data = json.loads(message)

                topic = data.get("topic", "")
                if topic != "TICKER":
                    # Handle pong or subscription ack
                    continue

                ticker = data.get("data", {})
                symbol = ticker.get("symbol", "")

                try:
                    bid = float(ticker.get("bid", 0) or 0)
                    ask = float(ticker.get("ask", 0) or 0)
                    last = float(ticker.get("close", 0) or 0)
                    volume = float(ticker.get("volume", 0) or 0)
                except (ValueError, TypeError):
                    continue

                tick = RawTick(
                    exchange="pionex",
                    pair=symbol,  # "BTC_USDT" format
                    data={
                        "bid": bid,
                        "ask": ask,
                        "last": last,
                        "vwap": None,
                        "volume_24h": volume,
                    },
                    received_at=time.time(),
                )

                await self._emit(tick)
