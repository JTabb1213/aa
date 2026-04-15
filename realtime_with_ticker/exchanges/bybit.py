"""
Bybit websocket connector.

Connects to Bybit's WebSocket v5 API, subscribes to the tickers stream
for USDT spot pairs, and emits RawTick events onto the ingestion queue.

Bybit WebSocket v5 docs:
    https://bybit-exchange.github.io/docs/v5/ws/connect

Message format (tickers):
    {
        "topic": "tickers.BTCUSDT",
        "type": "snapshot" | "delta",
        "data": {
            "symbol": "BTCUSDT",
            "lastPrice": "65432.10",
            "bid1Price": "65432.00",
            "ask1Price": "65433.00",
            "volume24h": "12345.67",
            ...
        },
        "ts": 1710000000000
    }

Note: Bybit uses USDT pairs for spot. The normalizer handles mapping.
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
# TEST MODE — swap in a short list for development
# ==============================================================================
TEST_SYMBOLS_OVERRIDE = None
# TEST_SYMBOLS_OVERRIDE = [
#     "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
#     "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "LTCUSDT",
# ]
# ==============================================================================


class BybitConnector(BaseExchange):
    """
    Bybit exchange websocket connector (v5 spot).

    Lifecycle:
        1. Load symbols from coin_aliases.json (or fallback to API)
        2. Subscribe to tickers for all symbols
        3. Parse incoming messages → RawTick → ingestion queue
    """

    NAME = "bybit"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.BYBIT_WS_URL
        self._rest_url = config.BYBIT_REST_URL
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """Load trading symbols from coin_aliases.json (primary source)."""
        logger.debug(f"[bybit] Attempting to load from: {config.ALIAS_JSON_PATH}")
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)

            symbols = []
            for entry in alias_data.get("assets", {}).values():
                bybit_sym = entry.get("exchange_symbols", {}).get("bybit")
                if bybit_sym:
                    symbols.append(f"{bybit_sym}USDT")

            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[bybit] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
            logger.warning("[bybit] JSON loaded but no symbols found for bybit")
        except Exception as e:
            logger.warning(f"[bybit] Failed to load symbols from JSON: {e}")
        return []

    async def _fetch_symbols_from_api(self) -> List[str]:
        """Fetch trading symbols from Bybit REST API (fallback)."""
        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                params = {"category": "spot"}
                async with session.get(self._rest_url, params=params, timeout=timeout) as resp:
                    data = await resp.json()

            symbols = []
            for item in data.get("result", {}).get("list", []):
                symbol = item.get("symbol", "")
                quote = item.get("quoteCoin", "")
                status = item.get("status", "")

                if quote.upper() in self._quote_currencies and status == "Trading":
                    symbols.append(symbol)

            symbols = sorted(set(symbols))
            logger.info(f"[bybit] Fetched {len(symbols)} symbols from API (fallback)")
            return symbols
        except Exception as e:
            logger.error(f"[bybit] API fallback failed: {e}")
            return []

    async def _fetch_symbols(self) -> List[str]:
        """Get trading symbols. Priority: JSON → API → hardcoded."""
        symbols = self._load_symbols_from_json()
        if symbols:
            return symbols

        symbols = await self._fetch_symbols_from_api()
        if symbols:
            return symbols

        logger.warning("[bybit] Using hardcoded seed symbols (last resort)")
        return [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
            "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "LTCUSDT",
        ]

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to Bybit v5 WebSocket, subscribe to tickers,
        and emit RawTick events to the ingestion queue.

        Note: Bybit spot tickers only provide lastPrice (no bid/ask).
        The normalizer uses lastPrice as bid/ask fallback.
        """
        self._symbols = await self._fetch_symbols()

        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(
                f"[bybit] ⚠️  TEST MODE: using {len(self._symbols)} symbols"
            )

        if not self._symbols:
            logger.warning("[bybit] No symbols — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[bybit] Subscribing to {len(self._symbols)} symbols")

        subscribe_msg = json.dumps({
            "op": "subscribe",
            "args": [f"tickers.{sym}" for sym in self._symbols],
        })

        async with websockets.connect(self._ws_url, ping_interval=20) as ws:
            logger.info(f"[bybit] WebSocket connected to {self._ws_url}")
            await ws.send(subscribe_msg)
            logger.info("[bybit] Connected and subscribed")

            async for message in ws:
                data = json.loads(message)
                topic = data.get("topic", "")

                if not topic.startswith("tickers."):
                    if data.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong"}))
                    continue

                ticker = data.get("data", {})
                symbol = ticker.get("symbol", "")
                if not symbol:
                    continue

                try:
                    # Bybit spot tickers have lastPrice but no bid1Price/ask1Price
                    last   = float(ticker.get("lastPrice",  0) or 0)
                    volume = float(ticker.get("volume24h",  0) or 0)
                except (ValueError, TypeError):
                    continue

                if not last:
                    continue

                tick = RawTick(
                    exchange="bybit",
                    pair=symbol,
                    data={
                        "bid": None,
                        "ask": None,
                        "last": last,
                        "vwap": None,
                        "volume_24h": volume,
                    },
                    received_at=time.time(),
                )
                await self._emit(tick)
