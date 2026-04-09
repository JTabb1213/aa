"""
Binance websocket connector.

Connects to Binance's WebSocket API, subscribes to the ticker stream
for USDT pairs, and emits RawTick events onto the ingestion queue.

Binance WebSocket docs:
    https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-ticker-streams

Message format (24hr ticker):
    {
        "e": "24hrTicker",
        "s": "BTCUSDT",
        "c": "65432.10",     # last price
        "b": "65432.00",     # best bid
        "a": "65433.00",     # best ask
        "v": "12345.67",     # volume
        "q": "789012345.00", # quote volume
        ...
    }

Note: Binance primarily uses USDT pairs, not USD. The normalizer
handles the mapping from USDT to canonical coin IDs.
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
#     "btcusdt", "ethusdt", "solusdt", "xrpusdt", "adausdt",
#     "dogeusdt", "avaxusdt", "dotusdt", "linkusdt", "ltcusdt",
# ]
# ==============================================================================


class BinanceConnector(BaseExchange):
    """
    Binance exchange websocket connector.

    Lifecycle:
        1. Load symbols from coin_aliases.json (or fallback to API)
        2. Build combined stream URL for all symbols
        3. Connect to WebSocket, receive ticker updates
        4. Parse incoming messages → RawTick → ingestion queue
    
    Binance allows subscribing to multiple streams via a combined URL:
        wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker
    """

    NAME = "binance"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_base_url = config.BINANCE_WS_URL
        self._rest_url = config.BINANCE_REST_URL
        # Binance uses USDT, not USD
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """
        Load trading symbols from coin_aliases.json (primary source).
        
        The JSON file is the source of truth for which symbols to subscribe.
        """
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)
            
            symbols = []
            for entry in alias_data.get("assets", {}).values():
                binance_sym = entry.get("exchange_symbols", {}).get("binance")
                if binance_sym:
                    # Binance uses lowercase symbols like "btcusdt"
                    symbols.append(f"{binance_sym.lower()}usdt")
            
            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[binance] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
        except Exception as e:
            logger.warning(f"[binance] Failed to load symbols from JSON: {e}")
        
        return []

    async def _fetch_symbols_from_api(self) -> List[str]:
        """
        Fetch trading symbols from Binance's REST API (fallback).
        
        Used only if coin_aliases.json is missing or empty.
        """
        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(self._rest_url, timeout=timeout) as resp:
                    data = await resp.json()

            symbols = []
            for item in data.get("symbols", []):
                symbol = item.get("symbol", "")
                quote = item.get("quoteAsset", "")
                status = item.get("status", "")

                # Filter by quote currency (USDT) and trading status
                if quote.upper() in self._quote_currencies and status == "TRADING":
                    symbols.append(symbol.lower())

            symbols = sorted(set(symbols))
            logger.info(f"[binance] Fetched {len(symbols)} symbols from API (fallback)")
            return symbols

        except Exception as e:
            logger.error(f"[binance] API fallback failed: {e}")
            return []

    async def _fetch_symbols(self) -> List[str]:
        """
        Get trading symbols to subscribe to.
        
        Priority:
          1. coin_aliases.json (primary, fast, offline-capable)
          2. Binance REST API (fallback if JSON missing/empty)
          3. Hardcoded seed list (last resort)
        """
        # Primary: JSON file
        symbols = self._load_symbols_from_json()
        if symbols:
            return symbols
        
        # Fallback: API
        symbols = await self._fetch_symbols_from_api()
        if symbols:
            return symbols
        
        # Last resort: hardcoded seed
        logger.warning("[binance] Using hardcoded seed symbols (last resort)")
        return [
            "btcusdt", "ethusdt", "solusdt", "xrpusdt", "adausdt",
            "dogeusdt", "avaxusdt", "dotusdt", "linkusdt", "ltcusdt",
        ]

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to Binance WebSocket, subscribe to ticker for all symbols,
        and emit RawTick events to the ingestion queue.
        """
        self._symbols = await self._fetch_symbols()

        # ==============================================================================
        # TEST MODE — override full symbol list with short dev list if set above
        # ==============================================================================
        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(
                f"[binance] ⚠️  TEST MODE ACTIVE: using {len(self._symbols)} symbols "
                f"instead of full list — set TEST_SYMBOLS_OVERRIDE = None to disable"
            )
        # ==============================================================================

        if not self._symbols:
            logger.warning("[binance] No symbols to subscribe to — sleeping 30s")
            await asyncio.sleep(30)
            return

        # Binance combined streams URL
        # Format: wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker
        streams = "/".join(f"{sym}@ticker" for sym in self._symbols)
        ws_url = f"{self._ws_base_url}/stream?streams={streams}"
        
        logger.info(f"[binance] Subscribing to {len(self._symbols)} symbols")

        async with websockets.connect(ws_url, ping_interval=30) as ws:
            logger.info("[binance] Connected to WebSocket")

            async for message in ws:
                data = json.loads(message)
                
                # Combined stream wraps data in {"stream": "btcusdt@ticker", "data": {...}}
                ticker = data.get("data", data)
                event_type = ticker.get("e")

                # Skip non-ticker messages
                if event_type != "24hrTicker":
                    continue

                symbol = ticker.get("s", "")  # e.g., "BTCUSDT"

                # Parse fields
                try:
                    bid = float(ticker.get("b", 0) or 0)
                    ask = float(ticker.get("a", 0) or 0)
                    last = float(ticker.get("c", 0) or 0)
                    volume = float(ticker.get("v", 0) or 0)
                except (ValueError, TypeError):
                    continue

                tick = RawTick(
                    exchange="binance",
                    pair=symbol,  # "BTCUSDT" format
                    data={
                        "bid": bid,
                        "ask": ask,
                        "last": last,
                        "vwap": None,  # Binance doesn't provide VWAP in ticker
                        "volume_24h": volume,
                    },
                    received_at=time.time(),
                )

                await self._emit(tick)
