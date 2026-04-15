"""
MEXC websocket connector.

Connects to MEXC's WebSocket v3 API, subscribes to the bookTicker stream
for USDT pairs, and emits RawTick events onto the ingestion queue.

MEXC WebSocket docs:
    https://mexcdevelop.github.io/apidocs/spot_v3_en/#websocket-market-streams

Subscription format:
    {"method": "SUBSCRIPTION", "params": ["spot@public.bookTicker.v3.api@BTCUSDT"]}

Message format (bookTicker — best bid & ask):
    {
        "c": "spot@public.bookTicker.v3.api@BTCUSDT",
        "d": {
            "s": "BTCUSDT",   # symbol
            "b": "72000.00",  # best bid price
            "B": "0.50",      # best bid quantity
            "a": "72001.00",  # best ask price
            "A": "0.30"       # best ask quantity
        },
        "t": 1710000000000    # timestamp ms
    }

Note: MEXC symbol format is BTCUSDT (no separator, identical to Binance).
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

import websockets

import config
from core.models import RawTick
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


# ==============================================================================
# TEST MODE — swap in a short list for development
# ==============================================================================
TEST_SYMBOLS_OVERRIDE = None
# TEST_SYMBOLS_OVERRIDE = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
# ==============================================================================


class MexcConnector(BaseExchange):
    """
    MEXC exchange websocket connector (v3 spot bookTicker).

    Lifecycle:
        1. Load symbols from coin_aliases.json
        2. Subscribe to bookTicker streams for all USDT pairs
        3. Receive bid/ask updates → RawTick → ingestion queue
        4. Send periodic PING to keep the connection alive
    """

    NAME = "mexc"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.MEXC_WS_URL
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """
        Load trading symbols from coin_aliases.json.
        Returns symbols in MEXC format: BTCUSDT, ETHUSDT, …
        """
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)

            symbols = []
            for entry in alias_data.get("assets", {}).values():
                mexc_sym = entry.get("exchange_symbols", {}).get("mexc")
                if mexc_sym:
                    # MEXC uses uppercase concatenated format: BTCUSDT
                    for quote in self._quote_currencies:
                        symbols.append(f"{mexc_sym.upper()}{quote.upper()}")

            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[mexc] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
        except Exception as e:
            logger.warning(f"[mexc] Failed to load symbols from JSON: {e}")

        return []

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to MEXC WebSocket, subscribe to bookTicker for all symbols,
        and emit RawTick events to the ingestion queue.
        """
        self._symbols = self._load_symbols_from_json()

        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(
                f"[mexc] ⚠️  TEST MODE: using {len(self._symbols)} symbols "
                f"— set TEST_SYMBOLS_OVERRIDE = None to disable"
            )

        if not self._symbols:
            logger.warning("[mexc] No symbols to subscribe to — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[mexc] Subscribing to {len(self._symbols)} bookTicker streams")

        async with websockets.connect(
            self._ws_url,
            ping_interval=None,   # We handle pings manually per MEXC spec
        ) as ws:
            logger.info("[mexc] Connected")

            # Subscribe to bookTicker for every symbol
            params = [
                f"spot@public.bookTicker.v3.api@{sym}"
                for sym in self._symbols
            ]
            await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
            logger.info(f"[mexc] Sent SUBSCRIPTION for {len(params)} streams")

            # Start the periodic ping task (MEXC requires a PING every ~30s)
            ping_task = asyncio.create_task(self._ping_loop(ws))

            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Skip ACK / PONG messages
                    code = msg.get("code")
                    if code is not None:
                        msg_text = msg.get("msg", "")
                        if "Blocked" in msg_text:
                            logger.warning(
                                f"[mexc] Geo-blocked by MEXC (US restriction?): {msg_text}"
                            )
                            raise ConnectionError("MEXC subscription blocked — likely US geo-restriction")
                        if code != 0:
                            logger.warning(f"[mexc] Server message: {msg}")
                        continue

                    data = msg.get("d")
                    if not data or not isinstance(data, dict):
                        continue

                    symbol = data.get("s", "")  # e.g. "BTCUSDT"

                    try:
                        bid = float(data.get("b") or 0)
                        ask = float(data.get("a") or 0)
                    except (ValueError, TypeError):
                        continue

                    tick = RawTick(
                        exchange="mexc",
                        pair=symbol,
                        data={
                            "bid": bid if bid else None,
                            "ask": ask if ask else None,
                            "last": None,
                            "vwap": None,
                            "volume_24h": None,
                        },
                        received_at=time.time(),
                    )
                    await self._emit(tick)

            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    async def _ping_loop(self, ws) -> None:
        """Send a PING every 30 seconds to keep the MEXC connection alive."""
        while True:
            await asyncio.sleep(30)
            try:
                await ws.send(json.dumps({"method": "PING"}))
            except Exception:
                break
