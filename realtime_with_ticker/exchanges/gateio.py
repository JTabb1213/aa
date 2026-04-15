"""
Gate.io websocket connector.

Connects to Gate.io's WebSocket v4 API, subscribes to the spot.tickers
channel for USDT pairs, and emits RawTick events onto the ingestion queue.

Gate.io WebSocket docs:
    https://www.gate.io/docs/developers/apiv4/ws/en/#spot-tickers

Subscription format:
    {
        "time": <unix_seconds>,
        "channel": "spot.tickers",
        "event": "subscribe",
        "payload": ["BTC_USDT", "ETH_USDT", ...]
    }

Update message format:
    {
        "time": 1710000000,
        "channel": "spot.tickers",
        "event": "update",
        "result": {
            "currency_pair": "BTC_USDT",
            "last":          "72000.50",
            "lowest_ask":    "72001.00",  # best ask
            "highest_bid":   "72000.00",  # best bid
            "change_percentage": "1.22",
            "base_volume":   "1234.56",
            "quote_volume":  "89012345.67",
            "high_24h":      "72500.00",
            "low_24h":       "71000.00"
        }
    }

Note: Gate.io symbol format is BTC_USDT (underscore separator).
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
# TEST_SYMBOLS_OVERRIDE = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]
# ==============================================================================


class GateioConnector(BaseExchange):
    """
    Gate.io exchange websocket connector (v4 spot tickers).

    Lifecycle:
        1. Load symbols from coin_aliases.json
        2. Subscribe to spot.tickers for all USDT pairs
        3. Receive highest_bid / lowest_ask updates → RawTick → queue
        4. Send periodic ping to keep the connection alive
    """

    NAME = "gateio"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.GATEIO_WS_URL
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """
        Load trading symbols from coin_aliases.json.
        Returns symbols in Gate.io format: BTC_USDT, ETH_USDT, …
        """
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)

            symbols = []
            for entry in alias_data.get("assets", {}).values():
                gateio_sym = entry.get("exchange_symbols", {}).get("gateio")
                if gateio_sym:
                    for quote in self._quote_currencies:
                        symbols.append(f"{gateio_sym.upper()}_{quote.upper()}")

            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[gateio] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
        except Exception as e:
            logger.warning(f"[gateio] Failed to load symbols from JSON: {e}")

        return []

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to Gate.io WebSocket, subscribe to spot.tickers,
        and emit RawTick events to the ingestion queue.
        """
        self._symbols = self._load_symbols_from_json()

        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(
                f"[gateio] ⚠️  TEST MODE: using {len(self._symbols)} symbols "
                f"— set TEST_SYMBOLS_OVERRIDE = None to disable"
            )

        if not self._symbols:
            logger.warning("[gateio] No symbols to subscribe to — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[gateio] Subscribing to {len(self._symbols)} symbols")

        async with websockets.connect(
            self._ws_url,
            ping_interval=None,  # We handle pings manually per Gate.io spec
        ) as ws:
            logger.info("[gateio] Connected")

            # Subscribe to spot.tickers for all symbols
            subscribe_msg = {
                "time": int(time.time()),
                "channel": "spot.tickers",
                "event": "subscribe",
                "payload": self._symbols,
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info(f"[gateio] Sent subscribe for {len(self._symbols)} pairs")

            # Start the periodic ping task (Gate.io times out without pings)
            ping_task = asyncio.create_task(self._ping_loop(ws))

            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    channel = msg.get("channel", "")
                    event = msg.get("event", "")

                    # Skip pong and subscribe-ack messages
                    if channel == "spot.pong" or event in ("subscribe", "unsubscribe"):
                        continue

                    if channel != "spot.tickers" or event != "update":
                        continue

                    result = msg.get("result")
                    if not result or not isinstance(result, dict):
                        continue

                    pair = result.get("currency_pair", "")  # e.g. "BTC_USDT"

                    try:
                        bid = float(result.get("highest_bid") or 0)
                        ask = float(result.get("lowest_ask") or 0)
                        last = float(result.get("last") or 0)
                        volume = float(result.get("base_volume") or 0)
                    except (ValueError, TypeError):
                        continue

                    tick = RawTick(
                        exchange="gateio",
                        pair=pair,
                        data={
                            "bid": bid if bid else None,
                            "ask": ask if ask else None,
                            "last": last if last else None,
                            "vwap": None,
                            "volume_24h": volume if volume else None,
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
        """
        Send a spot.ping every 10 seconds to keep the Gate.io connection alive.
        Gate.io responds with spot.pong; no response needed from our side.
        """
        while True:
            await asyncio.sleep(10)
            try:
                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "spot.ping",
                }))
            except Exception:
                break
