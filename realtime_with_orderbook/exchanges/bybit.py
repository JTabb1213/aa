"""
Bybit orderbook connector (v5 spot).

Subscribes to orderbook.{depth}.{symbol} channel.

Message format:
  {
    "topic": "orderbook.5.BTCUSDT",
    "type": "snapshot" | "delta",
    "data": {
      "s": "BTCUSDT",
      "b": [["65000.00", "1.5"], ...],   # bids
      "a": [["65001.00", "2.0"], ...],   # asks
      "u": 123456,
    },
    "ts": 1700000000000
  }
"""

import asyncio
import json
import logging
from typing import List, Optional

import websockets

import config
from core.models import RawOrderBook, PriceLevel
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


class BybitOrderBookConnector(BaseExchange):
    NAME = "bybit"

    def __init__(self, queue: asyncio.Queue, symbols: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.BYBIT_WS_URL
        self._symbols = symbols or self._load_symbols()
        self._depth = config.ORDERBOOK_DEPTH
        self._books: dict = {}

    def _load_symbols(self) -> List[str]:
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            symbols = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("bybit")
                if sym:
                    symbols.append(f"{sym}USDT")
            return sorted(set(symbols))
        except Exception as e:
            logger.warning(f"[bybit] Failed to load symbols: {e}")
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self._ws_url, ping_interval=20) as ws:
            # Bybit allows max 10 topics per subscribe message.
            # Send in batches to avoid silent rejection of extra topics.
            args = [f"orderbook.50.{sym}" for sym in self._symbols]
            for i in range(0, len(args), 10):
                batch = args[i:i + 10]
                await ws.send(json.dumps({"op": "subscribe", "args": batch}))
            logger.info(f"[bybit] Subscribed to orderbook for {len(self._symbols)} symbols")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Handle pong
                if msg.get("op") == "pong" or msg.get("ret_msg") == "pong":
                    continue

                topic = msg.get("topic", "")
                if not topic.startswith("orderbook."):
                    continue

                data = msg.get("data", {})
                symbol = data.get("s", "")
                msg_type = msg.get("type", "")
                raw_bids = data.get("b", [])
                raw_asks = data.get("a", [])

                if msg_type == "snapshot":
                    self._books[symbol] = {
                        "bids": {float(b[0]): float(b[1]) for b in raw_bids},
                        "asks": {float(a[0]): float(a[1]) for a in raw_asks},
                    }
                elif msg_type == "delta":
                    if symbol not in self._books:
                        continue
                    book = self._books[symbol]
                    for b in raw_bids:
                        p, q = float(b[0]), float(b[1])
                        if q == 0:
                            book["bids"].pop(p, None)
                        else:
                            book["bids"][p] = q
                    for a in raw_asks:
                        p, q = float(a[0]), float(a[1])
                        if q == 0:
                            book["asks"].pop(p, None)
                        else:
                            book["asks"][p] = q
                else:
                    continue

                book = self._books.get(symbol, {"bids": {}, "asks": {}})
                bids = sorted(
                    [PriceLevel(p, q) for p, q in book["bids"].items()],
                    key=lambda x: x.price, reverse=True,
                )[:self._depth]
                asks = sorted(
                    [PriceLevel(p, q) for p, q in book["asks"].items()],
                    key=lambda x: x.price,
                )[:self._depth]

                await self._emit(RawOrderBook(
                    exchange=self.NAME,
                    pair=symbol,  # e.g. "BTCUSDT"
                    bids=bids,
                    asks=asks,
                ))
