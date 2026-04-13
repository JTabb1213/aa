"""
Kraken orderbook connector (WebSocket v2).

Subscribes to the "book" channel with configurable depth.
Emits RawOrderBook events for each snapshot/update.

Message format (v2 book):
  {
    "channel": "book",
    "type": "snapshot" | "update",
    "data": [{
      "symbol": "BTC/USDT",
      "bids": [{"price": 65000.0, "qty": 1.5}, ...],
      "asks": [{"price": 65001.0, "qty": 2.0}, ...],
      ...
    }]
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


class KrakenOrderBookConnector(BaseExchange):
    NAME = "kraken"

    def __init__(self, queue: asyncio.Queue, pairs: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.KRAKEN_WS_URL
        self._pairs = pairs or self._load_pairs()
        self._depth = config.ORDERBOOK_DEPTH
        # Maintain local book state per symbol for incremental updates
        self._books: dict = {}  # symbol -> {"bids": {price: qty}, "asks": {price: qty}}

    def _load_pairs(self) -> List[str]:
        """Load pairs from coin_aliases.json in Kraken v2 format (BTC/USDT)."""
        V1_TO_V2 = {"XBT": "BTC", "XDG": "DOGE"}
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            pairs = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("kraken")
                if sym:
                    base = V1_TO_V2.get(sym, sym)
                    pairs.append(f"{base}/USDT")
            return sorted(set(pairs))
        except Exception as e:
            logger.warning(f"[kraken] Failed to load pairs: {e}")
            return ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self._ws_url, ping_interval=30) as ws:
            subscribe = {
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": self._pairs,
                    "depth": max(10, self._depth),  # min valid: 10, 25, 100, 500, 1000
                },
            }
            await ws.send(json.dumps(subscribe))
            logger.info(f"[kraken] Subscribed to book for {len(self._pairs)} pairs")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                channel = msg.get("channel")
                msg_type = msg.get("type")

                if channel != "book" or msg_type not in ("snapshot", "update"):
                    continue

                for entry in msg.get("data", []):
                    symbol = entry.get("symbol", "")
                    raw_bids = entry.get("bids", [])
                    raw_asks = entry.get("asks", [])

                    if msg_type == "snapshot":
                        # Full snapshot — replace local book
                        self._books[symbol] = {
                            "bids": {float(b["price"]): float(b["qty"]) for b in raw_bids},
                            "asks": {float(a["price"]): float(a["qty"]) for a in raw_asks},
                        }
                    else:
                        # Incremental update — merge into local book
                        if symbol not in self._books:
                            continue
                        book = self._books[symbol]
                        for b in raw_bids:
                            p, q = float(b["price"]), float(b["qty"])
                            if q == 0:
                                book["bids"].pop(p, None)
                            else:
                                book["bids"][p] = q
                        for a in raw_asks:
                            p, q = float(a["price"]), float(a["qty"])
                            if q == 0:
                                book["asks"].pop(p, None)
                            else:
                                book["asks"][p] = q

                    # Emit current state
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
                        pair=symbol,
                        bids=bids,
                        asks=asks,
                    ))
