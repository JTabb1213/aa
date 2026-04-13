"""
MEXC orderbook connector.

Subscribes to spot@public.depth.v3.api@{symbol} channel.

Note: MEXC may be geo-blocked in the US.

Message format:
  {
    "c": "spot@public.depth.v3.api@BTCUSDT",
    "d": {
      "bids": [{"p": "65000.00", "v": "1.5"}, ...],
      "asks": [{"p": "65001.00", "v": "2.0"}, ...],
    },
    "t": 1700000000000
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


class MexcOrderBookConnector(BaseExchange):
    NAME = "mexc"

    def __init__(self, queue: asyncio.Queue, symbols: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.MEXC_WS_URL
        self._symbols = symbols or self._load_symbols()

    def _load_symbols(self) -> List[str]:
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            symbols = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("mexc")
                if sym:
                    symbols.append(f"{sym}USDT")
            return sorted(set(symbols))
        except Exception as e:
            logger.warning(f"[mexc] Failed to load symbols: {e}")
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self._ws_url, ping_interval=30) as ws:
            subscribe = {
                "method": "SUBSCRIBE",
                "params": [
                    f"spot@public.depth.v3.api@{sym}" for sym in self._symbols
                ],
                "id": 1,
            }
            await ws.send(json.dumps(subscribe))
            logger.info(f"[mexc] Subscribed to depth for {len(self._symbols)} symbols")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Detect geo-block or subscription failure
                if isinstance(msg, dict) and "msg" in msg:
                    msg_text = str(msg.get("msg", ""))
                    if "Blocked" in msg_text:
                        raise ConnectionError(
                            f"[mexc] Subscription blocked (likely geo-restricted): {msg_text}"
                        )

                channel = msg.get("c", "")
                if not channel.startswith("spot@public.depth"):
                    continue

                # Extract symbol from channel: "spot@public.depth.v3.api@BTCUSDT"
                parts = channel.split("@")
                symbol = parts[-1] if len(parts) >= 3 else ""

                data = msg.get("d", {})
                raw_bids = data.get("bids", [])
                raw_asks = data.get("asks", [])

                bids = [
                    PriceLevel(float(b["p"]), float(b["v"]))
                    for b in raw_bids if "p" in b and "v" in b
                ]
                asks = [
                    PriceLevel(float(a["p"]), float(a["v"]))
                    for a in raw_asks if "p" in a and "v" in a
                ]

                # Sort properly (MEXC may not guarantee order)
                bids.sort(key=lambda x: x.price, reverse=True)
                asks.sort(key=lambda x: x.price)

                await self._emit(RawOrderBook(
                    exchange=self.NAME,
                    pair=symbol,  # e.g. "BTCUSDT"
                    bids=bids,
                    asks=asks,
                ))
