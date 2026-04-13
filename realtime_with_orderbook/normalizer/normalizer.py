"""
Normalizer — converts RawOrderBook → NormalizedOrderBook.

Handles all exchange pair format quirks:
  Kraken:   "BTC/USDT"
  Coinbase: "BTC-USDT"
  Binance:  "BTCUSDT"
  Bybit:    "BTCUSDT"
  OKX:      "BTC-USDT"
  Pionex:   "BTC_USDT"
  Gate.io:  "BTC_USDT"
  MEXC:     "BTCUSDT"
"""

import logging
from typing import Optional

from core.models import RawOrderBook, NormalizedOrderBook
from normalizer.aliases import AliasResolver

logger = logging.getLogger(__name__)


class Normalizer:
    """Converts RawOrderBook (exchange-specific) → NormalizedOrderBook (canonical)."""

    def __init__(self, alias_resolver: Optional[AliasResolver] = None):
        self._aliases = alias_resolver or AliasResolver()
        self._unresolved_logged: set = set()

    def normalize(self, raw: RawOrderBook) -> Optional[NormalizedOrderBook]:
        """
        Normalize a raw orderbook into unified format.
        Returns None if the base symbol cannot be resolved.
        """
        base_symbol, quote = self._split_pair(raw.pair)
        if not base_symbol:
            return None

        coin_id = self._aliases.resolve(base_symbol)
        if not coin_id:
            if base_symbol not in self._unresolved_logged:
                logger.debug(
                    f"Cannot resolve '{base_symbol}' from "
                    f"{raw.exchange} pair {raw.pair}"
                )
                self._unresolved_logged.add(base_symbol)
            return None

        return NormalizedOrderBook(
            coin_id=coin_id,
            quote=quote.lower(),
            exchange=raw.exchange,
            bids=raw.bids,
            asks=raw.asks,
            timestamp=raw.received_at,
        )

    @staticmethod
    def _split_pair(pair: str) -> tuple:
        """
        Split an exchange pair string into (base, quote).

        Handles:
          "BTC/USDT"  → ("BTC", "USDT")   Kraken
          "BTC-USDT"  → ("BTC", "USDT")   Coinbase, OKX
          "BTC_USDT"  → ("BTC", "USDT")   Pionex, Gate.io
          "BTCUSDT"   → ("BTC", "USDT")   Binance, Bybit, MEXC
        """
        if "/" in pair:
            parts = pair.split("/")
            return parts[0], parts[1]

        if "-" in pair:
            parts = pair.split("-")
            return parts[0], parts[1]

        if "_" in pair:
            parts = pair.split("_")
            return parts[0], parts[1]

        # Concatenated format: "BTCUSDT"
        QUOTES = ["USDT", "USDC", "BUSD", "TUSD", "USD", "BTC", "ETH"]
        upper = pair.upper()
        for q in QUOTES:
            if upper.endswith(q):
                base = upper[:-len(q)]
                if base:
                    return base, q
        return "", ""
