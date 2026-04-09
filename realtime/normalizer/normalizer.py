"""
Normalizer — converts exchange-specific RawTick events into
unified NormalizedTick format using the alias resolver.

This is the single place where exchange-specific quirks are
translated into a common language the rest of the system speaks.

Each exchange has its own pair format:
  Kraken:   "XBT/USD"
  Binance:  "BTCUSDT"    (future)
  Coinbase: "BTC-USD"    (future)

The normalizer handles all of these and outputs a clean NormalizedTick
with a canonical coin_id (e.g. "bitcoin") that the rest of the
pipeline uses as the universal identifier.
"""

import logging
from typing import Optional

from core.models import RawTick, NormalizedTick
from normalizer.aliases import AliasResolver

logger = logging.getLogger(__name__)


class Normalizer:
    """
    Converts RawTick (exchange-specific) → NormalizedTick (canonical).

    Responsibilities:
      1. Split exchange pair ("XBT/USD") into base + quote
      2. Resolve base symbol to canonical coin ID via alias resolver
      3. Compute derived fields (mid price, spread %)
      4. Return a clean NormalizedTick, or None if unresolvable
    """

    def __init__(self, alias_resolver: Optional[AliasResolver] = None):
        self._aliases = alias_resolver or AliasResolver()
        # Track unresolved symbols to avoid spamming logs
        self._unresolved_logged: set = set()

    def normalize(self, tick: RawTick) -> Optional[NormalizedTick]:
        """
        Normalize a raw exchange tick into unified format.

        Returns None if the base symbol cannot be resolved to a
        canonical coin ID (i.e. we don't know what coin this is).
        """
        # --- Split pair into base and quote ---
        base_symbol, quote_currency = self._split_pair(tick.pair)
        if not base_symbol:
            return None

        # --- Resolve base symbol to canonical coin ID ---
        coin_id = self._aliases.resolve(base_symbol)
        if not coin_id:
            # Log once per unknown symbol, then suppress
            if base_symbol not in self._unresolved_logged:
                logger.debug(
                    f"Cannot resolve '{base_symbol}' from "
                    f"{tick.exchange} pair {tick.pair}"
                )
                self._unresolved_logged.add(base_symbol)
            return None

        # --- Extract and compute fields ---
        data = tick.data
        bid = float(data.get("bid", 0))
        ask = float(data.get("ask", 0))
        mid = (bid + ask) / 2 if bid and ask else 0
        last = float(data.get("last", 0))
        spread_pct = round((ask - bid) / mid * 100, 4) if mid else 0

        return NormalizedTick(
            coin_id=coin_id,
            quote=quote_currency.lower(),
            exchange=tick.exchange,
            price=mid,
            bid=bid,
            ask=ask,
            last=last,
            vwap=data.get("vwap"),
            volume_24h=data.get("volume_24h"),
            spread_pct=spread_pct,
            timestamp=tick.received_at,
        )

    @staticmethod
    def _split_pair(pair: str) -> tuple:
        """
        Split an exchange pair string into (base, quote).

        Handles different exchange pair formats:
          "XBT/USD"   → ("XBT", "USD")   Kraken
          "ETH-USD"   → ("ETH", "USD")   Coinbase
          "BTCUSDT"   → ("BTC", "USDT")  Binance

        Returns ("", "") if the pair format is not recognized.
        """
        # Kraken format: "XBT/USD"
        if "/" in pair:
            parts = pair.split("/")
            return parts[0], parts[1]

        # Coinbase / OKX format: "BTC-USD", "BTC-USDT"
        if "-" in pair:
            parts = pair.split("-")
            return parts[0], parts[1]

        # Pionex format: "BTC_USDT"
        if "_" in pair:
            parts = pair.split("_")
            return parts[0], parts[1]

        # Binance / Bybit concatenated format: "BTCUSDT"
        # Check known quote currencies (longest first to avoid partial matches)
        CONCAT_QUOTES = ["USDT", "USDC", "BUSD", "TUSD", "USD", "BTC", "ETH", "BNB"]
        pair_upper = pair.upper()
        for quote in CONCAT_QUOTES:
            if pair_upper.endswith(quote):
                base = pair_upper[:-len(quote)]
                if base:  # ensure we actually have a base
                    return base, quote

        return "", ""
