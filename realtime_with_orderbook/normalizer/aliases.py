"""
Alias resolver — same pattern as realtime_with_ticker.

Reads data/coin_aliases.json and provides O(1) lookups
from any exchange symbol → canonical coin ID.
"""

import json
import logging
import os
from typing import Dict, Optional

import config

logger = logging.getLogger(__name__)


class AliasResolver:
    """
    Resolves exchange symbols to canonical coin IDs.

    Examples:
        resolve("XBT") → "bitcoin"
        resolve("BTC") → "bitcoin"
        resolve("SOL") → "solana"
    """

    def __init__(self, json_path: str = config.ALIAS_JSON_PATH):
        self._json_path = os.path.abspath(json_path)
        self._lookup: Dict[str, str] = {}
        self._symbols: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        try:
            with open(self._json_path) as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.error(f"Alias map not found: {self._json_path}")
            return
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in alias map: {e}")
            return

        assets = data.get("assets", {})
        for canonical_id, entry in assets.items():
            self._lookup[canonical_id.lower()] = canonical_id

            symbol = entry.get("symbol", "")
            if symbol:
                self._symbols[canonical_id] = symbol
                self._lookup[symbol.lower()] = canonical_id

            for alias in entry.get("aliases", []):
                self._lookup[alias.lower()] = canonical_id

            for sym in entry.get("exchange_symbols", {}).values():
                self._lookup[sym.lower()] = canonical_id

        logger.info(f"Loaded {len(assets)} assets / {len(self._lookup)} lookup entries")

    def resolve(self, symbol: str) -> Optional[str]:
        """Resolve a symbol to canonical coin ID, or None."""
        return self._lookup.get(symbol.lower())

    @property
    def total_assets(self) -> int:
        return len(self._symbols)

    @property
    def total_aliases(self) -> int:
        return len(self._lookup)
