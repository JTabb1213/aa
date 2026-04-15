"""
Lightweight alias resolver for the realtime server.

Reads the shared data/coin_aliases.json file (the same one used by
the backend and admin services) and provides O(1) lookups from any
exchange symbol or name → canonical coin ID.

This is a streamlined version of backend/services/alias/resolver.py,
containing only the incoming-direction resolution needed by the
realtime pipeline (exchange symbol → canonical ID).

JSON schema:
  {
    "assets": {
      "bitcoin": {
        "symbol":          "BTC",
        "aliases":         ["bitcoin", "btc", "xbt"],
        "exchange_symbols": { "kraken": "XBT" }
      }
    }
  }

The resolver builds its lookup dict with clear priority:
  1. canonical_id       → itself
  2. symbol             → canonical_id
  3. aliases[]          → canonical_id
  4. exchange_symbols values → canonical_id  (highest priority, applied last)

The alias map is loaded once at startup into an in-memory dict.
To refresh after running tools/populate_aliases/main.py, call
resolver.reload() or restart the service.
"""

import json
import logging
import os
from typing import Dict, Optional

import config

logger = logging.getLogger(__name__)


class AliasResolver:
    """
    Resolves exchange symbols/names to canonical coin IDs in O(1).

    Examples:
        resolve("XBT")     → "bitcoin"   (Kraken exchange code)
        resolve("btc")     → "bitcoin"   (standard symbol)
        resolve("Bitcoin")  → "bitcoin"   (display name)
        resolve("SOL")     → "solana"
        resolve("unknown")  → None
    """

    def __init__(self, json_path: str = config.ALIAS_JSON_PATH):
        self._json_path = os.path.abspath(json_path)
        self._lookup: Dict[str, str] = {}       # alias_lower → canonical_id
        self._symbols: Dict[str, str] = {}      # canonical_id → standard symbol
        self._load()

    def _load(self) -> None:
        """Load alias map from JSON and build in-memory lookup dict."""
        try:
            with open(self._json_path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.error(f"Alias map not found at {self._json_path}")
            logger.error("Run: python tools/populate_aliases/main.py")
            return
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in alias map: {e}")
            return

        assets = data.get("assets", {})

        for canonical_id, entry in assets.items():
            # 1. Canonical ID resolves to itself
            self._lookup[canonical_id.lower()] = canonical_id

            # 2. Standard symbol → canonical_id
            symbol = entry.get("symbol", "")
            if symbol:
                self._symbols[canonical_id] = symbol
                self._lookup[symbol.lower()] = canonical_id

            # 3. All aliases (curated + auto-derived)
            for alias in entry.get("aliases", []):
                self._lookup[alias.lower()] = canonical_id

            # 4. Exchange symbols → canonical_id (highest priority)
            # These are curated overrides, applied last so they always win.
            for sym in entry.get("exchange_symbols", {}).values():
                self._lookup[sym.lower()] = canonical_id

        meta = data.get("_meta", {})
        logger.info(
            f"Loaded {len(assets)} assets / {len(self._lookup)} lookup entries "
            f"(updated: {meta.get('updated_at', 'unknown')})"
        )

    def resolve(self, term: str) -> Optional[str]:
        """
        Resolve any alias/symbol/name to its canonical coin ID.
        Returns None if not found.
        """
        if not term:
            return None
        return self._lookup.get(term.strip().lower())

    def reload(self) -> None:
        """Reload alias map from disk without restarting the service."""
        self._lookup.clear()
        self._symbols.clear()
        self._load()

    @property
    def total_aliases(self) -> int:
        return len(self._lookup)

    @property
    def total_assets(self) -> int:
        return len(set(self._lookup.values()))
