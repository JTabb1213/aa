"""
Arbitrage signal scanner.

Scans the in-memory price cache periodically, comparing bid/ask prices
across all tracked exchanges for each coin. Emits an ArbitrageSignal
when the net spread — after taker fees on both sides — is positive and
above the configured minimum threshold.

─────────────────────────────────────────────────────────────────────
How cross-exchange arbitrage works (important)
─────────────────────────────────────────────────────────────────────
You need pre-funded accounts on BOTH exchanges.

  1. You see a price discrepancy: Exchange A's ask < Exchange B's bid
  2. You place a BUY  order on Exchange A (paying their ask price)
  3. You place a SELL order on Exchange B (receiving their bid price)
  4. Both orders execute on each exchange's internal ledger — no
     on-chain blockchain transfer is needed. This takes milliseconds,
     not minutes or hours.

On-chain transfers are only needed if you physically move coins
between exchanges to rebalance funds — that's a separate, slower
operation you'd do manually when balances drift.

─────────────────────────────────────────────────────────────────────
The price you actually pay (after fees)
─────────────────────────────────────────────────────────────────────
When you place a market order, you are NOT guaranteed the price you
saw on screen. The price may move slightly between observation and
execution (slippage). For small trade sizes on liquid coins, this is
usually negligible (< 0.01%).

The fee math:

  effective_buy_cost    = ask  × (1 + buy_taker_fee)
  effective_sell_income = bid  × (1 - sell_taker_fee)
  net_profit_per_unit   = effective_sell_income - effective_buy_cost

  net_spread_pct = net_profit_per_unit / effective_buy_cost × 100

Only signals where net_spread_pct >= MIN_NET_SPREAD_PCT are emitted.
"""

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import config
from storage.price_cache import PriceCache

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Taker fee rates per exchange (as a fraction, not percent)
# ---------------------------------------------------------------------------
# "Taker" fee applies when your order is filled immediately (market order).
# This is what you'll use for arbitrage — you want instant execution.
# "Maker" fees are lower but only apply to limit orders that sit in the book.
#
# Standard rates (no volume discount). If you have a higher-tier account
# on any exchange, lower these to improve signal accuracy.
# ---------------------------------------------------------------------------
TAKER_FEES: Dict[str, float] = {
    "kraken":   0.0026,   # 0.26%
    "coinbase": 0.0060,   # 0.60%  (Coinbase Advanced Trade)
    "binance":  0.0010,   # 0.10%  (Binance.US)
    "bybit":    0.0010,   # 0.10%  (Bybit spot)
    "okx":      0.0010,   # 0.10%  (OKX spot)
    "pionex":   0.0005,   # 0.05%  (Pionex spot — low-fee exchange)
}
DEFAULT_TAKER_FEE = 0.0050  # fallback for any exchange not listed above


# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------

@dataclass
class ArbitrageSignal:
    """
    A detected arbitrage opportunity.

    Fields:
        coin_id         — canonical coin ID (e.g. "bitcoin")
        buy_exchange    — exchange to buy on (cheapest ask)
        buy_ask         — price you'd pay on the buy exchange
        sell_exchange   — exchange to sell on (highest bid)
        sell_bid        — price you'd receive on the sell exchange
        gross_spread_pct — raw spread before fees: (sell_bid - buy_ask) / buy_ask * 100
        net_spread_pct   — spread after both taker fees (this is your actual edge)
        est_profit_usd   — estimated profit for a TRADE_SIZE_USD position
    """
    coin_id: str
    buy_exchange: str
    buy_ask: float
    sell_exchange: str
    sell_bid: float
    gross_spread_pct: float
    net_spread_pct: float
    est_profit_usd: float
    timestamp: float = field(default_factory=time.time)

    def __str__(self) -> str:
        return (
            f"[signal] {self.coin_id:<20} "
            f"BUY  {self.buy_exchange:<10} ask=${self.buy_ask:<12.4f} "
            f"SELL {self.sell_exchange:<10} bid=${self.sell_bid:<12.4f} "
            f"gross={self.gross_spread_pct:.4f}%  "
            f"net={self.net_spread_pct:.4f}%  "
            f"est_profit=${self.est_profit_usd:.4f}"
        )


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

class ArbitrageScanner:
    """
    Reads from PriceCache at a configurable interval and checks every
    tracked coin for a profitable cross-exchange arbitrage opportunity.

    For each coin with data from at least 2 exchanges, every exchange
    pair is tested in both directions:
        - buy ask_A → sell bid_B
        - buy ask_B → sell bid_A

    The best (highest net spread) opportunity per coin is returned.
    """

    def __init__(self, cache: PriceCache):
        self._cache = cache
        self._signal_count = 0

    # ------------------------------------------------------------------
    # Continuous scan loop (run as asyncio task)
    # ------------------------------------------------------------------

    async def scan_loop(self) -> None:
        """Run the scanner continuously at SCAN_INTERVAL_MS intervals."""
        interval = config.SCAN_INTERVAL_MS / 1000
        logger.info(
            f"Arbitrage scanner started — "
            f"interval={config.SCAN_INTERVAL_MS}ms  "
            f"min_net_spread={config.MIN_NET_SPREAD_PCT}%  "
            f"trade_size=${config.TRADE_SIZE_USD}"
        )
        while True:
            signals = self.scan()
            for signal in signals:
                self._signal_count += 1
                logger.info(str(signal))
                if config.SIGNAL_PRINT_ENABLED:
                    print(signal, flush=True)

            await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # Single scan pass
    # ------------------------------------------------------------------

    def scan(self) -> List[ArbitrageSignal]:
        """
        One full scan pass across all tracked coins.
        Returns a list of profitable ArbitrageSignal instances (one per coin max).
        """
        signals = []
        for coin_id in self._cache.get_all_coins():
            signal = self._check_coin(coin_id)
            if signal:
                signals.append(signal)
        return signals

    # ------------------------------------------------------------------
    # Per-coin check
    # ------------------------------------------------------------------

    def _check_coin(self, coin_id: str) -> Optional[ArbitrageSignal]:
        """
        Compare every exchange pair for a single coin.

        Returns the most profitable ArbitrageSignal found, or None
        if no opportunity clears the minimum spread threshold.
        """
        agg = self._cache.get_aggregates(coin_id)
        if not agg or agg["exchange_count"] < 2:
            return None

        exchanges = agg["exchanges"]  # {exchange_name: ExchangeSnapshot}
        best: Optional[ArbitrageSignal] = None

        for ex_a, ex_b in itertools.combinations(exchanges.keys(), 2):
            snap_a = exchanges[ex_a]
            snap_b = exchanges[ex_b]

            # Test both directions: A→B and B→A
            for buy_snap, sell_snap in [(snap_a, snap_b), (snap_b, snap_a)]:
                signal = self._evaluate(coin_id, buy_snap, sell_snap)
                if signal and (best is None or signal.net_spread_pct > best.net_spread_pct):
                    best = signal

        return best

    def _evaluate(self, coin_id: str, buy_snap, sell_snap) -> Optional["ArbitrageSignal"]:
        """
        Evaluate one directional pair (buy on buy_snap's exchange,
        sell on sell_snap's exchange).

        Returns an ArbitrageSignal if net spread >= MIN_NET_SPREAD_PCT, else None.
        """
        buy_ask  = buy_snap.ask
        sell_bid = sell_snap.bid

        # Skip if either price is missing or zero
        if not buy_ask or not sell_bid or buy_ask <= 0 or sell_bid <= 0:
            return None

        buy_fee  = TAKER_FEES.get(buy_snap.exchange,  DEFAULT_TAKER_FEE)
        sell_fee = TAKER_FEES.get(sell_snap.exchange, DEFAULT_TAKER_FEE)

        # What you actually pay and receive after fees
        effective_buy  = buy_ask  * (1 + buy_fee)
        effective_sell = sell_bid * (1 - sell_fee)

        gross_spread_pct = (sell_bid - buy_ask)  / buy_ask  * 100
        net_spread_pct   = (effective_sell - effective_buy) / effective_buy * 100

        if net_spread_pct < config.MIN_NET_SPREAD_PCT:
            return None

        # Estimate profit for a reference trade size
        units          = config.TRADE_SIZE_USD / effective_buy
        est_profit_usd = units * (effective_sell - effective_buy)

        return ArbitrageSignal(
            coin_id=coin_id,
            buy_exchange=buy_snap.exchange,
            buy_ask=buy_ask,
            sell_exchange=sell_snap.exchange,
            sell_bid=sell_bid,
            gross_spread_pct=round(gross_spread_pct, 6),
            net_spread_pct=round(net_spread_pct, 6),
            est_profit_usd=round(est_profit_usd, 6),
        )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        return {"signals_emitted": self._signal_count}
