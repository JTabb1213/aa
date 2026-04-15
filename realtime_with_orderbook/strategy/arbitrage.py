"""
Orderbook-based arbitrage scanner.

Unlike the ticker-based scanner that only sees top-of-book bid/ask,
this scanner walks the orderbook depth to calculate the *actual*
price you'd pay/receive for a given trade size after slippage.

For each coin with orderbooks from ≥2 exchanges:
  1. Walk ask-side on exchange A to compute effective buy price for $X
  2. Walk bid-side on exchange B to compute effective sell price for $X
  3. Subtract taker fees on both sides
  4. If net profit > threshold → emit signal

This is much more accurate than ticker-based arbitrage because it
accounts for liquidity at each price level.
"""

import asyncio
import itertools
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import config
from core.models import NormalizedOrderBook
from storage.orderbook_cache import OrderBookCache

# ---------------------------------------------------------------------------
# Trade signal file logger
# ---------------------------------------------------------------------------

_LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
_LOG_FILE = os.path.join(_LOGS_DIR, "trades.log")

# Header to prepend if the log file is new/empty
_LOG_HEADER = (
    f"{'TIMESTAMP':<22}"
    f"{'COIN':<20}"
    f"{'BUY FROM':<10}"
    f"{'AGE':>5}"
    f"{'BEST ASK':>13}"
    f"{'ASK DEPTH':>11}"
    f"  "
    f"{'SELL TO':<10}"
    f"{'AGE':>5}"
    f"{'BEST BID':>13}"
    f"{'BID DEPTH':>11}"
    f"  {'SPREAD %':>10}  {'NET %':>10}  {'EST PROFIT':>11}\n"
    + "-" * 152 + "\n"
)


def _ensure_log_dir() -> None:
    os.makedirs(_LOGS_DIR, exist_ok=True)
    if not os.path.exists(_LOG_FILE) or os.path.getsize(_LOG_FILE) == 0:
        with open(_LOG_FILE, "w") as f:
            f.write(_LOG_HEADER)


def _write_signal(signal: "ArbitrageSignal") -> None:
    """Append one detected signal to logs/trades.log in table-row format."""
    try:
        _ensure_log_dir()
        ts = datetime.fromtimestamp(signal.timestamp).strftime("%Y-%m-%d %H:%M:%S")
        ask_str   = f"${signal.buy_best_ask:,.4f}"
        bid_str   = f"${signal.sell_best_bid:,.4f}"
        ask_depth = f"${signal.buy_depth_usd:,.0f}"
        bid_depth = f"${signal.sell_depth_usd:,.0f}"
        spread_s  = f"{signal.gross_spread_pct:>+9.4f}%"
        net_s     = f"{signal.net_spread_pct:>+9.4f}%"
        prof_s    = f"${signal.est_profit_usd:>+10.4f}"
        buy_age   = f"{signal.buy_book_age_s:.1f}s"
        sell_age  = f"{signal.sell_book_age_s:.1f}s"
        line = (
            f"{ts:<22}"
            f"{signal.coin_id:<20}"
            f"{signal.buy_exchange:<10}"
            f"{buy_age:>5}"
            f"{ask_str:>13}"
            f"{ask_depth:>11}"
            f"  "
            f"{signal.sell_exchange:<10}"
            f"{sell_age:>5}"
            f"{bid_str:>13}"
            f"{bid_depth:>11}"
            f"  {spread_s:>10}  {net_s:>10}  {prof_s:>11}\n"
        )
        with open(_LOG_FILE, "a") as f:
            f.write(line)
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to write signal log: {e}")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Taker fee rates per exchange (fraction, not percent)
# ---------------------------------------------------------------------------
TAKER_FEES: Dict[str, float] = {
    "kraken":   0.0026,   # 0.26%
    "coinbase": 0.0060,   # 0.60%
    "binance":  0.0010,   # 0.10%
    "bybit":    0.0010,   # 0.10%
    "okx":      0.0010,   # 0.10%
    "pionex":   0.0005,   # 0.05%
    "mexc":     0.0010,   # 0.10%
    "gateio":   0.0020,   # 0.20%
}
DEFAULT_TAKER_FEE = 0.0050


# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------

@dataclass
class ArbitrageSignal:
    """
    A detected arbitrage opportunity with depth-aware pricing.

    Unlike ticker signals, these include:
      - effective prices after walking the book for TRADE_SIZE_USD
      - available liquidity at the target price levels
      - slippage estimate
    """
    coin_id: str
    buy_exchange: str
    buy_price: float              # effective buy price (after slippage)
    buy_best_ask: float           # top-of-book ask on buy exchange
    buy_depth_usd: float          # total USD depth on ask side
    sell_exchange: str
    sell_price: float             # effective sell price (after slippage)
    sell_best_bid: float          # top-of-book bid on sell exchange
    sell_depth_usd: float         # total USD depth on bid side
    gross_spread_pct: float
    net_spread_pct: float
    est_profit_usd: float
    trade_size_usd: float
    slippage_buy_pct: float       # how much worse than best ask
    slippage_sell_pct: float      # how much worse than best bid
    buy_book_age_s: float = 0.0   # seconds since buy-side book was last updated
    sell_book_age_s: float = 0.0  # seconds since sell-side book was last updated
    timestamp: float = field(default_factory=time.time)

    def __str__(self) -> str:
        return (
            f"\n  ┌─ ORDERBOOK ARBITRAGE SIGNAL ──────────────────────────────┐\n"
            f"  │  Coin         : {self.coin_id}\n"
            f"  │  Trade size   : ${self.trade_size_usd:,.2f}\n"
            f"  │  BUY  on      : {self.buy_exchange:<10}  eff.price = ${self.buy_price:,.4f}\n"
            f"  │    best ask   : ${self.buy_best_ask:,.4f}  depth = ${self.buy_depth_usd:,.2f}\n"
            f"  │    slippage   : {self.slippage_buy_pct:+.4f}%\n"
            f"  │  SELL on      : {self.sell_exchange:<10}  eff.price = ${self.sell_price:,.4f}\n"
            f"  │    best bid   : ${self.sell_best_bid:,.4f}  depth = ${self.sell_depth_usd:,.2f}\n"
            f"  │    slippage   : {self.slippage_sell_pct:+.4f}%\n"
            f"  │  Gross spread : {self.gross_spread_pct:+.4f}%\n"
            f"  │  Net spread   : {self.net_spread_pct:+.4f}%  (after fees + slippage)\n"
            f"  │  Est. profit  : ${self.est_profit_usd:,.4f}\n"
            f"  └────────────────────────────────────────────────────────────┘"
        )


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

class OrderBookArbitrageScanner:
    """
    Scans the OrderBookCache for cross-exchange arbitrage opportunities.

    For each coin with data from ≥2 exchanges, walks the orderbook
    depth to compute realistic execution prices.
    """

    def __init__(self, cache: OrderBookCache):
        self._cache = cache
        self._signal_count = 0

    async def scan_loop(self) -> None:
        """Run continuously at SCAN_INTERVAL_MS intervals."""
        interval = config.SCAN_INTERVAL_MS / 1000
        logger.info(
            f"Orderbook arbitrage scanner started — "
            f"interval={config.SCAN_INTERVAL_MS}ms  "
            f"min_net_spread={config.MIN_NET_SPREAD_PCT}%  "
            f"trade_size=${config.TRADE_SIZE_USD}"
        )
        _ensure_log_dir()
        while True:
            signals = self.scan()
            for signal in signals:
                self._signal_count += 1
                logger.info(str(signal))
                if config.SIGNAL_PRINT_ENABLED:
                    print(signal, flush=True)
                _write_signal(signal)
            await asyncio.sleep(interval)

    def scan(self) -> List[ArbitrageSignal]:
        """One full scan pass. Returns list of profitable signals."""
        signals = []
        for coin_id in self._cache.get_all_coins():
            signal = self._check_coin(coin_id)
            if signal:
                signals.append(signal)
        return signals

    def _check_coin(self, coin_id: str) -> Optional[ArbitrageSignal]:
        """Check all exchange pairs for a coin. Returns best signal or None."""
        books = self._cache.get_books(coin_id)
        if not books or len(books) < 2:
            return None

        best: Optional[ArbitrageSignal] = None

        for ex_a, ex_b in itertools.combinations(books.keys(), 2):
            book_a = books[ex_a]
            book_b = books[ex_b]

            # Test both directions
            for buy_book, sell_book in [(book_a, book_b), (book_b, book_a)]:
                signal = self._evaluate(coin_id, buy_book, sell_book)
                if signal and (best is None or signal.net_spread_pct > best.net_spread_pct):
                    best = signal

        return best

    def _evaluate(
        self,
        coin_id: str,
        buy_book: NormalizedOrderBook,
        sell_book: NormalizedOrderBook,
    ) -> Optional[ArbitrageSignal]:
        """
        Evaluate buying on buy_book's exchange and selling on sell_book's.

        Walks the orderbook depth to compute realistic execution prices
        for TRADE_SIZE_USD, then checks if net spread > threshold.
        """
        trade_usd = config.TRADE_SIZE_USD

        # Check if both books have data
        if not buy_book.best_ask or not sell_book.best_bid:
            return None

        # Walk the orderbook to get effective execution prices
        eff_buy = buy_book.effective_buy_price(trade_usd)
        eff_sell = sell_book.effective_sell_price(trade_usd)

        if eff_buy is None or eff_sell is None:
            return None  # not enough liquidity

        if eff_buy <= 0 or eff_sell <= 0:
            return None

        # Apply taker fees
        buy_fee = TAKER_FEES.get(buy_book.exchange, DEFAULT_TAKER_FEE)
        sell_fee = TAKER_FEES.get(sell_book.exchange, DEFAULT_TAKER_FEE)

        cost = eff_buy * (1 + buy_fee)
        revenue = eff_sell * (1 - sell_fee)

        gross_spread_pct = (eff_sell - eff_buy) / eff_buy * 100
        net_spread_pct = (revenue - cost) / cost * 100

        if net_spread_pct < config.MIN_NET_SPREAD_PCT:
            return None

        # Compute profit
        units = trade_usd / cost
        est_profit = units * (revenue - cost)

        # Slippage: how much worse than best price
        best_ask = buy_book.best_ask.price
        best_bid = sell_book.best_bid.price
        slippage_buy = (eff_buy - best_ask) / best_ask * 100 if best_ask else 0
        slippage_sell = (best_bid - eff_sell) / best_bid * 100 if best_bid else 0

        now = time.time()
        return ArbitrageSignal(
            coin_id=coin_id,
            buy_exchange=buy_book.exchange,
            buy_price=round(eff_buy, 6),
            buy_best_ask=best_ask,
            buy_depth_usd=round(buy_book.ask_depth_usd(), 2),
            sell_exchange=sell_book.exchange,
            sell_price=round(eff_sell, 6),
            sell_best_bid=best_bid,
            sell_depth_usd=round(sell_book.bid_depth_usd(), 2),
            gross_spread_pct=round(gross_spread_pct, 6),
            net_spread_pct=round(net_spread_pct, 6),
            est_profit_usd=round(est_profit, 6),
            trade_size_usd=trade_usd,
            slippage_buy_pct=round(slippage_buy, 6),
            slippage_sell_pct=round(slippage_sell, 6),
            buy_book_age_s=round(now - buy_book.timestamp, 1),
            sell_book_age_s=round(now - sell_book.timestamp, 1),
        )

    @property
    def stats(self) -> dict:
        return {"signals_emitted": self._signal_count}
