"""
Pipeline — orchestrates the orderbook data flow.

    Exchange Connectors → Queue → Normalizer → OrderBookCache
                                                     ↓
                                              Live Terminal Table
                                                     ↓
                                         ArbitrageScanner (external)

The live terminal table shows the best cross-exchange opportunities
for each coin, updated in-place using ANSI escape codes.

Columns:
    COIN | BUY FROM | BEST ASK | ASK DEPTH | SELL TO | BEST BID |
    BID DEPTH | SPREAD % | NET % | EST PROFIT
"""

import asyncio
import logging
import sys
import time
from typing import Dict, List, Optional

import config
from core.models import RawOrderBook, NormalizedOrderBook
from exchanges.base import BaseExchange
from normalizer.normalizer import Normalizer
from storage.orderbook_cache import OrderBookCache
from strategy.arbitrage import TAKER_FEES, DEFAULT_TAKER_FEE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Live terminal display
# ---------------------------------------------------------------------------

class _OrderBookPrinter:
    """
    Renders the live orderbook arbitrage table in-place on the terminal.

    For each coin, finds the best buy (lowest effective ask) and best
    sell (highest effective bid) across all exchanges and shows the
    spread, depth, and estimated profit for TRADE_SIZE_USD.

    The entire table is redrawn in-place using ANSI escape codes.
    """

    _HDR = (
        "  {:<20}{:<9}{:>5}{:>13}{:>11}  {:<9}{:>5}{:>13}{:>11}  {:>10}  {:>10}  {:>11}".format(
            "COIN", "BUY FROM", "AGE", "BEST ASK", "ASK DEPTH",
            "SELL TO", "AGE", "BEST BID", "BID DEPTH",
            "SPREAD %", "NET %", "EST PROFIT",
        )
    )
    _SEP = "  " + "\u2500" * 135

    def __init__(self) -> None:
        self._rows: Dict[str, str] = {}   # coin_id → rendered line
        self._order: List[str] = []       # stable insertion order
        self._lines_printed = 0

    # ------------------------------------------------------------------

    def update(self, coin_id: str, books: Dict[str, NormalizedOrderBook]) -> None:
        """
        Recompute the best cross-exchange opportunity for *coin_id*
        and cache the rendered row.  Actual terminal output happens
        in redraw() on a timer.
        """
        if not books or len(books) < 2:
            # Single exchange — show internal bid-ask spread (same exchange both sides)
            if books:
                ex_name, book = next(iter(books.items()))
                now = time.time()
                age_s = now - book.timestamp
                age_str = f"{age_s:.0f}s" if age_s < 60 else f"{age_s/60:.1f}m"
                ask_str   = f"${book.best_ask.price:,.4f}" if book.best_ask else "—"
                bid_str   = f"${book.best_bid.price:,.4f}" if book.best_bid else "—"
                ask_depth = f"${book.ask_depth_usd():,.0f}" if book.best_ask else "—"
                bid_depth = f"${book.bid_depth_usd():,.0f}" if book.best_bid else "—"

                # Compute the internal bid-ask spread for TRADE_SIZE_USD
                eff_buy  = book.effective_buy_price(config.TRADE_SIZE_USD)
                eff_sell = book.effective_sell_price(config.TRADE_SIZE_USD)

                if eff_buy and eff_sell and eff_buy > 0:
                    fee = TAKER_FEES.get(ex_name, DEFAULT_TAKER_FEE)
                    gross_spread_pct = (eff_sell - eff_buy) / eff_buy * 100
                    cost = eff_buy * (1 + fee)
                    rev  = eff_sell * (1 - fee)
                    net_spread_pct   = (rev - cost) / cost * 100
                    units      = config.TRADE_SIZE_USD / cost if cost else 0
                    est_profit = units * (rev - cost)

                    spread_s = f"{gross_spread_pct:>+9.4f}%"
                    net_s    = f"{net_spread_pct:>+9.4f}%"
                    prof_s   = f"${est_profit:>+10.4f}"

                    if gross_spread_pct > 0:
                        spread_s = f"\033[32m{spread_s}\033[0m"
                    elif gross_spread_pct < 0:
                        spread_s = f"\033[31m{spread_s}\033[0m"

                    if net_spread_pct > 0:
                        net_s  = f"\033[32m{net_s}\033[0m"
                        prof_s = f"\033[32m{prof_s}\033[0m"
                    elif net_spread_pct < 0:
                        net_s  = f"\033[31m{net_s}\033[0m"
                        prof_s = f"\033[31m{prof_s}\033[0m"
                else:
                    spread_s = net_s = prof_s = "—"

                self._rows[coin_id] = (
                    f"  {coin_id:<20}{ex_name:<9}{age_str:>5}{ask_str:>13}{ask_depth:>11}"
                    f"  {ex_name:<9}{age_str:>5}{bid_str:>13}{bid_depth:>11}"
                    f"  {spread_s:>10}  {net_s:>10}  {prof_s:>11}"
                )
                if coin_id not in self._order:
                    self._order.append(coin_id)
            return

        trade_usd = config.TRADE_SIZE_USD

        # Find the exchange with lowest effective buy (ask) price
        best_buy_ex: Optional[str] = None
        best_buy_price: float = float("inf")
        best_buy_book: Optional[NormalizedOrderBook] = None

        # Find the exchange with highest effective sell (bid) price
        best_sell_ex: Optional[str] = None
        best_sell_price: float = 0.0
        best_sell_book: Optional[NormalizedOrderBook] = None

        for ex, book in books.items():
            eff_buy = book.effective_buy_price(trade_usd)
            if eff_buy is not None and eff_buy < best_buy_price:
                best_buy_price = eff_buy
                best_buy_ex = ex
                best_buy_book = book

            eff_sell = book.effective_sell_price(trade_usd)
            if eff_sell is not None and eff_sell > best_sell_price:
                best_sell_price = eff_sell
                best_sell_ex = ex
                best_sell_book = book

        if not best_buy_book or not best_sell_book:
            return

        # Book ages
        now = time.time()
        buy_age_s  = now - best_buy_book.timestamp
        sell_age_s = now - best_sell_book.timestamp
        buy_age_str  = f"{buy_age_s:.0f}s"  if buy_age_s  < 60 else f"{buy_age_s /60:.1f}m"
        sell_age_str = f"{sell_age_s:.0f}s" if sell_age_s < 60 else f"{sell_age_s/60:.1f}m"

        # Compute spread (gross and net)
        buy_fee = TAKER_FEES.get(best_buy_ex, DEFAULT_TAKER_FEE)
        sell_fee = TAKER_FEES.get(best_sell_ex, DEFAULT_TAKER_FEE)

        gross_spread_pct = (
            (best_sell_price - best_buy_price) / best_buy_price * 100
            if best_buy_price else 0.0
        )

        cost_per_unit = best_buy_price * (1 + buy_fee)
        rev_per_unit = best_sell_price * (1 - sell_fee)
        net_spread_pct = (
            (rev_per_unit - cost_per_unit) / cost_per_unit * 100
            if cost_per_unit else 0.0
        )

        units = trade_usd / cost_per_unit if cost_per_unit else 0
        est_profit = units * (rev_per_unit - cost_per_unit)

        # Format price/depth strings
        ask_str       = f"${best_buy_book.best_ask.price:,.4f}" if best_buy_book.best_ask else "—"
        ask_depth_str = f"${best_buy_book.ask_depth_usd():,.0f}"
        bid_str       = f"${best_sell_book.best_bid.price:,.4f}" if best_sell_book.best_bid else "—"
        bid_depth_str = f"${best_sell_book.bid_depth_usd():,.0f}"

        # Pre-format to FIXED VISUAL WIDTHS before applying color.
        # Python's {:>N} counts ANSI escape chars as real chars — pre-formatting
        # ensures each column stays the same visual width regardless of color.
        spread_s = f"{gross_spread_pct:>+9.4f}%"   # always 10 visible chars
        net_s    = f"{net_spread_pct:>+9.4f}%"     # always 10 visible chars
        prof_s   = f"${est_profit:>+10.4f}"         # always 11 visible chars

        if gross_spread_pct > 0:
            spread_s = f"\033[32m{spread_s}\033[0m"
        elif gross_spread_pct < 0:
            spread_s = f"\033[31m{spread_s}\033[0m"

        if net_spread_pct > 0:
            net_s  = f"\033[32m{net_s}\033[0m"
            prof_s = f"\033[32m{prof_s}\033[0m"
        elif net_spread_pct < 0:
            net_s  = f"\033[31m{net_s}\033[0m"
            prof_s = f"\033[31m{prof_s}\033[0m"

        self._rows[coin_id] = (
            f"  {coin_id:<20}{best_buy_ex:<9}{buy_age_str:>5}{ask_str:>13}{ask_depth_str:>11}"
            f"  {best_sell_ex:<9}{sell_age_str:>5}{bid_str:>13}{bid_depth_str:>11}"
            f"  {spread_s}  {net_s}  {prof_s}"
        )
        if coin_id not in self._order:
            self._order.append(coin_id)

    # ------------------------------------------------------------------

    def redraw(self) -> None:
        """Repaint the entire table in-place (called by timer)."""
        if not self._rows:
            return
        self._redraw()

    def _redraw(self) -> None:
        if self._lines_printed:
            sys.stdout.write(f"\033[{self._lines_printed}A")

        lines = [self._rows[c] for c in self._order if c in self._rows]
        sys.stdout.write(f"\033[2K{self._HDR}\n")
        sys.stdout.write(f"\033[2K{self._SEP}\n")
        for line in lines:
            sys.stdout.write(f"\033[2K{line}\n")
        sys.stdout.flush()
        self._lines_printed = 2 + len(lines)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class Pipeline:
    """
    Orchestrates the full orderbook data pipeline.

    Components:
      - Exchange connectors: produce RawOrderBook events
      - Ingestion queue: decouples receiving from processing
      - Normalizer: converts RawOrderBook → NormalizedOrderBook
      - OrderBookCache: latest book per coin per exchange
      - Live printer: terminal table with spreads, depths, profits

    All components run as concurrent asyncio tasks.
    """

    def __init__(
        self,
        connectors: List[BaseExchange],
        normalizer: Normalizer,
        cache: OrderBookCache,
        queue: asyncio.Queue,
    ):
        self.connectors = connectors
        self.normalizer = normalizer
        self.cache = cache
        self.queue = queue

        # Stats
        self._raw_count = 0
        self._normalized_count = 0
        self._dropped_count = 0
        self._start_time: float = 0

        # In-place terminal display
        self._printer = _OrderBookPrinter()

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start all pipeline components as concurrent tasks."""
        self._start_time = time.time()
        logger.info("Pipeline starting...")

        tasks = []

        # 1. Exchange connectors (producers → ingestion queue)
        for connector in self.connectors:
            tasks.append(asyncio.create_task(
                connector.run(),
                name=f"connector:{connector.NAME}",
            ))

        # 2. Worker (queue → normalizer → cache)
        tasks.append(asyncio.create_task(
            self._process_loop(),
            name="worker",
        ))

        # 3. Stats logger
        tasks.append(asyncio.create_task(
            self._stats_loop(),
            name="stats",
        ))

        # 4. Live display timer
        if config.LIVE_TABLE:
            tasks.append(asyncio.create_task(
                self._print_loop(),
                name="print",
            ))

        logger.info(
            f"Pipeline running with {len(self.connectors)} connector(s): "
            f"{[c.NAME for c in self.connectors]}"
        )

        # When live table is on, redirect ALL logging to a file so that
        # terminal messages can't corrupt the cursor position of the ANSI table.
        # Errors/warnings are preserved in orderbook.log for debugging.
        if config.LIVE_TABLE:
            root = logging.getLogger()
            for h in list(root.handlers):
                root.removeHandler(h)
            fh = logging.FileHandler("orderbook.log", mode="a")
            fh.setFormatter(logging.Formatter(
                "%(asctime)s \u2502 %(levelname)-7s \u2502 %(name)-20s \u2502 %(message)s",
                datefmt="%H:%M:%S",
            ))
            root.addHandler(fh)
            print()   # blank line before the table

        # Wait for all tasks; if one crashes, log it and cancel the rest
        try:
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_EXCEPTION
            )
            for task in done:
                if task.exception():
                    logger.error(
                        f"Task '{task.get_name()}' crashed: {task.exception()}"
                    )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        except asyncio.CancelledError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _process_loop(self) -> None:
        """Pull RawOrderBook from queue, normalize, write to cache, update display."""
        logger.info("Worker started — processing ingestion queue")

        while True:
            raw: RawOrderBook = await self.queue.get()
            self._raw_count += 1

            try:
                normalized = self.normalizer.normalize(raw)

                if normalized:
                    await self.cache.write(normalized)
                    self._normalized_count += 1

                    if config.LIVE_TABLE:
                        books = self.cache.get_books(normalized.coin_id)
                        if books:
                            self._printer.update(normalized.coin_id, books)
                else:
                    self._dropped_count += 1
            except Exception as e:
                logger.error(f"Error processing orderbook: {e}")
                self._dropped_count += 1
            finally:
                self.queue.task_done()

    # ------------------------------------------------------------------
    # Timer-driven display
    # ------------------------------------------------------------------

    async def _print_loop(self) -> None:
        """Redraw the table every 500ms — decoupled from tick rate."""
        await asyncio.sleep(3)   # let exchanges connect first
        while True:
            self._printer.redraw()
            await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def _stats_loop(self) -> None:
        """Log pipeline stats every 30 seconds."""
        while True:
            await asyncio.sleep(30)

            uptime = time.time() - self._start_time
            rate = self._raw_count / uptime if uptime > 0 else 0

            logger.info(
                f"[stats] uptime={uptime:.0f}s | "
                f"raw={self._raw_count} | "
                f"normalized={self._normalized_count} | "
                f"dropped={self._dropped_count} | "
                f"rate={rate:.1f} books/s | "
                f"queue={self.queue.qsize()} | "
                f"cache={self.cache.stats}"
            )

    # ------------------------------------------------------------------
    # Health data
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        uptime = time.time() - self._start_time if self._start_time else 0
        return {
            "uptime_seconds": round(uptime),
            "raw_books": self._raw_count,
            "normalized_books": self._normalized_count,
            "dropped_books": self._dropped_count,
            "queue_depth": self.queue.qsize(),
            "cache": self.cache.stats,
            "connectors": {
                c.NAME: {"last_message": c.last_message_time}
                for c in self.connectors
            },
        }
