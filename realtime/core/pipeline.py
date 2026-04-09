"""

"""

import asyncio
import logging
import sys
import time
from typing import List

import config
from core.models import RawTick
from exchanges.base import BaseExchange
from normalizer.normalizer import Normalizer
from storage.price_cache import PriceCache

logger = logging.getLogger(__name__)


class _CachePrinter:
    """
    Renders the live price cache as a fixed, in-place terminal block.

    Each coin occupies one row. When a price updates, the entire block
    is redrawn in-place using ANSI escape codes — the output never scrolls.

    Column layout (arbitrage view):
        COIN | BUY FROM (lowest price) | SELL TO (highest price) | SPREAD
    """

    _HDR = "  {:<22}{:<10}{:<18}{:<10}{:<18}{:<10}".format(
        "COIN", "BUY FROM", "PRICE", "SELL TO", "PRICE", "SPREAD"
    )
    _SEP = "  " + "\u2500" * 88

    def __init__(self) -> None:
        self._rows: dict = {}   # coin_id -> rendered line
        self._order: list = []  # stable insertion-order list of coin_ids
        self._lines_printed = 0

    def update(self, coin_id: str, agg: dict) -> None:
        lowest  = agg["lowest"]
        highest = agg["highest"]
        spread  = (
            round((highest.price - lowest.price) / lowest.price * 100, 4)
            if lowest.price else 0.0
        )
        self._rows[coin_id] = "  {:<22}{:<10}${:<17.4f}{:<10}${:<17.4f}{:<9}%".format(
            coin_id,
            lowest.exchange,  lowest.price,
            highest.exchange, highest.price,
            spread,
        )
        if coin_id not in self._order:
            self._order.append(coin_id)
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


class Pipeline:
    """
    Orchestrates the full realtime data pipeline.

    Components:
      - Exchange connectors: produce RawTick events
      - Ingestion queue: decouples receiving from processing
      - Normalizer: converts RawTick → NormalizedTick
      - Redis writer: batched pipeline writes to Redis

    All components run as concurrent asyncio tasks.
    """

    def __init__(
        self,
        connectors: List[BaseExchange],
        normalizer: Normalizer,
        writer: PriceCache,
        queue: asyncio.Queue,
    ):
        self.connectors = connectors
        self.normalizer = normalizer
        self.writer = writer
        self.queue = queue

        # Stats
        self._raw_count = 0
        self._normalized_count = 0
        self._dropped_count = 0
        self._start_time: float = 0

        # In-place terminal display (used when CACHE_LIVE_PRINT is enabled)
        self._cache_printer = _CachePrinter()

    # ------------------------------------------------------------------
    # Run the full pipeline
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Start all pipeline components as concurrent tasks.
        Runs forever until cancelled or a critical task crashes.
        """
        self._start_time = time.time()
        logger.info("Pipeline starting...")

        tasks = []

        # 1. Exchange connectors (producers → ingestion queue)
        for connector in self.connectors:
            tasks.append(asyncio.create_task(
                connector.run(),
                name=f"connector:{connector.NAME}",
            ))

        # 2. Worker (ingestion queue → normalizer → Redis writer)
        tasks.append(asyncio.create_task(
            self._process_loop(),
            name="worker",
        ))

        # 3. Stats logger (periodic health output)
        tasks.append(asyncio.create_task(
            self._stats_loop(),
            name="stats",
        ))

        logger.info(
            f"Pipeline running with {len(self.connectors)} connector(s): "
            f"{[c.NAME for c in self.connectors]}"
        )

        # When live print is on, silence INFO logs so they don't interfere
        # with the in-place ANSI display. Print a blank line to separate
        # startup output from the cache block.
        if config.CACHE_LIVE_PRINT:
            logging.getLogger().setLevel(logging.WARNING)
            print()

        # Wait for all tasks — they run forever.  If one crashes,
        # log the error and cancel the rest.
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
            # Ctrl+C — cancel all tasks and wait for them to finish cleanly
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _process_loop(self) -> None:
        """
        Worker loop: pulls RawTick from the ingestion queue,
        normalizes them, and sends NormalizedTick to the Redis writer.

        This is the core of the pipeline — the single point where
        raw exchange data becomes canonical data.
        """
        logger.info("Worker started — processing ingestion queue")

        while True:
            tick: RawTick = await self.queue.get()
            self._raw_count += 1

            try:
                normalized = self.normalizer.normalize(tick)

                if normalized:
                    await self.writer.write(normalized)
                    self._normalized_count += 1
                    if config.CACHE_LIVE_PRINT:
                        agg = self.writer.get_aggregates(normalized.coin_id)
                        if agg:
                            self._cache_printer.update(normalized.coin_id, agg)
                else:
                    self._dropped_count += 1
            except Exception as e:
                logger.error(f"Error normalizing tick: {e}")
                self._dropped_count += 1
            finally:
                self.queue.task_done()

    # ------------------------------------------------------------------
    # Stats logger
    # ------------------------------------------------------------------

    async def _stats_loop(self) -> None:
        """Log pipeline stats every 30 seconds for monitoring."""
        while True:
            await asyncio.sleep(30)

            uptime = time.time() - self._start_time
            rate = self._raw_count / uptime if uptime > 0 else 0

            logger.info(
                f"[stats] uptime={uptime:.0f}s | "
                f"raw={self._raw_count} | "
                f"normalized={self._normalized_count} | "
                f"dropped={self._dropped_count} | "
                f"rate={rate:.1f} ticks/s | "
                f"queue={self.queue.qsize()} | "
                f"cache={self.writer.stats}"
            )

    # ------------------------------------------------------------------
    # Health check data
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        """Pipeline stats for the /health endpoint."""
        uptime = time.time() - self._start_time if self._start_time else 0
        return {
            "uptime_seconds": round(uptime),
            "raw_ticks": self._raw_count,
            "normalized_ticks": self._normalized_count,
            "dropped_ticks": self._dropped_count,
            "queue_depth": self.queue.qsize(),
            "cache": self.writer.stats,
            "connectors": {
                c.NAME: {"last_message": c.last_message_time}
                for c in self.connectors
            },
        }
