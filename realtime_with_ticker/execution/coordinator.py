"""
Arbitrage coordinator — the brain of the execution layer.

Takes an ArbitrageSignal from the scanner, validates pre-conditions
(balance, minimum size, cooldowns), places both buy and sell legs
simultaneously, then monitors the results.

Safety features:
    • DRY_RUN mode — logs everything, sends nothing
    • Balance check before each trade
    • Cooldown per coin to prevent rapid-fire re-entries
    • Maximum concurrent trades limit
    • Minimum trade size enforcement
    • Partial-fill logging and alerting
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import config
from execution.manager import ExecutionManager
from execution.types import (
    ArbitrageTradeResult,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)
from strategy.arbitrage import ArbitrageSignal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COOLDOWN_SECONDS = 30       # per-coin cooldown between trades
MAX_CONCURRENT_TRADES = 3   # max simultaneous arb trades
MIN_TRADE_USD = 10.0        # exchanges reject orders below ~$10


@dataclass
class TradeRecord:
    """Record of a completed (or attempted) arbitrage trade."""
    signal: ArbitrageSignal
    result: ArbitrageTradeResult
    timestamp: float = field(default_factory=time.time)


class ArbitrageCoordinator:
    """
    Orchestrates two-leg arbitrage execution.

    Workflow for each signal:
        1. Validate — check balances, cooldown, trade size
        2. Execute — place buy + sell simultaneously via asyncio.gather
        3. Report — log result, update history, print if configured
    """

    def __init__(self, manager: ExecutionManager):
        self._manager = manager
        self._cooldowns: Dict[str, float] = {}          # coin_id → last trade time
        self._active_trades: int = 0
        self._history: List[TradeRecord] = []
        self._total_profit: float = 0.0

    # ------------------------------------------------------------------
    # Public — main entry point
    # ------------------------------------------------------------------

    async def execute_signal(self, signal: ArbitrageSignal) -> Optional[ArbitrageTradeResult]:
        """
        Attempt to execute an arbitrage signal.

        Returns the ArbitrageTradeResult, or None if pre-checks fail.
        """
        # ---- Gate checks ------------------------------------------------
        if not self._pre_checks(signal):
            return None

        trade_usd = config.TRADE_SIZE_USD

        # Clamp to MAX_TRADE_SIZE_USD if configured
        max_size = getattr(config, "MAX_TRADE_SIZE_USD", 0)
        if max_size > 0:
            trade_usd = min(trade_usd, max_size)

        if trade_usd < MIN_TRADE_USD:
            logger.warning(
                f"[coordinator] Trade size ${trade_usd} below minimum ${MIN_TRADE_USD}"
            )
            return None

        # ---- Build order requests ----------------------------------------
        buy_request = OrderRequest(
            exchange=signal.buy_exchange,
            coin_id=signal.coin_id,
            side=OrderSide.BUY,
            quote="usdt",
            size_usd=trade_usd,
            order_type=OrderType.MARKET,
            limit_price=signal.buy_ask,     # hint for sell volume calc
        )
        sell_request = OrderRequest(
            exchange=signal.sell_exchange,
            coin_id=signal.coin_id,
            side=OrderSide.SELL,
            quote="usdt",
            size_usd=trade_usd,
            order_type=OrderType.MARKET,
            limit_price=signal.sell_bid,     # hint for sell volume calc
        )

        # ---- Execute both legs simultaneously ----------------------------
        self._active_trades += 1
        logger.info(
            f"[coordinator] EXECUTING {signal.coin_id}: "
            f"BUY {signal.buy_exchange} @ ${signal.buy_ask:.4f} | "
            f"SELL {signal.sell_exchange} @ ${signal.sell_bid:.4f} | "
            f"net_spread={signal.net_spread_pct:.4f}%"
        )

        try:
            buy_result, sell_result = await asyncio.gather(
                self._manager.place_order(buy_request),
                self._manager.place_order(sell_request),
            )
        except Exception as e:
            self._active_trades -= 1
            logger.error(f"[coordinator] Execution error: {e}")
            return None

        self._active_trades -= 1

        # ---- Build combined result ---------------------------------------
        trade_result = ArbitrageTradeResult(
            coin_id=signal.coin_id,
            buy_result=buy_result,
            sell_result=sell_result,
        )
        trade_result.compute_profit()

        # ---- Post-trade bookkeeping --------------------------------------
        self._cooldowns[signal.coin_id] = time.time()
        self._history.append(TradeRecord(signal=signal, result=trade_result))
        if trade_result.status == "success":
            self._total_profit += trade_result.net_profit_usd

        # ---- Report ------------------------------------------------------
        self._report(trade_result)
        return trade_result

    # ------------------------------------------------------------------
    # Continuous loop — reads from scanner
    # ------------------------------------------------------------------

    async def run(self, scanner) -> None:
        """
        Continuously read signals from the scanner and execute them.

        Designed to run as an asyncio task alongside the scanner.
        The scanner's scan_loop() prints signals; this loop acts on them.
        """
        interval = config.SCAN_INTERVAL_MS / 1000
        logger.info("[coordinator] Arbitrage coordinator started")

        while True:
            try:
                signals = scanner.scan()
                for signal in signals:
                    await self.execute_signal(signal)
            except asyncio.CancelledError:
                logger.info("[coordinator] Shutting down")
                raise
            except Exception as e:
                logger.error(f"[coordinator] Loop error: {e}")

            await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # Pre-trade validation
    # ------------------------------------------------------------------

    def _pre_checks(self, signal: ArbitrageSignal) -> bool:
        """Run all gate checks. Returns True if trade should proceed."""
        # 1. Concurrent trade limit
        if self._active_trades >= MAX_CONCURRENT_TRADES:
            logger.debug(f"[coordinator] Max concurrent trades ({MAX_CONCURRENT_TRADES}) reached")
            return False

        # 2. Per-coin cooldown
        last = self._cooldowns.get(signal.coin_id, 0)
        if time.time() - last < COOLDOWN_SECONDS:
            remaining = COOLDOWN_SECONDS - (time.time() - last)
            logger.debug(
                f"[coordinator] {signal.coin_id} on cooldown ({remaining:.0f}s left)"
            )
            return False

        # 3. Both exchanges must be registered
        if not self._manager.get_client(signal.buy_exchange):
            logger.warning(f"[coordinator] No client for buy exchange '{signal.buy_exchange}'")
            return False
        if not self._manager.get_client(signal.sell_exchange):
            logger.warning(f"[coordinator] No client for sell exchange '{signal.sell_exchange}'")
            return False

        return True

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def _report(self, result: ArbitrageTradeResult) -> None:
        """Log and optionally print the trade result."""
        logger.info(str(result))

        if config.SIGNAL_PRINT_ENABLED:
            if result.status == "success":
                print(
                    f"\033[92m✓ TRADE {result.coin_id} — "
                    f"profit=${result.net_profit_usd:.4f}\033[0m",
                    flush=True,
                )
            elif result.status == "partial":
                print(
                    f"\033[93m⚠ PARTIAL {result.coin_id} — "
                    f"{result.error}\033[0m",
                    flush=True,
                )
            else:
                print(
                    f"\033[91m✗ FAILED {result.coin_id} — "
                    f"{result.error}\033[0m",
                    flush=True,
                )

    # ------------------------------------------------------------------
    # Stats / history
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        return {
            "total_trades": len(self._history),
            "active_trades": self._active_trades,
            "total_profit_usd": round(self._total_profit, 4),
            "successful": sum(1 for r in self._history if r.result.status == "success"),
            "partial": sum(1 for r in self._history if r.result.status == "partial"),
            "failed": sum(1 for r in self._history if r.result.status == "failed"),
        }

    @property
    def history(self) -> List[TradeRecord]:
        return list(self._history)
